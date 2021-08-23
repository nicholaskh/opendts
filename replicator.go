/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replicator

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/juju/ratelimit"
	kafka "github.com/segmentio/kafka-go"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/config"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/evalengine"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/http_server/vo"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysqlctl"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
)

var (
	// idleTimeout is set to slightly above 1s, compared to heartbeatTime
	// set by VStreamer at slightly below 1s. This minimizes conflicts
	// between the two timeouts.
	// NOTE flush pos to mysql every 10s
	idleTimeout      = 11000 * time.Millisecond
	dbLockRetryDelay = 1 * time.Second
	relayLogMaxSize  = flag.Int("relay_log_max_size", 250000000, "Maximum buffer size (in bytes) for replication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	relayLogMaxItems = flag.Int("relay_log_max_items", 5000, "Maximum number of rows for replication target buffering.")
	// FIXME maybe longer
	copyTimeout         = 240 * time.Hour
	replicaLagTolerance = 10 * time.Second
)

type TopicMode uint8

const (
	TopicModeDB TopicMode = iota
	TopicModeTb
)

const (
	PartitionModeColumn     = "column"
	PartitionModeTable      = "table"
	PartitionModePrimaryKey = "primary_key"
)

const (
	ProducerPartitionByFallbackTable      = "table"
	ProducerPartitionByFallbackPrimaryKey = "primary_key"

	DefaultProducerPartitionByFallbackTable = ProducerPartitionByFallbackTable
)

type ReplicatorMode uint8

const (
	ModeOLAP ReplicatorMode = iota
	ModeOLTP
	ModeBoth
)

type StatsHistoryRecord struct {
	Time    time.Time
	Message string
}

// Replicator provides the core logic to start vreplication streams
type Replicator struct {
	Id      int
	State   string
	Message string

	dbClient    *DBClient
	kafkaWriter *kafka.Writer

	filter *proto.Filter

	sourceConnParams *mysql.ConnParams
	destConnParams   *mysql.ConnParams
	kafkaParams      *KafkaParams

	dbnames map[string]struct{}

	storage *DBClient

	stats *Stats
	// mysqld is used to fetch the local schema.
	mysqld mysqlctl.MysqlDaemon

	incrementalMode bool

	originalFKCheckSetting int64

	cfg *config.ReplicatorConfig

	pkInfoMap   map[string]map[string][]*PrimaryKeyInfo
	tbFieldInfo map[string]map[string][]*proto.Field

	serviceAddr string

	rateLimitCfg *vo.RateLimit
	bucket       *ratelimit.Bucket

	mode ReplicatorMode

	done chan struct{}
}

type KafkaParams struct {
	Addr                        string
	Topic                       string
	FlatFormat                  bool
	MaxwellFormat               bool
	TopicMode                   TopicMode
	PartitionMode               string
	PartitionColumns            []string
	ExcludeColumns              []string
	WithGtid                    bool
	ProducerPartitionByFallback string
}

// newReplicator creates a new Replicator. The valid fields from the source are:
// Keyspce, Shard, Filter, ExternalMySql.
// The Filter consists of Rules. Each Rule has a Match and an (inner) Filter field.
// The Match can be a table name or, if it begins with a "/", a wildcard.
// The Filter can be empty: get all rows and columns.
// The Filter can be a keyrange, like "-80": get all rows that are within the keyrange.
// The Filter can be a select expression. Examples.
//   "select * from t", same as an empty Filter,
//   "select * from t where in_keyrange('-80')", same as "-80",
//   "select * from t where in_keyrange(col1, 'hash', '-80')",
//   "select col1, col2 from t where...",
//   "select col1, keyspace_id() as ksid from t where...",
//   "select id, count(*), sum(price) from t group by id".
//   Only "in_keyrange" expressions are supported in the where clause.
//   The select expressions can be any valid non-aggregate expressions,
//   or count(*), or sum(col).
//   If the target column name does not match the source expression, an
//   alias like "a+b as targetcol" must be used.
//   More advanced constructs can be used. Please see the table plan builder
//   documentation for more info.
func NewReplicator(source *mysql.ConnParams, dest *mysql.ConnParams, kafkaParams *KafkaParams, filter *proto.Filter, rl *vo.RateLimit, stats *Stats, incrementalMode bool, serviceAddr string) (*Replicator, error) {
	storageConnParams := config.MysqlConfigToConnParams(&config.Conf.Storage)

	mysqld := mysqlctl.NewMysqld(source)
	// TODO -- use common storage dbclient
	var dbClient *DBClient
	if dest != nil {
		dbClient = NewDBClient(dest, stats)
		err := dbClient.Connect()
		if err != nil {
			return nil, err
		}
	}

	storage := NewDBClient(storageConnParams, stats)
	err := storage.Connect()
	if err != nil {
		return nil, err
	}

	var kafkaWriter *kafka.Writer
	if kafkaParams != nil && kafkaParams.Addr != "" {
		kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(kafkaParams.Addr),
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			BatchSize:    100,
			BatchBytes:   104857600,
		}
	}

	id, err := storage.CreateState(source, dest, kafkaParams, filter, serviceAddr, rl)
	if err != nil {
		log.Warn("Create State error: %s", err.Error())
		return nil, err
	}

	dbnames := make(map[string]struct{})
	for _, rule := range filter.Rules {
		if _, ok := dbnames[rule.Dbname]; !ok {
			dbnames[rule.Dbname] = struct{}{}
		}
	}

	var mode ReplicatorMode
	if dbClient != nil && kafkaWriter != nil {
		mode = ModeBoth
	} else if dbClient != nil {
		mode = ModeOLTP
	} else {
		mode = ModeOLAP
	}

	r := Replicator{
		Id:               id,
		filter:           filter,
		sourceConnParams: source,
		destConnParams:   dest,
		kafkaParams:      kafkaParams,
		dbnames:          dbnames,
		stats:            stats,
		dbClient:         dbClient,
		kafkaWriter:      kafkaWriter,
		storage:          storage,
		mysqld:           mysqld,
		cfg:              config.Conf,
		incrementalMode:  incrementalMode,
		mode:             mode,
		done:             make(chan struct{}),
	}

	if rl != nil {
		r.rateLimitCfg = rl
		r.bucket = ratelimit.NewBucketWithRate(float64(rl.Rate), int64(rl.Capacity))
	}

	ctx := context.Background()
	pkInfo, tbFieldInfo, err := r.buildPkAndFieldInfoMap(ctx, "")
	if err != nil {
		return nil, err
	}
	r.pkInfoMap = pkInfo
	r.tbFieldInfo = tbFieldInfo

	if !incrementalMode {
		if err := newCopier(&r).initTablesForCopy(ctx); err != nil {
			r.stats.ErrorCounts.Add([]string{"Copy"}, 1)
			return nil, err
		}
	}

	return &r, nil
}

func LoadFromDB(id int, stats *Stats, serviceAddr string) (*Replicator, error) {
	ctx := context.Background()
	storageConnParams := config.MysqlConfigToConnParams(&config.Conf.Storage)

	storage := NewDBClient(storageConnParams, stats)
	err := storage.Connect()
	if err != nil {
		return nil, err
	}

	result, err := storage.GetTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		err = fmt.Errorf("task not found for id: %d", id)
		return nil, err
	}
	row := result.Rows[0]

	var source, dest mysql.ConnParams
	var filter proto.Filter
	var kafkaParams KafkaParams
	var rl vo.RateLimit
	err = json.Unmarshal(row[8].Raw(), &source)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(row[9].Raw(), &dest)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(row[10].Raw(), &filter)
	if err != nil {
		return nil, err
	}

	dbnames := make(map[string]struct{})
	for _, rule := range filter.Rules {
		if _, ok := dbnames[rule.Dbname]; !ok {
			dbnames[rule.Dbname] = struct{}{}
		}
	}

	err = json.Unmarshal(row[11].Raw(), &kafkaParams)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(row[13].Raw(), &rl)
	if err != nil {
		return nil, err
	}

	var dbClient *DBClient
	if dest.Host != "" {
		dbClient = NewDBClient(&dest, stats)
		err = dbClient.Connect()
		if err != nil {
			return nil, err
		}
	}
	mysqld := mysqlctl.NewMysqld(&source)

	var kafkaWriter *kafka.Writer
	if kafkaParams.Addr != "" {
		kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(kafkaParams.Addr),
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			BatchSize:    100,
			BatchBytes:   104857600,
		}
	}

	var mode ReplicatorMode
	if dbClient != nil && kafkaWriter != nil {
		mode = ModeBoth
	} else if dbClient != nil {
		mode = ModeOLTP
	} else {
		mode = ModeOLAP
	}

	r := Replicator{
		Id:               id,
		filter:           &filter,
		sourceConnParams: &source,
		destConnParams:   &dest,
		kafkaParams:      &kafkaParams,
		dbnames:          dbnames,
		stats:            stats,
		dbClient:         dbClient,
		kafkaWriter:      kafkaWriter,
		storage:          storage,
		mysqld:           mysqld,
		cfg:              config.Conf,
		incrementalMode:  true,
		serviceAddr:      serviceAddr,
		mode:             mode,
		done:             make(chan struct{}),
	}

	if rl.Rate != 0 {
		r.rateLimitCfg = &rl
		r.bucket = ratelimit.NewBucketWithRate(float64(rl.Rate), int64(rl.Capacity))
	}

	pkInfo, tbFieldInfo, err := r.buildPkAndFieldInfoMap(ctx, "")
	if err != nil {
		return nil, err
	}
	r.pkInfoMap = pkInfo
	r.tbFieldInfo = tbFieldInfo

	return &r, nil
}

// Replicate starts a replication stream. It can be in one of three phases:
// 1. Init: If a request is issued with no starting position, we assume that the
// contents of the tables must be copied first. During this phase, the list of
// tables to be copied is inserted into the copy_state table. A successful insert
// gets us out of this phase.
// 2. Copy: If the copy_state table has rows, then we are in this phase. During this
// phase, we repeatedly invoke copyNext until all the tables are copied. After each
// table is successfully copied, it's removed from the copy_state table. We exit this
// phase when there are no rows left in copy_state.
// 3. Replicate: In this phase, we replicate binlog events indefinitely, unless
// a stop position was requested. This phase differs from the Init phase because
// there is a replication position.
// If a request had a starting position, then we go directly into phase 3.
// During these phases, the state of replication is reported as 'Init', 'Copying',
// or 'Running'. They all mean the same thing. The difference in the phases depends
// on the criteria defined above. The different states reported are mainly
// informational. The 'Stopped' state is, however, honored.
// All phases share the same plan building framework. We leverage the fact the
// row representation of a read (during copy) and a binlog event are identical.
// However, there are some subtle differences, explained in the plan builder
// code.
func (r *Replicator) Replicate(ctx context.Context) error {
	err := r.replicate(ctx)
	if err != nil {
		log.Warn("Replicate error: %s", err.Error())
		message := fmt.Sprintf("Error: %s", err.Error())
		r.stats.History.Add(&StatsHistoryRecord{
			Time:    time.Now(),
			Message: message,
		})
		if err := r.setState(BlpError, message); err != nil {
			log.Warn("Failed to set error state: %v", err)
		}
	}
	close(r.done)
	return err
}

func (r *Replicator) replicate(ctx context.Context) error {
	if r.dbClient != nil {
		if err := r.getSettingFKCheck(ctx); err != nil {
			return err
		}
		//defensive guard, should be a no-op since it should happen after copy is done
		defer r.resetFKCheckAfterCopy(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		// This rollback is a no-op. It's here for safety
		// in case the functions below leave transactions open.
		if r.dbClient != nil {
			r.dbClient.Rollback()
		}

		startPos, state, found, err := r.storage.ReadStats(r.Id)
		if err != nil {
			return err
		}

		if !found {
			var err error
			id, err := r.storage.CreateState(r.sourceConnParams, r.destConnParams, r.kafkaParams, r.filter, r.serviceAddr, r.rateLimitCfg)
			if err != nil {
				log.Warn("Create State error: %s", err.Error())
				return err
			}

			if !r.incrementalMode {
				if err := newCopier(r).initTablesForCopy(ctx); err != nil {
					r.stats.ErrorCounts.Add([]string{"Copy"}, 1)
					return err
				}
			}
			r.Id = id
		}

		numTablesToCopy, err := r.storage.ReadCopyStats(r.Id)
		if err != nil {
			return err
		}

		// If any of the operations below changed state to Stopped, we should return.
		if state == BlpStopped {
			return nil
		}

		switch {
		case numTablesToCopy != 0:
			if r.dbClient != nil {
				if err := r.clearFKCheck(ctx); err != nil {
					log.Warn("Unable to clear FK check %v", err)
					return err
				}
			}
			if err := newCopier(r).copyNext(ctx); err != nil {
				r.stats.ErrorCounts.Add([]string{"Copy"}, 1)
				return err
			}
		default:
			if r.dbClient != nil {
				if err := r.resetFKCheckAfterCopy(ctx); err != nil {
					log.Warn("Unable to reset FK check %v", err)
					return err
				}
			}
			if err := r.setState(BlpRunning, ""); err != nil {
				r.stats.ErrorCounts.Add([]string{"Replicate"}, 1)
				return err
			}
			return newPlayer(r, nil, startPos, mysql.Position{}, "replicate").play(ctx)
		}
	}
}

// PrimaryKeyInfo is used to store charset and collation for primary keys where applicable
type PrimaryKeyInfo struct {
	Index      int64 // base 1
	Name       string
	CharSet    string
	Collation  string
	DataType   string
	ColumnType string
}

func (r *Replicator) buildPkAndFieldInfoMap(ctx context.Context, wantedDbname string) (map[string]map[string][]*PrimaryKeyInfo, map[string]map[string][]*proto.Field, error) {
	queryTemplate := "select character_set_name, collation_name, column_name, data_type, column_type, ordinal_position from information_schema.columns where table_schema=%s and table_name=%s;"
	pkInfoMap := make(map[string]map[string][]*PrimaryKeyInfo)
	tbFieldMap := make(map[string]map[string][]*proto.Field)

	for dbname, _ := range r.dbnames {
		if wantedDbname != "" && dbname != wantedDbname {
			continue
		}
		schema, err := r.mysqld.GetSchema(ctx, dbname, []string{"/.*/"}, nil, false)
		if err != nil {
			return nil, nil, err
		}
		pkInfoTable := make(map[string][]*PrimaryKeyInfo)
		tbFieldTable := make(map[string][]*proto.Field)
		for _, td := range schema.TableDefinitions {
			query := fmt.Sprintf(queryTemplate, encodeString(dbname), encodeString(td.Name))
			qr, err := r.mysqld.FetchSuperQuery(ctx, query)
			if err != nil {
				return nil, nil, err
			}
			if len(qr.Rows) == 0 {
				return nil, nil, fmt.Errorf("no data returned from information_schema.columns for table %s", td.Name)
			}

			var pks []string
			if len(td.PrimaryKeyColumns) != 0 {
				pks = td.PrimaryKeyColumns
			} else {
				pks = td.Columns
			}
			var pkInfos []*PrimaryKeyInfo
			for _, pk := range pks {
				charSet := ""
				collation := ""
				var dataType, columnType string
				var idx int64
				for _, row := range qr.Rows {
					columnName := row[2].ToString()
					if strings.EqualFold(columnName, pk) {
						var currentField *proto.Field
						for _, field := range td.Fields {
							if field.Name == pk {
								currentField = field
								break
							}
						}
						if currentField == nil {
							continue
						}
						dataType = row[3].ToString()
						columnType = row[4].ToString()
						idx, err = row[5].ToInt64()
						if err != nil {
							return nil, nil, err
						}
						if sqltypes.IsText(currentField.Type) {
							charSet = row[0].ToString()
							collation = row[1].ToString()
						}
						break
					}
				}
				if dataType == "" || columnType == "" {
					return nil, nil, fmt.Errorf("no dataType/columnType found in information_schema.columns for table %s, column %s", td.Name, pk)
				}
				pkInfos = append(pkInfos, &PrimaryKeyInfo{
					Index:      idx,
					Name:       pk,
					CharSet:    charSet,
					Collation:  collation,
					DataType:   dataType,
					ColumnType: columnType,
				})
			}
			pkInfoTable[td.Name] = pkInfos
			tbFieldTable[td.Name] = td.Fields
		}
		pkInfoMap[dbname] = pkInfoTable
		tbFieldMap[dbname] = tbFieldTable
	}
	return pkInfoMap, tbFieldMap, nil
}

func (r *Replicator) setState(state, message string) error {
	if message != "" {
		r.stats.History.Add(&StatsHistoryRecord{
			Time:    time.Now(),
			Message: message,
		})
	}
	r.State = state
	r.Message = message
	r.stats.State.Set(state)
	err := r.storage.SetState(r.Id, state, message)
	return err
}

// TODO remove
func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}

func (r *Replicator) getSettingFKCheck(ctx context.Context) error {
	qr, err := r.dbClient.ExecuteWithRetry(ctx, "select @@foreign_key_checks;")
	if err != nil {
		return err
	}
	if qr.RowsAffected != 1 || len(qr.Fields) != 1 {
		return fmt.Errorf("unable to select @@foreign_key_checks")
	}
	r.originalFKCheckSetting, err = evalengine.ToInt64(qr.Rows[0][0])
	if err != nil {
		return err
	}
	return nil
}

func (r *Replicator) resetFKCheckAfterCopy(ctx context.Context) error {
	_, err := r.dbClient.ExecuteWithRetry(ctx, fmt.Sprintf("set foreign_key_checks=%d;", r.originalFKCheckSetting))
	return err
}

func (r *Replicator) clearFKCheck(ctx context.Context) error {
	_, err := r.dbClient.ExecuteWithRetry(ctx, "set foreign_key_checks=0;")
	return err
}

func (r *Replicator) Done() chan struct{} {
	return r.done
}

func MessageTruncate(msg string) string {
	// message length is 1000 bytes.
	if len(msg) > 950 {
		return msg[:950] + "..."
	}
	return msg
}
