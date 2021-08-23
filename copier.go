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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	proto2 "github.com/golang/protobuf/proto"
	kafka "github.com/segmentio/kafka-go"

	"github.com/nicholaskh/opendts/log"
	"github.com/nicholaskh/opendts/mysql"
	"github.com/nicholaskh/opendts/proto"
	"github.com/nicholaskh/opendts/sqlparser"
	"github.com/nicholaskh/opendts/sqltypes"
)

var dummyError = errors.New("dummy")
var waitRetryTime = 1 * time.Second

type copier struct {
	r         *Replicator
	tablePlan *TablePlan
}

func newCopier(r *Replicator) *copier {
	return &copier{
		r: r,
	}
}

// initTablesForCopy (phase 1) identifies the list of tables to be copied and inserts
// them into copy_state. If there are no tables to copy, it explicitly stops
// the stream. Otherwise, the copy phase (phase 2) may think that all tables are copied.
// This will cause us to go into the replication phase (phase 3) without a starting position.
func (c *copier) initTablesForCopy(ctx context.Context) error {
	defer c.r.storage.Rollback()

	plan, err := buildReplicatorPlan(c.r.filter, c.r.pkInfoMap, nil)
	if err != nil {
		return err
	}
	if err := c.r.storage.Begin(); err != nil {
		return err
	}

	// Insert the table list only if at least one table matches.
	if len(plan.TargetTables) != 0 {
		var buf strings.Builder
		buf.WriteString("insert into replicator.copy_state(repl_id, dbname, table_name) values ")
		prefix := ""
		for dbname, tables := range plan.TargetTables {
			for tablename := range tables {
				fmt.Fprintf(&buf, "%s(%d, %s, %s)", prefix, c.r.Id, encodeString(dbname), encodeString(tablename))
				prefix = ", "
			}
		}
		if prefix == "" {
			return errors.New("No tables to be copied, pls check the filter")
		}
		if _, err := c.r.storage.ExecuteWithRetry(ctx, buf.String()); err != nil {
			return err
		}
		if err := c.r.setState(ReplicationCopying, ""); err != nil {
			return err
		}
	} else {
		if err := c.r.setState(BlpStopped, "There is nothing to replicate"); err != nil {
			return err
		}
	}
	return c.r.storage.Commit()
}

// copyNext performs a multi-step process on each iteration.
// Step 1: catchup: During this step, it replicates from the source from the last position.
// This is a partial replication: events are applied only to tables or subsets of tables
// that have already been copied. This goes on until replication catches up.
// Step 2: Start streaming. This returns the initial field info along with the GTID
// as of which the snapshot is being streamed.
// Step 3: fastForward: The target is fast-forwarded to the GTID obtained. This should
// be quick because we were mostly caught up as of step 1. This ensures that the
// snapshot of the rows are consistent with the position where the target stopped.
// Step 4: copy rows: Copy the next set of rows from the stream that was started in Step 2.
// This goes on until all rows are copied, or a timeout. In both cases, copyNext
// returns, and the replicator decides whether to invoke copyNext again, or to
// go to the next phase if all the copying is done.
// Steps 2, 3 and 4 are performed by copyTable.
// copyNext also builds the copyState metadata that contains the tables and their last
// primary key that was copied. A nil Result means that nothing has been copied.
// A table that was fully copied is removed from copyState.
func (c *copier) copyNext(ctx context.Context) error {
	qr, err := c.r.storage.ExecuteWithRetry(ctx, fmt.Sprintf("select dbname, table_name, lastpk from replicator.copy_state where repl_id=%d", c.r.Id))
	if err != nil {
		log.Warn("%+v", err)
		return err
	}
	var dbToCopy, tableToCopy string
	copyState := make(map[string]map[string]*sqltypes.Result)
	for _, row := range qr.Rows {
		dbname := row[0].ToString()
		tableName := row[1].ToString()
		lastpk := row[2].ToString()
		if tableToCopy == "" {
			dbToCopy = dbname
			tableToCopy = tableName
		}
		if _, ok := copyState[dbname]; !ok {
			copyState[dbname] = make(map[string]*sqltypes.Result)
		}
		copyState[dbname][tableName] = nil
		if lastpk != "" {
			var r proto.QueryResult
			if err := proto2.UnmarshalText(lastpk, &r); err != nil {
				log.Warn("%+v", err)
				return err
			}
			copyState[dbname][tableName] = sqltypes.Proto3ToResult(&r)
		}
	}
	if len(copyState) == 0 {
		return fmt.Errorf("unexpected: there are no tables to copy")
	}
	if err := c.catchup(ctx, copyState); err != nil {
		log.Warn("%+v", err)
		return err
	}
	log.Trace("finish catchup, copyState: %v", copyState)

	err = dummyError
	for err != nil {
		err = c.copyTable(ctx, dbToCopy, tableToCopy, copyState)
		if err != nil {
			log.Warn("%v", err)
			if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.CRServerLost {
				// get latest lastpk
				var err2 error
				qr, err2 = c.r.storage.ExecuteWithRetry(ctx, fmt.Sprintf("select dbname, table_name, lastpk from replicator.copy_state where repl_id=%d", c.r.Id))
				if err2 != nil {
					log.Warn("%+v", err2)
					return err2
				}
				copyState = make(map[string]map[string]*sqltypes.Result)
				for _, row := range qr.Rows {
					dbname := row[0].ToString()
					tableName := row[1].ToString()
					lastpk := row[2].ToString()
					if _, ok := copyState[dbname]; !ok {
						copyState[dbname] = make(map[string]*sqltypes.Result)
					}
					copyState[dbname][tableName] = nil
					if lastpk != "" {
						var r proto.QueryResult
						if err2 = proto2.UnmarshalText(lastpk, &r); err2 != nil {
							log.Warn("%+v", err2)
							return err2
						}
						copyState[dbname][tableName] = sqltypes.Proto3ToResult(&r)
					}
				}
				if len(copyState) == 0 {
					return fmt.Errorf("unexpected: there are no tables to copy")
				}

				continue
			}
			break
		}
	}

	return nil
}

// catchup replays events to the subset of the tables that have been copied
// until replication is caught up. In order to stop, the seconds behind master has
// to fall below replicationLagTolerance.
func (c *copier) catchup(ctx context.Context, copyState map[string]map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		c.r.stats.PhaseTimings.Record("catchup", time.Now())
	}()

	startPos, _, _, err := c.r.storage.ReadStats(c.r.Id)
	if err != nil {
		return err
	}
	// If there's no start position, it means we're copying the
	// first table. So, there's nothing to catch up to.
	if startPos.IsZero() {
		return nil
	}

	// Start vreplication.
	errch := make(chan error, 1)
	go func() {
		errch <- newPlayer(c.r, copyState, startPos, mysql.Position{}, "catchup").play(ctx)
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	seconds := int64(replicaLagTolerance / time.Second)
	for {
		sbm := c.r.stats.SecondsBehindMaster.Get(SbmPhaseStream)
		if sbm < seconds {
			cancel()
			// Make sure player returns before returning.
			<-errch
			return nil
		}
		select {
		case err := <-errch:
			if err != nil {
				return err
			}
			return io.EOF
		case <-ctx.Done():
			// Make sure vplayer returns before returning.
			<-errch
			return io.EOF
		case <-tkr.C:
		}
	}
}

// copyTable performs the synchronized copy of the next set of rows from
// the current table being copied. Each packet received is transactionally
// committed with the lastpk. This allows for consistent resumability.
func (c *copier) copyTable(ctx context.Context, dbname, tableName string, copyState map[string]map[string]*sqltypes.Result) error {
	if c.r.dbClient != nil {
		defer c.r.dbClient.Rollback()
	}
	defer func() {
		c.r.stats.PhaseTimings.Record("copy", time.Now())
		c.r.stats.CopyLoopCount.Add(1)
	}()

	if copyStateTable, _ := copyState[dbname]; copyStateTable == nil {
		err := fmt.Errorf("db[%s] not in copyState", dbname)
		log.Warn(err.Error())
		return err
	}

	log.Trace("Copying table %s, lastpk: %v", tableName, copyState[dbname][tableName])

	plan, err := buildReplicatorPlan(c.r.filter, c.r.pkInfoMap, nil)
	if err != nil {
		return err
	}

	_, ok := plan.TargetTables[dbname]
	if !ok {
		return fmt.Errorf("plan not found for db: %s, current plans are: %#v", dbname, plan.TargetTables)
	}

	initialPlan, ok := plan.TargetTables[dbname][tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, current plans are: %#v", tableName, plan.TargetTables)
	}

	ctx, cancel := context.WithTimeout(ctx, copyTimeout)
	defer cancel()

	var lastpk []sqltypes.Value
	if lastpkResult := copyState[dbname][tableName]; lastpkResult != nil && len(lastpkResult.Rows) > 0 {
		lastpk = lastpkResult.Rows[0]
	}

	var pkfields []*proto.Field
	var updateCopyState *sqlparser.ParsedQuery
	var bv map[string]*proto.BindVariable
	connParams := c.r.sourceConnParams
	connParams.DbName = dbname
	c.tablePlan = nil // Every rowStreamer starts a new tablePlan
	err = newRowStreamer(ctx, connParams, c).StreamRows(lastpk, initialPlan.SendRule.Filter, func(rows *proto.VStreamRowsResponse) error {
	Loop:
		for {
			select {
			case <-ctx.Done():
				return io.EOF
			default:
				break Loop
			}
		}

		if c.tablePlan == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			if err := c.fastForward(ctx, copyState, rows.Gtid); err != nil {
				log.Warn("%v", err)
				return err
			}
			fieldEvent := &proto.FieldEvent{
				Dbname:    dbname,
				TableName: initialPlan.SendRule.Table,
				Fields:    rows.Fields,
			}
			c.tablePlan, err = plan.buildExecutionPlan(fieldEvent, c.r.kafkaParams)
			if err != nil {
				log.Warn("%v", err)
				return err
			}
			pkfields = rows.Pkfields
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("update replicator.copy_state set lastpk=%a where repl_id=%s and dbname=%s and table_name=%s", ":lastpk", strconv.Itoa(int(c.r.Id)), encodeString(dbname), encodeString(tableName))
			updateCopyState = buf.ParsedQuery()
		}
		if len(rows.Rows) == 0 {
			return nil
		}

		// The number of rows we receive depends on the packet size set
		// for the row streamer. Since the packet size is roughly equivalent
		// to data size, this should map to a uniform amount of pages affected
		// per statement. A packet size of 30K will roughly translate to 8
		// mysql pages of 4K each.
		// FIXME not atomic between dbClient and storage, maybe occure 'duplicate primary key'
		if c.r.dbClient != nil {
			if err := c.r.dbClient.Begin(); err != nil {
				return err
			}
			_, err = c.tablePlan.applyBulkInsert(rows, func(sql string) (*sqltypes.Result, error) {
				start := time.Now()
				qr, err := c.r.dbClient.ExecuteWithRetry(ctx, sql)
				if err != nil {
					return nil, err
				}
				c.r.stats.QueryTimings.Record([]string{"copy", DestTypeMysql}, start)
				c.r.stats.CopyRowCount.Add(int64(qr.RowsAffected))

				return qr, err
			})
			if err != nil {
				return err
			}

			if err := c.r.dbClient.Commit(); err != nil {
				return err
			}
		}
		if c.r.kafkaWriter != nil {
			if c.r.kafkaParams.FlatFormat {
				err = c.tablePlan.applyBulkKafkaFlat(rows, tableName, dbname, func(m map[string]interface{}) error {
					start := time.Now()
					j, err := json.Marshal(m)
					if err != nil {
						return err
					}
					err = c.r.kafkaWriter.WriteMessages(ctx, kafka.Message{
						// TODO use pk
						//Key:   []byte(),
						Value: j,
					})
					c.r.stats.QueryTimings.Record([]string{"copy", DestTypeKafka}, start)
					c.r.stats.CopyRowCount.Add(int64(len(rows.Rows)))

					return err
				})
			} else {
				err = c.tablePlan.applyBulkKafka(rows, tableName, dbname, func(m *KafkaMessage) error {
					start := time.Now()
					j, err := json.Marshal(m)
					if err != nil {
						return err
					}
					err = c.r.kafkaWriter.WriteMessages(ctx, kafka.Message{
						// TODO use pk
						//Key:   []byte(),
						Value: j,
					})
					c.r.stats.QueryTimings.Record([]string{"copy", DestTypeKafka}, start)
					c.r.stats.CopyRowCount.Add(int64(len(rows.Rows)))

					return err
				})
			}
		}

		var buf bytes.Buffer
		err = proto2.CompactText(&buf, &proto.QueryResult{
			Fields: pkfields,
			Rows:   []*proto.Row{rows.Lastpk},
		})
		if err != nil {
			return err
		}
		bv = map[string]*proto.BindVariable{
			"lastpk": {
				Type:  sqltypes.VarBinary,
				Value: buf.Bytes(),
			},
		}
		updateState, err := updateCopyState.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}

		if err := c.r.storage.Begin(); err != nil {
			return err
		}
		if _, err = c.r.storage.ExecuteWithRetry(ctx, "set sql_mode=''"); err != nil {
			return fmt.Errorf("could not set sql_mode to empty: %v", err)
		}
		if _, err := c.r.storage.ExecuteWithRetry(ctx, updateState); err != nil {
			return err
		}

		return c.r.storage.Commit()
	})

	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Trace("Copy of %v stopped at lastpk: %v", tableName, bv)
		return nil
	default:
	}
	if err != nil {
		return err
	}
	log.Trace("Copy of %v finished at lastpk: %v", tableName, bv)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete from replicator.copy_state where repl_id=%s and dbname=%s and table_name=%s", strconv.Itoa(int(c.r.Id)), encodeString(dbname), encodeString(tableName))
	if _, err := c.r.storage.ExecuteWithRetry(ctx, buf.String()); err != nil {
		return err
	}
	return nil
}

func (c *copier) fastForward(ctx context.Context, copyState map[string]map[string]*sqltypes.Result, gtid string) error {
	defer func() {
		c.r.stats.PhaseTimings.Record("fastforward", time.Now())
	}()
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	startPos, _, _, err := c.r.storage.ReadStats(c.r.Id)
	if err != nil {
		return err
	}
	log.Trace("start fast forward, start pos: %s, stop pos: %s", startPos.String(), pos.String())
	if startPos.IsZero() {
		update := GenerateUpdatePos(c.r.Id, pos, time.Now().Unix(), 0)
		_, err := c.r.storage.ExecuteWithRetry(ctx, update)
		return err
	}
	return newPlayer(c.r, copyState, startPos, pos, "fastforward").play(ctx)
}
