package http_server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nicholaskh/opendts/log"
	"github.com/nicholaskh/opendts"
	"github.com/nicholaskh/opendts/http_server/vo"
	"github.com/nicholaskh/opendts/mysql"
	"github.com/nicholaskh/opendts/proto"
	"github.com/nicholaskh/opendts/sqltypes"
)

func mysqlToParams(m *vo.Mysql) *mysql.ConnParams {
	return &mysql.ConnParams{
		Host:             m.Host,
		Port:             m.Port,
		Uname:            m.Username,
		Pass:             m.Password,
		DbName:           m.DB,
		Charset:          m.Charset,
		ConnectTimeoutMs: m.ConnectTimeoutMs,
	}
}

func kafkaParamsToInternal(k *vo.Kafka) (*replicator.KafkaParams, error) {
	if k.ProducerPartitionByFallback == "" {
		k.ProducerPartitionByFallback = replicator.DefaultProducerPartitionByFallbackTable
	}

	if k.PartitionMode == "" {
		k.PartitionMode = replicator.PartitionModeColumn
	}
	if k.PartitionMode != replicator.PartitionModeColumn && k.PartitionMode != replicator.PartitionModeTable && k.PartitionMode != replicator.PartitionModePrimaryKey {
		err := fmt.Errorf("invalid partitionMode: %s", k.PartitionMode)
		log.Warn("%v", err)
		return nil, err
	}

	return &replicator.KafkaParams{
		Addr:                        k.Addr,
		Topic:                       k.Topic,
		FlatFormat:                  k.FlatFormat,
		MaxwellFormat:               k.MaxwellFormat,
		TopicMode:                   replicator.TopicMode(k.TopicMode),
		PartitionMode:               k.PartitionMode,
		PartitionColumns:            k.PartitionColumns,
		ExcludeColumns:              k.ExcludeColumns,
		WithGtid:                    k.WithGtid,
		ProducerPartitionByFallback: k.ProducerPartitionByFallback,
	}, nil
}

func filtersToProto(fs []*vo.Filter) (*proto.Filter, error) {
	filter := proto.Filter{
		Rules: make([]*proto.Rule, 0, len(fs)),
	}

	for _, f := range fs {
		if f.Dbname == "" {
			return nil, errors.New("empty dbname in filter")
		}

		rule := proto.Rule{
			Dbname:       f.Dbname,
			Filter:       f.Filter,
			TargetDbname: f.TargetDbname,
		}
		switch {
		case strings.HasPrefix(f.Match, "/"):
			rule.Type = proto.RuleTypeRegexp
			rule.Regexp = strings.Trim(f.Match, "/")
		case strings.ContainsRune(f.Match, ','):
			rule.Type = proto.RuleTypeTables
			tables := strings.Split(strings.Trim(f.Match, ","), ",")
			rule.Tables = make(map[string]struct{}, len(tables))
			for _, tb := range tables {
				rule.Tables[tb] = struct{}{}
			}
		default:
			rule.Type = proto.RuleTypeTable
			rule.Table = f.Match
		}
		filter.Rules = append(filter.Rules, &rule)
	}

	return &filter, nil
}

func rowsToTasks(rows [][]sqltypes.Value) ([]*vo.TaskEntity, error) {
	tasks := make([]*vo.TaskEntity, 0, len(rows))
	for _, row := range rows {
		task, err := rowToTask(row)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func rowToTask(row []sqltypes.Value) (*vo.TaskEntity, error) {
	id, err := row[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return &vo.TaskEntity{
		Id:              int(id),
		SourceAddr:      row[1].ToString(),
		DestAddr:        row[2].ToString(),
		Pos:             row[3].ToString(),
		UpdatedAt:       row[4].ToString(),
		TransactionTime: row[5].ToString(),
		State:           row[6].ToString(),
		Message:         row[7].ToString(),
		ServiceAddr:     row[12].ToString(),
	}, nil
}
