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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/evalengine"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqlparser"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/util/hack"
)

// ReplicatorPlan is the execution plan for the replicator. It contains
// plans for all the tables it's replicating. Every phase of vreplication
// builds its own instance of the ReplicatorPlan. This is because the plan
// depends on copyState, which changes on every iteration.
// The table plans within ReplicatorPlan will not be fully populated because
// all the information is not available initially.
// For simplicity, the ReplicatorPlan is immutable.
// Once we get the field info for a table from the stream response,
// we'll have all the necessary info to build the final plan.
// At that time, buildExecutionPlan is invoked, which will make a copy
// of the TablePlan from ReplicatorPlan, and fill the rest
// of the members, leaving the original plan unchanged.
// The constructor is buildReplicatorPlan in table_plan_builder.go
type ReplicatorPlan struct {
	VStreamFilter *proto.Filter
	TargetTables  map[string]map[string]*TablePlan
	TablePlans    map[string]map[string]*TablePlan
	PKInfoMap     map[string]map[string][]*PrimaryKeyInfo
}

// buildExecution plan uses the field info as input and the partially built
// TablePlan for that table to build a full plan.
func (rp *ReplicatorPlan) buildExecutionPlan(fieldEvent *proto.FieldEvent, kafkaParams *KafkaParams) (*TablePlan, error) {
	prelimDb := rp.TablePlans[fieldEvent.Dbname]
	if prelimDb == nil {
		// Unreachable code.
		return nil, fmt.Errorf("plan not found for db %s", fieldEvent.Dbname)
	}
	prelim := prelimDb[fieldEvent.TableName]
	if prelim == nil {
		// Unreachable code.
		return nil, fmt.Errorf("plan not found for table %s", fieldEvent.TableName)
	}
	// If Insert is initialized, then it means that we knew the column
	// names and have already built most of the plan.
	if prelim.Insert != nil {
		tplanv := *prelim
		// We know that we sent only column names, but they may be backticked.
		// If so, we have to strip them out to allow them to match the expected
		// bind var names.
		tplanv.Fields = make([]*proto.Field, 0, len(fieldEvent.Fields))
		for _, fld := range fieldEvent.Fields {
			trimmed := *fld
			trimmed.Name = strings.Trim(trimmed.Name, "`")
			tplanv.Fields = append(tplanv.Fields, &trimmed)
		}
		return &tplanv, nil
	}
	// select * construct was used. We need to use the field names.
	tplan, err := rp.buildFromFields(prelim.Dbname, prelim.TargetName, prelim.TargetDbname, prelim.Lastpk, fieldEvent.Fields)
	if err != nil {
		return nil, err
	}
	tplan.Fields = fieldEvent.Fields

	var pcIdx []int
	if kafkaParams != nil && kafkaParams.PartitionMode == PartitionModeColumn && kafkaParams.PartitionColumns != nil {
		for _, col := range kafkaParams.PartitionColumns {
			for i, field := range fieldEvent.Fields {
				if field.Name == col {
					pcIdx = append(pcIdx, i)
					break
				}
			}
		}
	}
	var ecMap map[string]struct{}
	if kafkaParams != nil && kafkaParams.ExcludeColumns != nil {
		ecMap = make(map[string]struct{}, len(kafkaParams.ExcludeColumns))
		for _, col := range kafkaParams.ExcludeColumns {
			ecMap[col] = struct{}{}
		}
	}

	tplan.PartitionColumnIndex = pcIdx
	tplan.ExcludeColumnMap = ecMap
	return tplan, nil
}

// buildFromFields builds a full TablePlan, but uses the field info as the
// full column list. This happens when the query used was a 'select *', which
// requires us to wait for the field info sent by the source.
func (rp *ReplicatorPlan) buildFromFields(dbname, tableName, targetDbname string, lastpk *sqltypes.Result, fields []*proto.Field) (*TablePlan, error) {
	if _, ok := rp.PKInfoMap[dbname]; !ok {
		return nil, fmt.Errorf("db[%s] not in PKInfoMap", dbname)
	}
	tpb := &tablePlanBuilder{
		targetDbname: sqlparser.NewTableIdent(targetDbname),
		dbname:       sqlparser.NewTableIdent(dbname),
		name:         sqlparser.NewTableIdent(tableName),
		lastpk:       lastpk,
		pkInfos:      rp.PKInfoMap[dbname][tableName],
	}
	for _, field := range fields {
		colName := sqlparser.NewColIdent(field.Name)
		cexpr := &colExpr{
			colName: colName,
			colType: field.Type,
			expr: &sqlparser.ColName{
				Name: colName,
			},
			references: map[string]bool{
				field.Name: true,
			},
		}
		tpb.colExprs = append(tpb.colExprs, cexpr)
	}
	// The following actions are a subset of buildTablePlan.
	if err := tpb.analyzePK(rp.PKInfoMap); err != nil {
		return nil, err
	}
	return tpb.generate(), nil
}

// MarshalJSON performs a custom JSON Marshalling.
func (rp *ReplicatorPlan) MarshalJSON() ([]byte, error) {
	var targets []string
	for k := range rp.TargetTables {
		targets = append(targets, k)
	}
	sort.Strings(targets)
	v := struct {
		VStreamFilter *proto.Filter
		TargetTables  []string
		TablePlans    map[string]map[string]*TablePlan
	}{
		VStreamFilter: rp.VStreamFilter,
		TargetTables:  targets,
		TablePlans:    rp.TablePlans,
	}
	return json.Marshal(&v)
}

// TablePlan is the execution plan for a table within a replicator.
// If the column names are not known at the time of plan building (like
// select *), then only TargetName, SendRule and Lastpk are initialized.
// When the stream returns the field info, those are used as column
// names to build the final plan.
// Lastpk comes from copyState. If it's set, then the generated plans
// are significantly different because any events that fall beyond
// Lastpk must be excluded.
// If column names were known upfront, then all fields of TablePlan
// are built except for Fields. This member is populated only after
// the field info is received from the stream.
// The ParsedQuery objects assume that a map of before and after values
// will be built based on the streaming rows. Before image values will
// be prefixed with a "b_", and after image values will be prefixed
// with a "a_". The TablePlan structure is used during all the phases
// of vreplication: catchup, copy, fastforward, or regular replication.
type TablePlan struct {
	// Dbname, TargetName, SendRule will always be initialized.
	Dbname       string
	TargetName   string
	TargetDbname string

	DbTopic string
	TbTopic string

	PartitionColumnIndex []int
	ExcludeColumnMap     map[string]struct{}

	SendRule *proto.Rule
	// Lastpk will be initialized if it was specified, and
	// will be used for building the final plan after field info
	// is received.
	Lastpk *sqltypes.Result
	// BulkInsertFront, BulkInsertValues and BulkInsertOnDup are used
	// by vcopier. These three parts are combined to build bulk insert
	// statements. This is functionally equivalent to generating
	// multiple statements using the "Insert" construct, but much more
	// efficient for the copy phase.
	BulkInsertFront  *sqlparser.ParsedQuery
	BulkInsertValues *sqlparser.ParsedQuery
	BulkInsertOnDup  *sqlparser.ParsedQuery
	// Insert, Update and Delete are used by vplayer.
	// If the plan is an insertIgnore type, then Insert
	// and Update contain 'insert ignore' statements and
	// Delete is nil.
	Insert *sqlparser.ParsedQuery
	Update *sqlparser.ParsedQuery
	Delete *sqlparser.ParsedQuery
	Fields []*proto.Field
	// PKReferences is used to check if an event changed
	// a primary key column (row move).
	PKReferences []string
}

// MarshalJSON performs a custom JSON Marshalling.
func (tp *TablePlan) MarshalJSON() ([]byte, error) {
	v := struct {
		Dbname       string
		TargetName   string
		SendRule     string
		InsertFront  *sqlparser.ParsedQuery `json:",omitempty"`
		InsertValues *sqlparser.ParsedQuery `json:",omitempty"`
		InsertOnDup  *sqlparser.ParsedQuery `json:",omitempty"`
		Insert       *sqlparser.ParsedQuery `json:",omitempty"`
		Update       *sqlparser.ParsedQuery `json:",omitempty"`
		Delete       *sqlparser.ParsedQuery `json:",omitempty"`
		PKReferences []string               `json:",omitempty"`
	}{
		Dbname:       tp.Dbname,
		TargetName:   tp.TargetName,
		SendRule:     tp.SendRule.Table,
		InsertFront:  tp.BulkInsertFront,
		InsertValues: tp.BulkInsertValues,
		InsertOnDup:  tp.BulkInsertOnDup,
		Insert:       tp.Insert,
		Update:       tp.Update,
		Delete:       tp.Delete,
		PKReferences: tp.PKReferences,
	}
	return json.Marshal(&v)
}

func (tp *TablePlan) applyBulkInsert(rows *proto.VStreamRowsResponse, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	bindvars := make(map[string]*proto.BindVariable, len(tp.Fields))
	var buf strings.Builder
	if err := tp.BulkInsertFront.Append(&buf, nil, nil); err != nil {
		return nil, err
	}
	buf.WriteString(" values ")
	separator := ""
	for _, row := range rows.Rows {
		vals := sqltypes.MakeRowTrusted(tp.Fields, row)
		for i, field := range tp.Fields {
			bindvars["a_"+field.Name] = sqltypes.ValueBindVariable(vals[i])
		}
		buf.WriteString(separator)
		separator = ", "
		tp.BulkInsertValues.Append(&buf, bindvars, nil)
	}
	if tp.BulkInsertOnDup != nil {
		tp.BulkInsertOnDup.Append(&buf, nil, nil)
	}
	return executor(buf.String())
}

func (tp *TablePlan) applyBulkKafka(rows *proto.VStreamRowsResponse, tableName, dbName string, executor func(*KafkaMessage) error) error {
	data := make([]map[string]interface{}, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		bindvars := make(map[string]interface{}, len(tp.Fields))
		vals := sqltypes.MakeRowTrusted(tp.Fields, row)
		for i, field := range tp.Fields {
			bindvars[field.Name] = vals[i].ToString()
		}
		data = append(data, bindvars)
	}

	ts := time.Now().UnixNano() / 1e6
	return executor(&KafkaMessage{
		Table:       tp.TargetName,
		Database:    tp.Dbname,
		ExecuteTime: ts,
		Ts:          ts,
		PkNames:     tp.PKReferences,
		IsDDL:       false,
		Type:        "INSERT",
		Data:        data,
	})
}

func (tp *TablePlan) applyBulkKafkaFlat(rows *proto.VStreamRowsResponse, tableName, dbName string, executor func(map[string]interface{}) error) error {
	ts := time.Now().Unix()
	for _, row := range rows.Rows {
		bindvars := make(map[string]interface{}, len(tp.Fields))
		vals := sqltypes.MakeRowTrusted(tp.Fields, row)
		for i, field := range tp.Fields {
			bindvars[field.Name] = vals[i].ToString()
		}

		bindvars["table"] = tp.TargetName
		bindvars["database"] = tp.Dbname
		bindvars["es"] = ts
		bindvars["ts"] = ts
		bindvars["isDdl"] = false
		bindvars["type"] = "INSERT"
		bindvars["pkNames"] = tp.PKReferences
		bindvars["sql"] = ""

		err := executor(bindvars)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tp *TablePlan) applyChange(rowChange *proto.RowChange, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	// MakeRowTrusted is needed here because Proto3ToResult is not convenient.
	var before, after bool
	bindvars := make(map[string]*proto.BindVariable, len(tp.Fields))
	if rowChange.Before != nil {
		before = true
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
		for i, field := range tp.Fields {
			bindvars["b_"+field.Name] = sqltypes.ValueBindVariable(vals[i])
		}
	}
	if rowChange.After != nil {
		after = true
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
		for i, field := range tp.Fields {
			bindvars["a_"+field.Name] = sqltypes.ValueBindVariable(vals[i])
		}
	}
	switch {
	case !before && after:
		return execParsedQuery(tp.Insert, bindvars, executor)
	case before && !after:
		if tp.Delete == nil {
			return nil, nil
		}
		return execParsedQuery(tp.Delete, bindvars, executor)
	case before && after:
		if !tp.pkChanged(bindvars) {
			return execParsedQuery(tp.Update, bindvars, executor)
		}
		if tp.Delete != nil {
			if _, err := execParsedQuery(tp.Delete, bindvars, executor); err != nil {
				return nil, err
			}
		}
		return execParsedQuery(tp.Insert, bindvars, executor)
	}

	return nil, nil
}

func (tp *TablePlan) applyChangeToKafka(rowEvent *proto.RowEvent, ts int64, kafkaParams *KafkaParams, executor func([]kafka.Message) error) error {
	var before, after bool
	var sqltype string
	bindVarsBefore := make(map[string]interface{}, len(tp.Fields))
	bindVarsAfter := make(map[string]interface{}, len(tp.Fields))

	messages := make([]kafka.Message, 0, len(rowEvent.RowChanges))
	var native interface{}
	var err error
	var vals []sqltypes.Value
	var valsBefore []sqltypes.Value

	var topicName string
	if kafkaParams.TopicMode == TopicModeDB {
		topicName = tp.DbTopic
	} else {
		topicName = tp.TbTopic
	}
	for _, rowChange := range rowEvent.RowChanges {
		before = false
		after = false
		vals = vals[:0]
		valsBefore = valsBefore[:0]
		var partitionKey []byte
		if rowChange.Before != nil {
			before = true
		}
		if rowChange.After != nil {
			after = true
			vals = sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
		}

		if before {
			valsBefore = sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
			for i, field := range tp.Fields {
				if _, ok := tp.ExcludeColumnMap[field.Name]; ok {
					continue
				}
				if kafkaParams.MaxwellFormat {
					if before && after && bytes.Compare(valsBefore[i].Raw(), vals[i].Raw()) == 0 {
						continue
					}
					native, err = evalengine.ToNative2(valsBefore[i])
					if err != nil {
						log.Warn("result to native error: %v", err)
						return err
					}
					bindVarsBefore[field.Name] = native
				} else {
					bindVarsBefore[field.Name] = valsBefore[i].ToString()
				}
			}
		}

		if after {
			for i, field := range tp.Fields {
				if _, ok := tp.ExcludeColumnMap[field.Name]; ok {
					continue
				}
				if kafkaParams.MaxwellFormat {
					native, err = evalengine.ToNative2(vals[i])
					if err != nil {
						log.Warn("result to native error: %v", err)
						return err
					}
					bindVarsAfter[field.Name] = native
				} else {
					bindVarsAfter[field.Name] = vals[i].ToString()
				}
			}
		}

		switch kafkaParams.PartitionMode {
		case PartitionModeColumn:
			if tp.PartitionColumnIndex != nil {
				for _, idx := range tp.PartitionColumnIndex {
					if after {
						if idx > len(vals)-1 {
							return fmt.Errorf("PartitionColumnIndex overflow, idx: %d, value length: %d", idx, len(vals))
						}
						partitionKey = append(partitionKey, vals[idx].Raw()...)
					} else {
						if idx > len(valsBefore)-1 {
							return fmt.Errorf("PartitionColumnIndex overflow, idx: %d, value length: %d", idx, len(valsBefore))
						}
						partitionKey = append(partitionKey, valsBefore[idx].Raw()...)
					}
				}
			} else {
				switch kafkaParams.ProducerPartitionByFallback {
				case ProducerPartitionByFallbackPrimaryKey:
					partitionKey = rowChange.PkValue
				case ProducerPartitionByFallbackTable:
					fallthrough
				default:
					partitionKey = hack.Slice(tp.TargetName)
				}
			}
		case PartitionModeTable:
			partitionKey = hack.Slice(tp.TargetName)
		case PartitionModePrimaryKey:
			partitionKey = rowChange.PkValue
		default:
			// never happen
			return fmt.Errorf("invalid PartitionMode: %d", kafkaParams.PartitionMode)
		}

		if sqltype == "" {
			switch {
			case !before && after:
				sqltype = "INSERT"
			case before && !after:
				sqltype = "DELETE"
			case before && after:
				sqltype = "UPDATE"
			}
		}

		var v []byte
		if kafkaParams.MaxwellFormat {
			m := KafkaMessageMaxwell{
				Table:    tp.TargetName,
				Database: tp.Dbname,
				Ts:       time.Now().Unix(),
				Type:     strings.ToLower(sqltype),
			}
			if before && !after {
				// delete
				m.Data = bindVarsBefore
			} else {
				m.Old = bindVarsBefore
				m.Data = bindVarsAfter
			}
			if kafkaParams.WithGtid {
				m.Gtid = rowEvent.Gtid.String()
			}
			v, err = json.Marshal(m)
			if err != nil {
				return err
			}
		} else {
			m := KafkaMessage{
				Table:       tp.TargetName,
				Database:    tp.Dbname,
				Ts:          time.Now().Unix(),
				Type:        sqltype,
				Old:         []map[string]interface{}{bindVarsBefore},
				Data:        []map[string]interface{}{bindVarsAfter},
				IsDDL:       false,
				ExecuteTime: ts,
				PkNames:     tp.PKReferences,
			}
			if before && !after {
				// delete
				m.Data = []map[string]interface{}{bindVarsBefore}
			} else {
				m.Old = []map[string]interface{}{bindVarsBefore}
				m.Data = []map[string]interface{}{bindVarsAfter}
			}
			if kafkaParams.WithGtid {
				m.Gtid = rowEvent.Gtid.String()
			}
			v, err = json.Marshal(m)
			if err != nil {
				return err
			}
		}

		messages = append(messages, kafka.Message{Topic: topicName, Key: partitionKey, Value: v})
	}
	err = executor(messages)
	if err != nil {
		return fmt.Errorf("apply to kafka[%s] error: %v", topicName, err)
	}
	return nil
}

func (tp *TablePlan) applyChangeToKafkaFlat(rowEvent *proto.RowEvent, ts int64, kafkaParams *KafkaParams, executor func([]kafka.Message) error) error {
	var sqltype string
	var before, after bool
	messages := make([]kafka.Message, 0, len(rowEvent.RowChanges))
	bindVarsAfter := make(map[string]interface{}, len(tp.Fields)+8)
	curTs := time.Now().Unix()
	var topicName string
	if kafkaParams.TopicMode == TopicModeDB {
		topicName = tp.DbTopic
	} else {
		topicName = tp.TbTopic
	}
	var vals []sqltypes.Value
	var valsBefore []sqltypes.Value
	for _, rowChange := range rowEvent.RowChanges {
		vals = vals[:0]
		valsBefore = valsBefore[:0]

		if rowChange.Before != nil {
			before = true
			valsBefore = sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
		}
		if rowChange.After != nil {
			after = true
			vals = sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
			for i, field := range tp.Fields {
				if _, ok := tp.ExcludeColumnMap[field.Name]; ok {
					continue
				}
				bindVarsAfter[field.Name] = vals[i].ToString()
			}
		}

		if sqltype == "" {
			switch {
			case !before && after:
				sqltype = "INSERT"
			case before && !after:
				sqltype = "DELETE"
			case before && after:
				sqltype = "UPDATE"
			}
		}

		bindVarsAfter["table"] = tp.TargetName
		bindVarsAfter["database"] = tp.Dbname
		bindVarsAfter["es"] = ts
		bindVarsAfter["ts"] = curTs
		bindVarsAfter["isDdl"] = false
		bindVarsAfter["type"] = sqltype
		bindVarsAfter["pkNames"] = tp.PKReferences
		bindVarsAfter["sql"] = ""
		if kafkaParams.WithGtid {
			bindVarsAfter["gtid"] = rowEvent.Gtid
		}

		v, err := json.Marshal(bindVarsAfter)
		if err != nil {
			return err
		}

		var partitionKey []byte
		switch kafkaParams.PartitionMode {
		case PartitionModeColumn:
			if tp.PartitionColumnIndex != nil {
				for _, idx := range tp.PartitionColumnIndex {
					if after {
						if idx > len(vals)-1 {
							return fmt.Errorf("PartitionColumnIndex overflow, idx: %d, value length: %d", idx, len(vals))
						}
						partitionKey = append(partitionKey, vals[idx].Raw()...)
					} else {
						if idx > len(valsBefore)-1 {
							return fmt.Errorf("PartitionColumnIndex overflow, idx: %d, value length: %d", idx, len(valsBefore))
						}
						partitionKey = append(partitionKey, valsBefore[idx].Raw()...)
					}
				}
			} else {
				partitionKey = rowChange.PkValue
			}
		case PartitionModeTable:
			partitionKey = hack.Slice(tp.TargetName)
		case PartitionModePrimaryKey:
			partitionKey = rowChange.PkValue
		default:
			// never happen
			return fmt.Errorf("invalid PartitionMode: %d", kafkaParams.PartitionMode)
		}

		messages = append(messages, kafka.Message{Topic: topicName, Key: partitionKey, Value: v})
	}

	return executor(messages)
}

func execParsedQuery(pq *sqlparser.ParsedQuery, bindvars map[string]*proto.BindVariable, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	sql, err := pq.GenerateQuery(bindvars, nil)
	if err != nil {
		return nil, err
	}
	return executor(sql)
}

func (tp *TablePlan) pkChanged(bindvars map[string]*proto.BindVariable) bool {
	for _, pkref := range tp.PKReferences {
		v1, _ := sqltypes.BindVariableToValue(bindvars["b_"+pkref])
		v2, _ := sqltypes.BindVariableToValue(bindvars["a_"+pkref])
		if !valsEqual(v1, v2) {
			return true
		}
	}
	return false
}

func valsEqual(v1, v2 sqltypes.Value) bool {
	if v1.IsNull() && v2.IsNull() {
		return true
	}
	// If any one of them is null, something has changed.
	if v1.IsNull() || v2.IsNull() {
		return false
	}
	// Compare content only if none are null.
	return v1.ToString() == v2.ToString()
}
