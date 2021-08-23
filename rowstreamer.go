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
	"errors"
	"fmt"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqlparser"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
)

// rowStreamer is used for copying the existing rows of a table
// before vreplication begins streaming binlogs. The rowStreamer
// responds to a request with the GTID position as of which it
// streams the rows of a table. This allows vreplication to synchronize
// its events as of the returned GTID before adding the new rows.
// For every set of rows sent, the last pk value is also sent.
// This allows for the streaming to be resumed based on the last
// pk value processed.
type rowStreamer struct {
	ctx    context.Context
	cancel func()

	cp   *mysql.ConnParams
	send func(*proto.VStreamRowsResponse) error

	plan      *Plan
	pkColumns []int
	sendQuery string

	c *copier
}

func newRowStreamer(ctx context.Context, cp *mysql.ConnParams, c *copier) *rowStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &rowStreamer{
		ctx:    ctx,
		cancel: cancel,
		cp:     cp,
		c:      c,
	}
}

func (rs *rowStreamer) Cancel() {
	log.Trace("Rowstreamer Cancel() called")
	rs.cancel()
}

func (rs *rowStreamer) StreamRows(lastpk []sqltypes.Value, query string, send func(*proto.VStreamRowsResponse) error) error {
	var err error
	if err = rs.buildPlan(lastpk, query); err != nil {
		return err
	}

	var conn *snapshotConn
	conn, err = snapshotConnect(rs.ctx, rs.cp)
	defer conn.Close()
	if err != nil {
		log.Warn("%v", err)
		return err
	}
	if _, err = conn.ExecuteFetch("set names binary", 1, false); err != nil {
		log.Warn("%v", err)
		return err
	}

	return rs.streamQuery(conn, send)
}

func (rs *rowStreamer) buildPlan(lastpk []sqltypes.Value, query string) error {
	// This pre-parsing is required to extract the table name
	// and create its metadata.
	_, fromTable, err := analyzeSelect(query)
	if err != nil {
		return err
	}

	dbname := rs.cp.DbName
	tableName := fromTable.String()

	if rs.c.r.tbFieldInfo == nil {
		return errors.New("empty table fields info")
	}
	if rs.c.r.pkInfoMap == nil {
		return errors.New("empty table pk info")
	}

	if _, ok := rs.c.r.tbFieldInfo[dbname]; !ok {
		return fmt.Errorf("table fields not found, table: %s", tableName)
	}
	if _, ok := rs.c.r.tbFieldInfo[dbname][tableName]; !ok {
		return fmt.Errorf("table fields not found, table: %s", tableName)
	}

	fields := rs.c.r.tbFieldInfo[dbname][tableName]

	if pkInfoDb, ok := rs.c.r.pkInfoMap[dbname]; ok {
		if pkInfo, ok := pkInfoDb[tableName]; ok {
			rs.pkColumns = make([]int, 0, len(pkInfo))
			for _, pk := range pkInfo {
				if pk.Index >= int64(len(fields)) {
					return fmt.Errorf("primary key %s index %d refers to non-existent column", pk.Name, pk.Index)
				}
				rs.pkColumns = append(rs.pkColumns, int(pk.Index-1))
			}
		} else {
			return fmt.Errorf("table pks not found, table: %s", tableName)
		}
	} else {
		return fmt.Errorf("table pks not found, db: %s", dbname)
	}

	ti := &Table{
		Name:   tableName,
		Fields: fields,
	}
	// The plan we build is identical to the one for vstreamer.
	// This is because the row format of a read is identical
	// to the row format of a binlog event. So, the same
	// filtering will work.
	rs.plan, err = buildTablePlan(ti, query)
	if err != nil {
		log.Warn("%s", err.Error())
		return err
	}

	if rs.c.r.tbFieldInfo == nil {
		return errors.New("empty table fields info")
	}

	rs.sendQuery, err = rs.buildSelect(lastpk)
	if err != nil {
		return err
	}
	return err
}

func (rs *rowStreamer) buildSelect(lastpk []sqltypes.Value) (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	// We could have used select *, but being explicit is more predictable.
	buf.Myprintf("select ")
	prefix := ""
	for _, col := range rs.plan.Table.Fields {
		buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(col.Name))
		prefix = ", "
	}
	buf.Myprintf(" from %v", sqlparser.NewTableIdent(rs.plan.Table.Name))
	if len(lastpk) != 0 {
		if len(lastpk) != len(rs.pkColumns) {
			return "", fmt.Errorf("primary key values don't match length: %v vs %v", lastpk, rs.pkColumns)
		}
		buf.WriteString(" where ")
		prefix := ""
		// This loop handles the case for composite pks. For example,
		// if lastpk was (1,2), the where clause would be:
		// (col1 = 1 and col2 > 2) or (col1 > 1).
		// A tuple inequality like (col1,col2) > (1,2) ends up
		// being a full table scan for mysql.
		for lastcol := len(rs.pkColumns) - 1; lastcol >= 0; lastcol-- {
			buf.Myprintf("%s(", prefix)
			prefix = " or "
			for i, pk := range rs.pkColumns[:lastcol] {
				buf.Myprintf("%v = ", sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
				lastpk[i].EncodeSQL(buf)
				buf.Myprintf(" and ")
			}
			buf.Myprintf("%v > ", sqlparser.NewColIdent(rs.plan.Table.Fields[rs.pkColumns[lastcol]].Name))
			lastpk[lastcol].EncodeSQL(buf)
			buf.Myprintf(")")
		}
	}
	buf.Myprintf(" order by ", sqlparser.NewTableIdent(rs.plan.Table.Name))
	prefix = ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	return buf.String(), nil
}

func (rs *rowStreamer) streamQuery(conn *snapshotConn, send func(*proto.VStreamRowsResponse) error) error {
	log.Trace("Streaming query: %v\n", rs.sendQuery)
	gtid, err := conn.streamWithSnapshot(rs.ctx, rs.plan.Table.Name, rs.sendQuery)
	if err != nil {
		return err
	}

	// first call the callback with the fields
	flds, err := conn.Fields()
	if err != nil {
		return err
	}
	pkfields := make([]*proto.Field, len(rs.pkColumns))
	for i, pk := range rs.pkColumns {
		pkfields[i] = &proto.Field{
			Name: flds[pk].Name,
			Type: flds[pk].Type,
		}
	}

	err = send(&proto.VStreamRowsResponse{
		Fields:   rs.plan.fields(),
		Pkfields: pkfields,
		Gtid:     gtid,
	})
	if err != nil {
		return fmt.Errorf("stream send error: %v", err)
	}

	response := &proto.VStreamRowsResponse{}
	lastpk := make([]sqltypes.Value, len(rs.pkColumns))
	byteCount := 0
	for {
		// log.Trace("StreamResponse for loop iteration starts")
		select {
		case <-rs.ctx.Done():
			log.Trace("Stream ended because of ctx.Done")
			return fmt.Errorf("stream ended: %v", rs.ctx.Err())
		default:
		}

		row, err := conn.FetchNext()
		if err != nil {
			log.Warn("%v", err)
			return err
		}
		if row == nil {
			break
		}
		// Compute lastpk here, because we'll need it
		// at the end after the loop exits.
		for i, pk := range rs.pkColumns {
			lastpk[i] = row[pk]
		}
		// Reuse the vstreamer's filter.
		ok, filtered, err := rs.plan.filter(row)
		if err != nil {
			log.Warn("%v", err)
			return err
		}
		if ok {
			response.Rows = append(response.Rows, sqltypes.RowToProto3(filtered))
			for _, s := range filtered {
				byteCount += s.Len()
			}
		}

		if byteCount >= *BatchPacketSize {
			response.Lastpk = sqltypes.RowToProto3(lastpk)
			err = send(response)
			if err != nil {
				log.Warn("Rowstreamer send returned error %v", err)
			}
			// empty the rows so we start over, but we keep the
			// same capacity
			response.Rows = nil
			byteCount = 0
		}
	}

	if len(response.Rows) > 0 {
		response.Lastpk = sqltypes.RowToProto3(lastpk)
		err = send(response)
		if err != nil {
			return err
		}
	}

	return nil
}
