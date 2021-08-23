package replicator

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqlparser"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
)

// NOTE 经测试，一个包约为250 Bytes左右，kafka类型的可调大，mysql类型的应尽可能调小
// FIXME 当前先调小PacketSize，后续增加ap类业务的commit event
var PacketSize = flag.Int("stream_packet_size", 25, "Suggested packet size for Replication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount.")
var BatchPacketSize = flag.Int("rowstream_packet_size", 250000, "Suggested packet size for Copy Table streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount.")

// HeartbeatTime is set to slightly below 1s, compared to idleTimeout
// set by player at slightly above 1s. This minimizes conflicts
// between the two timeouts.
var HeartbeatTime = 900 * time.Millisecond

var unexpectedServerError = errors.New("unexpected server EOF")

type streamer struct {
	ctx    context.Context
	cancel func()

	cp *mysql.ConnParams

	conn *BinlogConnection

	// filter: the list of filtering rules. If a rule has a select expression for its filter,
	//   the select list can only reference direct columns. No other expressions are allowed.
	//   The select expression is allowed to contain the special 'keyspace_id()' function which
	//   will return the keyspace id of the row. Examples:
	//   "select * from t", same as an empty Filter,
	//   "select * from t where in_keyrange('-80')", same as "-80",
	//   "select * from t where in_keyrange(col1, 'hash', '-80')",
	//   "select col1, col2 from t where...",
	//   "select col1, keyspace_id() from t where...".
	//   Only "in_keyrange" expressions are supported in the where clause.
	//   Other constructs like joins, group by, etc. are not supported.
	filter *proto.Filter

	plans     map[uint64]*streamerPlan
	pkColumns map[uint64][]int

	// format and pos are updated by parseEvent.
	format mysql.BinlogFormat
	pos    mysql.Position

	curGtid mysql.GTID

	p *player
	r *Replicator
}

// newStreamer creates a new streamer.
// cp: the mysql conn params.
// sh: the schema engine. The streamer uses it to convert the TableMap into field info.
// startPos: a flavor compliant position to stream from. This can also contain the special
//   value "current", which means start from the current position.
func newStreamer(ctx context.Context, cp *mysql.ConnParams, pos mysql.Position, filter *proto.Filter, p *player, r *Replicator) *streamer {
	// FIXME unused cancel
	ctx, cancel := context.WithCancel(ctx)

	return &streamer{
		ctx:       ctx,
		cancel:    cancel,
		cp:        cp,
		pos:       pos,
		filter:    filter,
		plans:     make(map[uint64]*streamerPlan),
		pkColumns: make(map[uint64][]int),
		p:         p,
		r:         r,
	}
}

func (s *streamer) buildTablePlan(id uint64, tm *mysql.TableMap) (*proto.VEvent, error) {
	cols, err := s.buildTableColumns(tm)
	if err != nil {
		return nil, err
	}

	table := &Table{
		Name:   tm.Name,
		Fields: cols,
	}
	plan, err := buildPlan(table, s.filter)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		s.plans[id] = nil
		return nil, nil
	}
	s.plans[id] = &streamerPlan{
		Plan:     plan,
		TableMap: tm,
	}

	if pkInfoDb, ok := s.p.r.pkInfoMap[tm.Database]; ok {
		if pkInfo, ok := pkInfoDb[tm.Name]; ok {
			fields := s.p.r.tbFieldInfo[tm.Database][tm.Name]
			if _, ok := s.pkColumns[id]; !ok {
				s.pkColumns[id] = make([]int, 0, len(pkInfo))
			}
			for _, pk := range pkInfo {
				if pk.Index > int64(len(fields)) {
					return nil, fmt.Errorf("primary key %s index %d refers to non-existent column", pk.Name, pk.Index)
				}
				s.pkColumns[id] = append(s.pkColumns[id], int(pk.Index-1))
			}
		} else {
			return nil, fmt.Errorf("table pks not found, table: %s", tm.Name)
		}
	} else {
		return nil, fmt.Errorf("table pks not found, db: %s", tm.Database)
	}
	return &proto.VEvent{
		Type: proto.VEventType_FIELD,
		FieldEvent: &proto.FieldEvent{
			Dbname:    tm.Database,
			TableName: plan.Table.Name,
			Fields:    plan.fields(),
		},
	}, nil
}

func (s *streamer) buildTableColumns(tm *mysql.TableMap) ([]*proto.Field, error) {
	var fields []*proto.Field
	for i, typ := range tm.Types {
		t, err := sqltypes.MySQLToType(int64(typ), 0)
		if err != nil {
			return nil, fmt.Errorf("unsupported type: %d, position: %d", typ, i)
		}
		fields = append(fields, &proto.Field{
			Name: fmt.Sprintf("@%d", i+1),
			Type: t,
		})
	}

	if s.p.r.tbFieldInfo == nil {
		return nil, errors.New("empty table fields info")
	}

	var tbFields []*proto.Field
	var ok bool
	if _, ok = s.p.r.tbFieldInfo[tm.Database]; !ok {
		return nil, fmt.Errorf("table fields not found, db: %s", tm.Database)
	}
	if tbFields, ok = s.p.r.tbFieldInfo[tm.Database][tm.Name]; !ok {
		return nil, fmt.Errorf("table fields not found, table: %s", tm.Name)
	}

	// check if the schema returned by schema.Engine matches with row.
	for i := range tm.Types {
		// FIXME array out of range
		if !sqltypes.AreTypesEquivalent(fields[i].Type, tbFields[i].Type) {
			return fields, nil
		}
	}

	// Columns should be truncated to match those in tm.
	fields = tbFields[:len(tm.Types)]
	extColInfos, err := s.getExtColInfos(tm.Name, tm.Database)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		typ := strings.ToLower(field.Type.String())
		if typ == "enum" || typ == "set" {
			if extColInfo, ok := extColInfos[field.Name]; ok {
				field.ColumnType = extColInfo.columnType
			}
		}
	}
	return fields, nil
}

// additional column attributes from information_schema.columns. Currently only column_type is used, but
// we expect to add more in the future
type extColInfo struct {
	columnType string
}

func (s *streamer) getExtColInfos(table, database string) (map[string]*extColInfo, error) {
	extColInfos := make(map[string]*extColInfo)
	conn, err := mysql.Connect(s.ctx, s.cp)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	queryTemplate := "select column_name, column_type from information_schema.columns where table_schema=%s and table_name=%s;"
	query := fmt.Sprintf(queryTemplate, encodeString(database), encodeString(table))
	qr, err := conn.ExecuteFetch(query, 10000, false)
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		extColInfo := &extColInfo{
			columnType: row[1].ToString(),
		}
		extColInfos[row[0].ToString()] = extColInfo
	}
	return extColInfos, nil
}

// Stream streams binlog events.
func (s *streamer) StreamFromLast(ctx context.Context, send func([]*proto.VEvent) error) error {
	err := unexpectedServerError
	for err == unexpectedServerError {
		s.conn, err = NewBinlogConnection(s.cp)
		if err != nil {
			if s.conn != nil {
				s.conn.Close()
			}
			return wrapError(err, s.pos)
		}

		var events <-chan mysql.BinlogEvent
		var errs <-chan error
		_, events, errs, err = s.conn.StartBinlogDumpFromCurrent(ctx)
		if err != nil {
			s.conn.Close()
			return wrapError(err, s.pos)
		}
		err = s.parseEvents(ctx, events, errs, send)
		s.conn.Close()
	}
	return wrapError(err, s.pos)
}

func (s *streamer) StreamFromPos(ctx context.Context, send func([]*proto.VEvent) error) error {
	err := unexpectedServerError
	var skipToCurrent bool
	for err == unexpectedServerError || skipToCurrent {
		s.conn, err = NewBinlogConnection(s.cp)
		if err != nil {
			s.conn.Close()
			return wrapError(err, s.pos)
		}

		var events <-chan mysql.BinlogEvent
		var errs <-chan error
		if skipToCurrent {
			_, events, errs, err = s.conn.StartBinlogDumpFromCurrent(ctx)
		} else {
			events, errs, err = s.conn.StartBinlogDumpFromPosition(ctx, s.pos)
		}
		if err != nil {
			s.conn.Close()
			return wrapError(err, s.pos)
		}
		err = s.parseEvents(ctx, events, errs, send)
		if err != nil {
			if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.ERMasterHasPurgedRequiredGtids {
				log.Warn("mysql.ERMasterHasPurgedRequiredGtids: %v", err)
				skipToCurrent = true
			} else {
				log.Warn("%v", err)
				skipToCurrent = false
			}
		} else {
			skipToCurrent = false
		}
		s.conn.Close()
	}

	return wrapError(err, s.pos)
}

func (s *streamer) parseEvents(ctx context.Context, events <-chan mysql.BinlogEvent, errs <-chan error, send func([]*proto.VEvent) error) error {
	// bufferAndTransmit uses bufferedEvents and curSize to buffer events.
	var (
		bufferedEvents []*proto.VEvent
		curSize        int
	)
	// Only the following patterns are possible:
	// BEGIN->ROWs or Statements->GTID->COMMIT. In the case of large transactions, this can be broken into chunks.
	// BEGIN->JOURNAL->GTID->COMMIT
	// GTID->DDL
	// GTID->OTHER
	// HEARTBEAT is issued if there's inactivity, which is likely
	// to heppend between one group of events and another.
	//
	// Buffering only takes row or statement lengths into consideration.
	// Length of other events is considered negligible.
	// If a new row event causes the packet size to be exceeded,
	// all existing rows are sent without the new row.
	// If a single row exceeds the packet size, it will be in its own packet.
	bufferAndTransmit := func(vevent *proto.VEvent) error {
		switch vevent.Type {
		// FIXME ap业务GTID要在row前
		case proto.VEventType_GTID, proto.VEventType_RAW_GTID, proto.VEventType_BEGIN, proto.VEventType_FIELD,
			proto.VEventType_JOURNAL:
			// We never have to send GTID, BEGIN, FIELD events on their own.
			// A JOURNAL event is always preceded by a BEGIN and followed by a COMMIT.
			// So, we don't have to send it right away.
			bufferedEvents = append(bufferedEvents, vevent)
		case proto.VEventType_COMMIT, proto.VEventType_DDL, proto.VEventType_OTHER,
			proto.VEventType_HEARTBEAT, proto.VEventType_VERSION:
			// COMMIT, DDL, OTHER and HEARTBEAT must be immediately sent.
			// Although unlikely, it's possible to get a HEARTBEAT in the middle
			// of a transaction. If so, we still send the partial transaction along
			// with the heartbeat.
			if vevent.Type == proto.VEventType_DDL {
				log.Trace("ddl comming %s", vevent.Statement)
			}
			bufferedEvents = append(bufferedEvents, vevent)
			vevents := bufferedEvents
			bufferedEvents = nil
			curSize = 0
			err := send(vevents)
			if err != nil {
				return err
			}
			if vevent.Type == proto.VEventType_DDL {
				<-s.p.ddlChan
			}
			return nil
		case proto.VEventType_INSERT, proto.VEventType_DELETE, proto.VEventType_UPDATE, proto.VEventType_REPLACE:
			newSize := len(vevent.Dml)
			if curSize+newSize > *PacketSize {
				vevents := bufferedEvents
				bufferedEvents = []*proto.VEvent{vevent}
				curSize = newSize
				return send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		case proto.VEventType_ROW:
			// ROW events happen inside transactions. So, we can chunk them.
			// Buffer everything until packet size is reached, and then send.
			newSize := 0
			for _, rowChange := range vevent.RowEvent.RowChanges {
				if rowChange.Before != nil {
					newSize += len(rowChange.Before.Values)
				}
				if rowChange.After != nil {
					newSize += len(rowChange.After.Values)
				}
			}
			// FIXME 总大小不能超过PacketSize
			if curSize+newSize > *PacketSize {
				/*
					vevents := bufferedEvents
					bufferedEvents = []*proto.VEvent{vevent}
					curSize = newSize
				*/
				vevents := append(bufferedEvents, vevent)
				bufferedEvents = nil
				curSize = 0
				return send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		case proto.VEventType_SAVEPOINT:
			bufferedEvents = append(bufferedEvents, vevent)
		default:
			return fmt.Errorf("unexpected event: %v", vevent)
		}
		return nil
	}

	// Main loop: calls bufferAndTransmit as events arrive.
	timer := time.NewTimer(HeartbeatTime)
	defer timer.Stop()

	for {
		timer.Reset(HeartbeatTime)
		// Drain event if timer fired before reset.
		select {
		case <-timer.C:
		default:
		}

		select {
		case err, ok := <-errs:
			if ok {
				return err
			}
		case ev, ok := <-events:
			if s.r.bucket != nil {
				s.r.bucket.Wait(int64(1))
			}
			if !ok {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				return unexpectedServerError
			}
			vevents, err := s.parseEvent(ev)
			if err != nil {
				log.Warn(err.Error())
				continue
			}
			for _, vevent := range vevents {
				if err := bufferAndTransmit(vevent); err != nil {
					if err == io.EOF {
						return nil
					}
					return fmt.Errorf("error sending event: %v", err)
				}
			}
		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now().Unix()
			if err := bufferAndTransmit(&proto.VEvent{
				Type:        proto.VEventType_HEARTBEAT,
				Timestamp:   now,
				CurrentTime: now,
			}); err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("error sending event: %v", err)
			}
		}
	}
}

// parseEvent parses an event from the binlog and converts it to a list of VEvents.
func (s *streamer) parseEvent(ev mysql.BinlogEvent) ([]*proto.VEvent, error) {
	if !ev.IsValid() {
		return nil, fmt.Errorf("can't parse binlog event: invalid data: %#v", ev)
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if ev.IsFormatDescription() {
		var err error
		s.format, err = ev.Format()
		if err != nil {
			return nil, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
		}
		return nil, nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if s.format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT, which the master sends to tell us the name
		// of the current log file.
		if ev.IsRotate() {
			return nil, nil
		}
		return nil, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
	}

	// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
	ev, _, err := ev.StripChecksum(s.format)
	if err != nil {
		return nil, fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
	}

	var vevents []*proto.VEvent
	switch {
	case ev.IsGTID():
		s.p.r.stats.EventsCount.Add(EventTypeGTID, 1)
		gtid, hasBegin, err := ev.GTID(s.format)
		if err != nil {
			return nil, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
		}
		if hasBegin {
			vevents = append(vevents, &proto.VEvent{
				Type: proto.VEventType_BEGIN,
			})
		}
		if s.pos.GTIDSet == nil {
			// NOTE only support mysql56 GTID
			s.pos, err = mysql.ParsePosition(mysql.Mysql56FlavorID, fmt.Sprintf("%s:%d-%d", gtid.SourceServer().(mysql.SID).String(), 1, gtid.SequenceNumber().(int64)-1))
			if err != nil {
				return nil, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
			}
		}
		s.curGtid = gtid
		s.pos = mysql.AppendGTID(s.pos, gtid)
		if s.r.mode == ModeOLAP {
			return nil, nil
		}
		vevents = append(vevents, &proto.VEvent{
			Type: proto.VEventType_RAW_GTID,
			Gtid: gtid,
		})
	case ev.IsXID():
		s.p.r.stats.EventsCount.Add(EventTypeXID, 1)
		vevents = append(vevents, &proto.VEvent{
			Type: proto.VEventType_GTID,
			Gtid: s.pos,
		}, &proto.VEvent{
			Type: proto.VEventType_COMMIT,
		})
	case ev.IsQuery():
		s.p.r.stats.EventsCount.Add(EventTypeQuery, 1)
		q, err := ev.Query(s.format)
		if err != nil {
			return nil, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
		}

		// Insert/Delete/Update are supported only to be used in the context of external mysql streams where source databases
		// could be using SBR. Vitess itself will never run into cases where it needs to consume non rbr statements.

		switch cat := sqlparser.Preview(q.SQL); cat {
		case sqlparser.StmtInsert:
			mustSend := mustSendStmt(q, s.p.r.dbnames)
			if mustSend {
				vevents = append(vevents, &proto.VEvent{
					Type: proto.VEventType_INSERT,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtUpdate:
			mustSend := mustSendStmt(q, s.p.r.dbnames)
			if mustSend {
				vevents = append(vevents, &proto.VEvent{
					Type: proto.VEventType_UPDATE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtDelete:
			mustSend := mustSendStmt(q, s.p.r.dbnames)
			if mustSend {
				vevents = append(vevents, &proto.VEvent{
					Type: proto.VEventType_DELETE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtReplace:
			mustSend := mustSendStmt(q, s.p.r.dbnames)
			if mustSend {
				vevents = append(vevents, &proto.VEvent{
					Type: proto.VEventType_REPLACE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtBegin:
			if s.r.mode == ModeOLAP {
				return nil, nil
			}
			vevents = append(vevents, &proto.VEvent{
				Type: proto.VEventType_BEGIN,
			})
		case sqlparser.StmtCommit:
			if s.r.mode == ModeOLAP {
				return nil, nil
			}
			vevents = append(vevents, &proto.VEvent{
				Type: proto.VEventType_COMMIT,
			})
		case sqlparser.StmtDDL:
			rule, ok := mustSendDDL(q, s.filter)
			if ok {
				var targetDbname string
				if rule != nil {
					targetDbname = rule.TargetDbname
				}
				vevents = append(vevents, &proto.VEvent{
					Type: proto.VEventType_GTID,
					Gtid: s.pos,
				}, &proto.VEvent{
					Type:         proto.VEventType_DDL,
					Statement:    q.SQL,
					Dbname:       q.Database,
					TargetDbname: targetDbname,
				})
			} else {
				// If the DDL need not be sent, send a dummy OTHER event.
				vevents = append(vevents, &proto.VEvent{
					Type: proto.VEventType_GTID,
					Gtid: s.pos,
				}, &proto.VEvent{
					Type: proto.VEventType_OTHER,
				})
			}
		case sqlparser.StmtSavepoint:
			if s.r.mode == ModeOLAP {
				return nil, nil
			}
			mustSend := mustSendStmt(q, s.p.r.dbnames)
			if mustSend {
				vevents = append(vevents, &proto.VEvent{
					Type:      proto.VEventType_SAVEPOINT,
					Statement: q.SQL,
				})
			}
		case sqlparser.StmtOther, sqlparser.StmtPriv, sqlparser.StmtSet, sqlparser.StmtComment, sqlparser.StmtFlush:
			if s.r.mode == ModeOLAP {
				return nil, nil
			}
			// These are either:
			// 1) DBA statements like REPAIR that can be ignored.
			// 2) Privilege-altering statements like GRANT/REVOKE
			//    that we want to keep out of the stream for now.
			vevents = append(vevents, &proto.VEvent{
				Type: proto.VEventType_GTID,
				Gtid: s.pos,
			}, &proto.VEvent{
				Type: proto.VEventType_OTHER,
			})
		default:
			return nil, fmt.Errorf("unexpected statement type %s in row-based replication: %q", cat, q.SQL)
		}
	case ev.IsTableMap():
		// This is very frequent. It precedes every row event.
		// If it's the first time for a table, we generate a FIELD
		// event, and also cache the plan. Subsequent TableMap events
		// for that table id don't generate VEvents.
		// A schema change will result in a change in table id, which
		// will generate a new plan and FIELD event.
		s.p.r.stats.EventsCount.Add(EventTypeTableMap, 1)
		id := ev.TableID(s.format)

		if _, ok := s.plans[id]; ok {
			return nil, nil
		}

		tm, err := ev.TableMap(s.format)
		if err != nil {
			return nil, err
		}

		rule, err := ruleMatches(tm.Database, tm.Name, s.filter)
		if err != nil {
			log.Warn("rule matches error: %v", err)
			return nil, err
		}
		if rule == nil {
			log.Debug("rule not match for table: %s, filter: %+v", tm.Name, s.filter)
			s.plans[id] = nil
			return nil, nil
		}

		vevent, err := s.buildTablePlan(id, tm)
		if err != nil {
			return nil, err
		}
		if vevent != nil {
			vevents = append(vevents, vevent)
		}
	case ev.IsWriteRows() || ev.IsDeleteRows() || ev.IsUpdateRows():
		// The existence of before and after images can be used to
		// identify statement types. It's also possible that the
		// before and after images end up going to different shards.
		// If so, an update will be treated as delete on one shard
		// and insert on the other.
		s.p.r.stats.EventsCount.Add(EventTypeRows, 1)
		id := ev.TableID(s.format)
		plan := s.plans[id]
		if plan == nil {
			return nil, nil
		}
		rows, err := ev.Rows(s.format, plan.TableMap)
		if err != nil {
			return nil, err
		}

		vevents, err = s.processRowEvent(id, vevents, plan, rows)
		if err != nil {
			return nil, err
		}
	}
	seconds := time.Now().Unix()
	for _, vevent := range vevents {
		vevent.Timestamp = int64(ev.Timestamp())
		vevent.CurrentTime = seconds
	}

	return vevents, nil
}

func (s *streamer) processRowEvent(tableId uint64, vevents []*proto.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*proto.VEvent, error) {
	rowChanges := make([]*proto.RowChange, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		beforeOK, beforeValues, err := s.extractRowAndFilter(plan, row.Identify, rows.IdentifyColumns, row.NullIdentifyColumns)
		if err != nil {
			return nil, err
		}
		afterOK, afterValues, err := s.extractRowAndFilter(plan, row.Data, rows.DataColumns, row.NullColumns)
		if err != nil {
			return nil, err
		}
		if !beforeOK && !afterOK {
			continue
		}
		rowChange := &proto.RowChange{}
		if beforeOK {
			rowChange.Before = sqltypes.RowToProto3(beforeValues)
		}
		if afterOK {
			rowChange.After = sqltypes.RowToProto3(afterValues)
		}
		pkColumns, ok := s.pkColumns[tableId]
		if !ok {
			err := fmt.Errorf("pk columns not for table: %s.%s", plan.TableMap.Database, plan.Table.Name)
			log.Warn(err.Error())
			return nil, err
		}
		// TODO optimize using object pool
		var lastpk []byte
		if len(afterValues) > 0 {
			lastpk = make([]byte, 0, len(pkColumns)*10)
			for _, pk := range pkColumns {
				lastpk = append(lastpk, afterValues[pk].Raw()...)
			}
		}
		rowChange.PkValue = lastpk
		rowChanges = append(rowChanges, rowChange)
	}
	if len(rowChanges) != 0 {
		vevents = append(vevents, &proto.VEvent{
			Type: proto.VEventType_ROW,
			RowEvent: &proto.RowEvent{
				Gtid:       s.curGtid,
				Dbname:     plan.TableMap.Database,
				TableName:  plan.Table.Name,
				RowChanges: rowChanges,
			},
		})
		s.p.r.stats.StreamRowCount.Add(int64(len(rowChanges)))
	}
	return vevents, nil
}

func (s *streamer) extractRowAndFilter(plan *streamerPlan, data []byte, dataColumns, nullColumns mysql.Bitmap) (bool, []sqltypes.Value, error) {
	if len(data) == 0 {
		return false, nil, nil
	}
	values := make([]sqltypes.Value, dataColumns.Count())
	valueIndex := 0
	pos := 0
	for colNum := 0; colNum < dataColumns.Count(); colNum++ {
		if !dataColumns.Bit(colNum) {
			return false, nil, fmt.Errorf("partial row image encountered: ensure binlog_row_image is set to 'full'")
		}
		if nullColumns.Bit(valueIndex) {
			valueIndex++
			continue
		}
		value, l, err := mysql.CellValue(data, pos, plan.TableMap.Types[colNum], plan.TableMap.Metadata[colNum], plan.Table.Fields[colNum].Type)
		if err != nil {
			return false, nil, err
		}
		pos += l
		values[colNum] = value
		valueIndex++
	}
	return plan.filter(values)
}

func wrapError(err error, stopPos mysql.Position) error {
	if err != nil {
		err = fmt.Errorf("stream from source error @ %v: %v", stopPos, err)
		log.Warn(err.Error())
		return err
	}
	log.Trace("stream from source ended @ %v", stopPos)
	return nil
}

type streamerPlan struct {
	*Plan
	TableMap *mysql.TableMap
}
