package replicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
)

// player replays binlog events by pulling them from a vstreamer.
type player struct {
	r         *Replicator
	startPos  mysql.Position
	stopPos   mysql.Position
	copyState map[string]map[string]*sqltypes.Result

	replicatorPlan *ReplicatorPlan
	tablePlans     map[string]map[string]*TablePlan

	pos mysql.Position
	// unsavedEvent is set any time we skip an event without
	// saving, which is on an empty commit.
	// If nothing else happens for idleTimeout since timeLastSaved,
	// the position of the unsavedEvent gets saved.
	unsavedEvent *proto.VEvent
	// timeLastSaved is set every time a GTID is saved.
	timeLastSaved time.Time
	// lastTimestamp is the last timestamp seen so far.
	lastTimestamp int64
	// canAcceptStmtEvents is set to true if the current player can accept events in statement mode. Only true for filters that are match all.
	canAcceptStmtEvents bool
	phase               string
	ddlChan             chan bool

	appliers []Applier
}

func newPlayer(r *Replicator, copyState map[string]map[string]*sqltypes.Result, startPos mysql.Position, stopPos mysql.Position, phase string) *player {
	var appliers []Applier
	if r.dbClient != nil {
		appliers = append(appliers, &ApplierMysql{
			dbClient: r.dbClient,
		})
	}

	if r.kafkaWriter != nil {
		appliers = append(appliers, &ApplierKafka{
			kafkaWriter: r.kafkaWriter,
		})
	}

	return &player{
		r:             r,
		startPos:      startPos,
		pos:           startPos,
		stopPos:       stopPos,
		copyState:     copyState,
		timeLastSaved: time.Now(),
		tablePlans:    make(map[string]map[string]*TablePlan),
		phase:         phase,
		ddlChan:       make(chan bool),
		appliers:      appliers,
	}
}

// play is the entry point for playing binlogs.
func (p *player) play(ctx context.Context) error {
	if !p.stopPos.IsZero() && p.startPos.AtLeast(p.stopPos) {
		log.Trace("Stop position %v already reached: %v", p.startPos, p.stopPos)
		return nil
	}

	plan, err := buildReplicatorPlan(p.r.filter, p.r.pkInfoMap, p.copyState)
	if err != nil {
		p.r.stats.ErrorCounts.Add([]string{"Plan"}, 1)
		return err
	}
	p.replicatorPlan = plan

	// We can't run in statement mode if there are filters defined.
	p.canAcceptStmtEvents = true
	for _, rule := range p.r.filter.Rules {
		if rule.Filter != "" || strings.HasPrefix(rule.Regexp, "/.*") {
			p.canAcceptStmtEvents = false
			break
		}
	}

	return p.fetchAndApply(ctx)
}

// fetchAndApply performs the fetching and application of the binlogs.
// This is done by two different threads. The fetcher thread pulls
// events from the vstreamer and adds them to the relayLog.
// The applyEvents thread pulls accumulated events from the relayLog
// to apply them to mysql. The reason for this separation is because
// commits are slow during apply. So, more events can accumulate in
// the relay log during a commit. In such situations, the next iteration
// of apply combines all the transactions in the relay log into a single
// one. This allows for the apply thread to catch up more quickly if
// a backlog builds up.
func (p *player) fetchAndApply(ctx context.Context) (err error) {
	log.Trace("Starting VReplication player id: %v, startPos: %v, filter: %v", p.r.Id, p.startPos, p.r.filter)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	relay := newRelayLog(context.Background(), *relayLogMaxItems, *relayLogMaxSize, p.r.stats)

	streamErr := make(chan error, 1)
	if p.startPos.IsZero() {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Warn("%v", err)
					debug.PrintStack()
				}
			}()
			streamErr <- newStreamer(ctx, p.r.sourceConnParams, mysql.Position{}, p.replicatorPlan.VStreamFilter, p, p.r).StreamFromLast(ctx, func(events []*proto.VEvent) error {
				return relay.Send(events)
			})
		}()
	} else {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Warn("%v", err)
					debug.PrintStack()
				}
			}()
			streamErr <- newStreamer(ctx, p.r.sourceConnParams, p.startPos, p.replicatorPlan.VStreamFilter, p, p.r).StreamFromPos(ctx, func(events []*proto.VEvent) error {
				return relay.Send(events)
			})
		}()
	}

	applyErr := make(chan error, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Warn("%v", err)
			}
		}()
		applyErr <- p.applyEvents(ctx, relay)
	}()

	select {
	case err := <-applyErr:
		defer func() {
			// cancel and wait for the other thread to finish.
			cancel()
			<-streamErr
		}()

		// If the apply thread ends with io.EOF, it means either the Engine
		// is shutting down and canceled the context, or stop position was reached,
		// or a journal event was encountered.
		// If so, we return nil which will cause the controller to not retry.
		if err == io.EOF {
			return nil
		}
		return err
	case err := <-streamErr:
		defer func() {
			// cancel and wait for the other thread to finish.
			cancel()
			<-applyErr
		}()
		// If context is done, don't return an error.
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		// If the stream ends normally we have to return an error indicating
		// that the controller has to retry a different vttablet.
		if err == nil || err == io.EOF {
			return errors.New("stream ended")
		}
		return err
	}
}

// applyStmtEvent applies an actual DML statement received from the source, directly onto the backend database
func (p *player) applyStmtEvent(ctx context.Context, event *proto.VEvent) error {
	sql := event.Statement
	if sql == "" {
		sql = event.Dml
	}
	if p.canAcceptStmtEvents {
		start := time.Now()
		var err error
		if p.r.dbClient != nil {
			_, err = p.r.dbClient.ExecuteWithRetry(ctx, sql)
			p.r.stats.QueryTimings.Record([]string{p.phase, DestTypeMysql}, start)
		}
		// TODO kafka produce
		return err
	}
	return fmt.Errorf("filter rules are not supported for SBR replication: %v", p.r.filter.Rules)
}

func (p *player) applyRowEvent(ctx context.Context, rowEvent *proto.RowEvent, ts int64) error {
	tplanDb := p.tablePlans[rowEvent.Dbname]
	if tplanDb == nil {
		return fmt.Errorf("unexpected event on db %s", rowEvent.Dbname)
	}
	tplan := tplanDb[rowEvent.TableName]
	if tplan == nil {
		return fmt.Errorf("unexpected event on table %s", rowEvent.TableName)
	}
	if p.r.dbClient != nil {
		for _, change := range rowEvent.RowChanges {
			_, err := tplan.applyChange(change, func(sql string) (*sqltypes.Result, error) {
				start := time.Now()
				qr, err := p.r.dbClient.ExecuteWithRetry(ctx, sql)
				p.r.stats.QueryTimings.Record([]string{p.phase, DestTypeMysql}, start)
				return qr, err
			})
			if err != nil {
				return err
			}
		}
	}

	if p.r.kafkaWriter != nil {
		if p.r.kafkaParams.FlatFormat {
			err := tplan.applyChangeToKafkaFlat(rowEvent, ts, p.r.kafkaParams,
				func(messages []kafka.Message) error {
					start := time.Now()
					err := p.r.kafkaWriter.WriteMessages(ctx, messages...)
					p.r.stats.QueryTimings.Record([]string{p.phase, DestTypeKafka}, start)
					return err
				})

			if err != nil {
				return err
			}
		} else {
			err := tplan.applyChangeToKafka(rowEvent, ts, p.r.kafkaParams,
				func(messages []kafka.Message) error {
					start := time.Now()
					err := p.r.kafkaWriter.WriteMessages(ctx, messages...)
					p.r.stats.QueryTimings.Record([]string{p.phase, DestTypeKafka}, start)
					return err
				})

			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *player) updatePos(ctx context.Context, ts int64) (err error) {
	update := GenerateUpdatePos(p.r.Id, p.pos, time.Now().Unix(), ts)
	if _, err := p.r.storage.ExecuteWithRetry(ctx, update); err != nil {
		return fmt.Errorf("error %v updating position", err)
	}
	p.unsavedEvent = nil
	p.timeLastSaved = time.Now()
	p.r.stats.SetLastPosition(p.pos)
	return nil
}

func (p *player) recordHeartbeat(ctx context.Context) (err error) {
	tm := time.Now().Unix()
	p.r.stats.RecordHeartbeat(tm)
	update, err := GenerateUpdateTime(p.r.Id, tm)
	if err != nil {
		return err
	}
	if _, err := p.r.storage.ExecuteWithRetry(ctx, update); err != nil {
		return fmt.Errorf("error %v updating time", err)
	}
	return nil
}

func GenerateUpdatePos(uid int, pos mysql.Position, timeUpdated int64, txTimestamp int64) string {
	updatedAt := time.Unix(timeUpdated, 0)
	txTime := time.Unix(txTimestamp, 0)
	if txTimestamp != 0 {
		return fmt.Sprintf(
			"update replicator.replication set pos=%v, updated_at='%s', transaction_time='%s', message='' where id=%v",
			encodeString(mysql.EncodePosition(pos)), updatedAt.Format("2006-01-02 15:04:05"), txTime.Format("2006-01-02 15:04:05"), uid)
	}

	return fmt.Sprintf(
		"update replicator.replication set pos=%v, updated_at='%s', message='' where id=%v",
		encodeString(mysql.EncodePosition(pos)), updatedAt.Format("2006-01-02 15:04:05"), uid)
}

func GenerateUpdateTime(uid int, timeUpdated int64) (string, error) {
	updatedAt := time.Unix(timeUpdated, 0)
	if timeUpdated == 0 {
		return "", fmt.Errorf("timeUpdated cannot be zero")
	}
	return fmt.Sprintf("update replicator.replication set updated_at='%s' where id=%v", updatedAt.Format("2006-01-02 15:04:05"), uid), nil
}

// applyEvents is the main thread that applies the events. It has the following use

// applyEvents is the main thread that applies the events. It has the following use
// cases to take into account:
// * Normal transaction that has row mutations. In this case, the transaction
//   is committed along with an update of the position.
// * DDL event: the action depends on the OnDDL setting.
// * OTHER event: the current position of the event is saved.
// * JOURNAL event: if the event is relevant to the current stream, invoke registerJournal
//   of the engine, and terminate.
// * HEARTBEAT: update SecondsBehindMaster.
// * Empty transaction: The event is remembered as an unsavedEvent. If no commits
//   happen for idleTimeout since timeLastSaved, the current position of the unsavedEvent
//   is committed (updatePos).
// * An empty transaction: Empty transactions are necessary because the current
//   position of that transaction may be the stop position. If so, we have to record it.
//   If not significant, we should avoid saving these empty transactions individually
//   because they can cause unnecessary churn and binlog bloat. We should
//   also not go for too long without saving because we should not fall way behind
//   on the current replication position. Additionally, WaitForPos or other external
//   agents could be waiting on that specific position by watching the vreplication
//   record.
// * A group of transactions: Combine them into a single transaction.
// * Partial transaction: Replay the events received so far and refetch from relay log
//   for more.
// * A combination of any of the above: The trickier case is the one where a group
//   of transactions come in, with the last one being partial. In this case, all transactions
//   up to the last one have to be committed, and the final one must be partially applied.
//
// Of the above events, the saveable ones are COMMIT, DDL, and OTHER. Eventhough
// A GTID comes as a separate event, it's not saveable until a subsequent saveable
// event occurs. VStreamer currently sequences the GTID to be sent just before
// a saveable event, but we do not rely on this. To handle this, we only remember
// the position when a GTID is encountered. The next saveable event causes the
// current position to be saved.
//
// In order to handle the above use cases, we use an implicit transaction scheme:
// A BEGIN does not really start a transaction. Ony a ROW event does. With this
// approach, no transaction gets started if an empty one arrives. If a we receive
// a commit, and a we are not in a transaction, we infer that the transaction was
// empty, and remember it as an unsaved event. The next GTID event will reset the
// unsaved event. If the next commit is also an empty transaction, then the latest
// one gets remembered as unsaved. A non-empty transaction, a must-save event,
// or a timeout will eventually cause the next save.
// The timeout (1s) plays another significant role: If the source and target shards of
// the replication are the same, then a commit of an unsaved event will generate
// another empty event. This is an infinite loop, and the timeout prevents
// this from becoming a tight loop.
// TODO(sougou): we can look at recognizing self-generated events and find a better
// way to handle them.
func (p *player) applyEvents(ctx context.Context, relay *relayLog) error {
	defer func() {
		if err := recover(); err != nil {
			log.Warn("%v", err)
		}
	}()

	if p.r.dbClient != nil {
		defer p.r.dbClient.Rollback()
	}

	// If we're not running, set SecondsBehindMaster to be very high.
	// TODO if we also stored the time of the last event, we
	// can estimate this value more accurately.
	// defer p.r.stats.SecondsBehindMaster.Set(math.MaxInt64)
	var sbm int64 = -1
	var sbmReplay int64 = -1
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		items, err := relay.Fetch()
		if err != nil {
			log.Warn("%v", err)
			return err
		}

		// No events were received. This likely means that there's a network partition.
		// So, we should assume we're falling behind.
		if len(items) == 0 {
			behind := time.Now().Unix() - p.lastTimestamp
			p.r.stats.SecondsBehindMaster.Set(SbmPhaseReplay, behind)
		}
		// Empty transactions are saved at most once every idleTimeout.
		// This covers two situations:
		// 1. Fetch was idle for idleTimeout.
		// 2. We've been receiving empty events for longer than idleTimeout.
		// In both cases, now > timeLastSaved. If so, the GTID of the last unsavedEvent
		// must be saved.
		if time.Since(p.timeLastSaved) >= idleTimeout && p.unsavedEvent != nil {
			err := p.updatePos(ctx, p.unsavedEvent.Timestamp)
			if err != nil {
				log.Warn("%v", err)
				continue
			}
			if err := p.r.storage.Commit(); err != nil {
				log.Warn("%v", err)
				continue
			}
		}
		now := time.Now().Unix()
		for i, events := range items {
			for j, event := range events {
				if event.Timestamp != 0 {
					p.lastTimestamp = event.Timestamp
					sbm = event.CurrentTime - event.Timestamp
					sbmReplay = now - event.Timestamp
				}
				mustSave := false
				switch event.Type {
				case proto.VEventType_COMMIT:
					// If we've reached the stop position, we must save the current commit
					// even if it's empty. So, the next applyEvent is invoked with the
					// mustSave flag.
					if !p.stopPos.IsZero() && p.pos.AtLeast(p.stopPos) {
						mustSave = true
						break
					}
					// In order to group multiple commits into a single one, we look ahead for
					// the next commit. If there is one, we skip the current commit, which ends up
					// applying the next set of events as part of the current transaction. This approach
					// also handles the case where the last transaction is partial. In that case,
					// we only group the transactions with commits we've seen so far.
					if hasAnotherCommit(items, i, j+1) {
						continue
					}
				}
				if err := p.applyEvent(ctx, event, mustSave); err != nil {
					if err == io.EOF {
						return nil
					}
					p.r.stats.ErrorCounts.Add([]string{"Apply"}, 1)
					log.Warn("Error applying event: %s", err.Error())
				}
			}
		}
		if sbm >= 0 {
			p.r.stats.SecondsBehindMaster.Set(SbmPhaseStream, sbm)
			p.r.stats.SecondsBehindMaster.Set(SbmPhaseReplay, sbmReplay)
		}
	}
}

func hasAnotherCommit(items [][]*proto.VEvent, i, j int) bool {
	for i < len(items) {
		for j < len(items[i]) {
			// We skip GTID, BEGIN, FIELD, ROW and DMLs.
			switch items[i][j].Type {
			case proto.VEventType_COMMIT:
				return true
			case proto.VEventType_DDL, proto.VEventType_OTHER, proto.VEventType_JOURNAL:
				return false
			}
			j++
		}
		j = 0
		i++
	}
	return false
}

// TODO wrapError
func (p *player) applyEvent(ctx context.Context, event *proto.VEvent, mustSave bool) error {
	switch event.Type {
	case proto.VEventType_RAW_GTID:
		if false && p.r.dbClient != nil && !p.r.dbClient.InTransaction {
			if err := p.r.dbClient.SetNextGtid(ctx, event.Gtid.String()); err != nil {
				return err
			}
		}
	case proto.VEventType_GTID:
		p.pos = event.Gtid.(mysql.Position)
		// A new position should not be saved until a saveable event occurs.
		if p.r.dbClient != nil {
			p.unsavedEvent = nil
		}
	case proto.VEventType_BEGIN:
		// Nothing need to do
	case proto.VEventType_COMMIT:
		if p.r.dbClient != nil {
			if !p.r.dbClient.InTransaction {
				// We're skipping an empty transaction. We may have to save the position on inactivity.
				p.unsavedEvent = event
				return nil
			}
			if err := p.r.dbClient.Commit(); err != nil {
				return err
			}
		}

		var posReached bool
		if posReached = !p.stopPos.IsZero() && p.pos.AtLeast(p.stopPos); mustSave || posReached || time.Since(p.timeLastSaved) >= idleTimeout {
			err := p.updatePos(ctx, event.Timestamp)
			if err != nil {
				return err
			}
		} else {
			p.unsavedEvent = event
		}
		if posReached {
			log.Trace("Stopped at position: %v", p.stopPos)
			return io.EOF
		}
	case proto.VEventType_FIELD:
		if p.r.dbClient != nil {
			if _, err := p.r.dbClient.ExecuteWithRetry(ctx, "begin"); err != nil {
				return err
			}
		}
		tplan, err := p.replicatorPlan.buildExecutionPlan(event.FieldEvent, p.r.kafkaParams)
		if err != nil {
			return err
		}
		if _, ok := p.tablePlans[event.FieldEvent.Dbname]; !ok {
			p.tablePlans[event.FieldEvent.Dbname] = make(map[string]*TablePlan)
		}
		p.tablePlans[event.FieldEvent.Dbname][event.FieldEvent.TableName] = tplan
	case proto.VEventType_INSERT, proto.VEventType_DELETE, proto.VEventType_UPDATE,
		proto.VEventType_REPLACE, proto.VEventType_SAVEPOINT:
		// use event.Statement if available, preparing for deprecation in 8.0
		sql := event.Statement
		if sql == "" {
			sql = event.Dml
		}
		// If the event is for one of the AWS RDS "special" tables, we skip
		if !strings.Contains(sql, " mysql.rds_") {
			// This is a player using statement based replication
			if p.r.dbClient != nil {
				if _, err := p.r.dbClient.ExecuteWithRetry(ctx, "begin"); err != nil {
					return err
				}
			}
			if err := p.applyStmtEvent(ctx, event); err != nil {
				return err
			}
		}
	case proto.VEventType_ROW:
		// This player is configured for row based replication
		if p.r.dbClient != nil {
			if _, err := p.r.dbClient.ExecuteWithRetry(ctx, "begin"); err != nil {
				return err
			}
		} else if p.r.kafkaWriter != nil {
			// TODO Split bucket of streamer and applier
			if p.r.bucket != nil {
				p.r.bucket.Wait(int64(1))
			}
			p.unsavedEvent = event
		}
		if err := p.applyRowEvent(ctx, event.RowEvent, event.Timestamp); err != nil {
			return err
		}
	case proto.VEventType_OTHER:
		if p.r.dbClient != nil && p.r.dbClient.InTransaction {
			// Unreachable
			log.Warn("internal error: player is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: player is in a transaction on event: %v", event)
		}
		// Just update the position.
		err := p.updatePos(ctx, event.Timestamp)
		if err != nil {
			return err
		}
		if !p.stopPos.IsZero() && p.pos.AtLeast(p.stopPos) {
			log.Trace("Stopped at position: %v", p.stopPos)
			return io.EOF
		}
	case proto.VEventType_DDL:
		// TODO when change column name, change it in filter too
		dbname := event.Dbname
		wantedDbname := dbname
		if event.TargetDbname != "" {
			dbname = event.TargetDbname
		}

		if p.r.dbClient != nil {
			if p.r.dbClient.InTransaction {
				// Unreachable
				log.Warn("internal error: player is in a transaction on event: %v", event)
				return fmt.Errorf("internal error: player is in a transaction on event: %v", event)
			}
			if _, err := p.r.dbClient.ExecuteWithRetry(ctx, fmt.Sprintf("USE %s", dbname)); err != nil {
				log.Warn("use db[%s] error: %v", event.Dbname, err)
			} else if _, err := p.r.dbClient.ExecuteWithRetry(ctx, event.Statement); err != nil {
				log.Trace("Ignoring error: %v for DDL: %s", err, event.Statement)
			}
			p.r.dbClient.EndDDL()
		}
		err := p.updatePos(ctx, event.Timestamp)
		if err != nil {
			return err
		}
		if !p.stopPos.IsZero() && p.pos.AtLeast(p.stopPos) {
			log.Trace("Stopped at position: %v", p.stopPos)
			return io.EOF
		}

		//update schema after ddl
		pkInfo, tbFieldInfo, err := p.r.buildPkAndFieldInfoMap(ctx, wantedDbname)
		if err != nil {
			return err
		}
		p.r.pkInfoMap[wantedDbname] = pkInfo[wantedDbname]
		p.r.tbFieldInfo[wantedDbname] = tbFieldInfo[wantedDbname]

		// rebuild plan
		plan, err := buildReplicatorPlan(p.r.filter, p.r.pkInfoMap, p.copyState)
		if err != nil {
			p.r.stats.ErrorCounts.Add([]string{"Plan"}, 1)
			return err
		}
		p.replicatorPlan = plan

		p.ddlChan <- true
	case proto.VEventType_HEARTBEAT:
		if p.r.dbClient != nil && !p.r.dbClient.InTransaction {
			err := p.recordHeartbeat(ctx)
			if err != nil {
				return err
			}
		} else {
			err := p.recordHeartbeat(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
