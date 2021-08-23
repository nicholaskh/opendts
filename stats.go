package replicator

import (
	"sync"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/stats"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/util/history"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/util/sync2"
)

const (
	DestTypeMysql = "mysql"
	DestTypeKafka = "kafka"

	SbmPhaseReplay = "replay"
	SbmPhaseStream = "stream"

	EventTypeGTID     = "gtid"
	EventTypeXID      = "xid"
	EventTypeQuery    = "query"
	EventTypeTableMap = "tablemap"
	EventTypeRows     = "rows"
)

type Stats struct {
	// Stats about the player, keys used are BlplQuery and BlplTransaction
	Timings *stats.MultiTimings

	// Last saved status
	lastPositionMutex sync.Mutex
	lastPosition      mysql.Position

	heartbeatMutex sync.Mutex
	heartbeat      int64

	SecondsBehindMaster *stats.GaugesWithSingleLabel
	History             *history.History

	State sync2.AtomicString

	PhaseTimings   *stats.Timings
	QueryTimings   *stats.MultiTimings
	StreamRowCount *stats.Counter
	CopyRowCount   *stats.Counter
	CopyLoopCount  *stats.Counter
	ErrorCounts    *stats.CountersWithMultiLabels
	RelayLogLength *stats.Gauge
	EventsCount    *stats.CountersWithSingleLabel
}

// RecordHeartbeat updates the time the last heartbeat from vstreamer was seen
func (bps *Stats) RecordHeartbeat(tm int64) {
	bps.heartbeatMutex.Lock()
	defer bps.heartbeatMutex.Unlock()
	bps.heartbeat = tm
}

// Heartbeat gets the time the last heartbeat from vstreamer was seen
func (bps *Stats) Heartbeat() int64 {
	bps.heartbeatMutex.Lock()
	defer bps.heartbeatMutex.Unlock()
	return bps.heartbeat
}

// SetLastPosition sets the last replication position.
func (bps *Stats) SetLastPosition(pos mysql.Position) {
	bps.lastPositionMutex.Lock()
	defer bps.lastPositionMutex.Unlock()
	bps.lastPosition = pos
}

// LastPosition gets the last replication position.
func (bps *Stats) LastPosition() mysql.Position {
	bps.lastPositionMutex.Lock()
	defer bps.lastPositionMutex.Unlock()
	return bps.lastPosition
}

// MessageHistory gets all the messages, we store 3 at a time
func (bps *Stats) MessageHistory() []string {
	strs := make([]string, 0, 3)
	for _, h := range bps.History.Records() {
		h1, _ := h.(*StatsHistoryRecord)
		if h1 != nil {
			strs = append(strs, h1.Message)
		}
	}
	return strs
}

// NewStats creates a new Stats structure.
func NewStats() *Stats {
	bps := &Stats{}
	bps.Timings = stats.NewMultiTimings("DBQuery", "DBQuery timings help", []string{"State", "Addr"})
	bps.History = history.New(3)
	bps.SecondsBehindMaster = stats.NewGaugesWithSingleLabel("SecondsBehindMaster", "Seconds behind master gauge help", "phase")
	bps.PhaseTimings = stats.NewTimings("CopyPhaseTiming", "Copy phase timing help", "Phase")
	bps.QueryTimings = stats.NewMultiTimings("QueryTiming", "Query timing help", []string{"Phase", "DestType"})
	bps.StreamRowCount = stats.NewCounter("StreamRows", "Stream rows counter help")
	bps.CopyRowCount = stats.NewCounter("CopyRows", "Copy rows counter help")
	bps.CopyLoopCount = stats.NewCounter("CopyTables", "Copy tables counter help")
	bps.ErrorCounts = stats.NewCountersWithMultiLabels("ErrorCount", "Error count counter help", []string{"type"})
	bps.RelayLogLength = stats.NewGauge("RelayLogLength", "Queue length of relay log")
	bps.EventsCount = stats.NewCountersWithSingleLabel("EventsCount", "Events extracted count", "EventType")
	return bps
}
