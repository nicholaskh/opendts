package replicator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/evalengine"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/http_server/vo"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
)

const (
	maxLocalTasks = 999999
)

const (
	BlplQuery       = "Query"
	BlplTransaction = "Transaction"

	ReplicationInit    = "Init"
	ReplicationCopying = "Copying"
	BlpStopped         = "Stopped"
	BlpRunning         = "Running"
	BlpError           = "Error"
)

// DBClient is a wrapper on DBClient.
// It allows us to retry a failed transactions on lock errors.
type DBClient struct {
	connParams *mysql.ConnParams
	dbConn     *mysql.Conn

	addr string

	stats         *Stats
	InTransaction bool
	startTime     time.Time
	queries       []string

	canSetGtid bool
}

func NewDBClient(params *mysql.ConnParams, stats *Stats) *DBClient {
	return &DBClient{
		connParams: params,
		stats:      stats,
		addr:       fmt.Sprintf("%s:%d", params.Host, params.Port),
	}
}

func (c *DBClient) SetNextGtid(ctx context.Context, gtid string) error {
	if c.canSetGtid {
		if _, err := c.ExecuteWithRetry(ctx, fmt.Sprintf("SET @@session.gtid_next='%s'", gtid)); err != nil {
			log.Warn("set gtid_next error: %v, gtid: %s", err, gtid)
			return err
		}
		c.canSetGtid = false
	}
	return nil
}

func (c *DBClient) Begin() error {
	if c.InTransaction {
		return nil
	}

	_, err := c.dbConn.ExecuteFetch("begin", 1, false)
	if err != nil {
		log.Warn("BEGIN failed w/ error %v", err)
		c.handleError(err)
		return err
	}

	c.queries = append(c.queries, "begin")
	c.InTransaction = true
	c.canSetGtid = false
	c.startTime = time.Now()
	return nil
}

func (c *DBClient) Commit() error {
	_, err := c.dbConn.ExecuteFetch("commit", 1, false)
	if err != nil {
		log.Warn("COMMIT failed w/ error %v", err)
		c.dbConn.Close()
		return err
	}

	c.InTransaction = false
	c.canSetGtid = true
	c.queries = nil
	// FIXME error in ap case
	c.stats.Timings.Record([]string{BlplTransaction, c.addr}, c.startTime)
	return nil
}

func (c *DBClient) Rollback() error {
	if !c.InTransaction {
		return nil
	}

	_, err := c.dbConn.ExecuteFetch("rollback", 1, false)
	if err != nil {
		log.Warn("ROLLBACK failed w/ error %v", err)
		c.dbConn.Close()
		return err
	}

	c.InTransaction = false
	// Don't reset queries to allow for vplayer to retry.
	c.canSetGtid = true
	return nil
}

func (c *DBClient) EndDDL() {
	c.InTransaction = false
	c.canSetGtid = true
	return
}

func (c *DBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	defer c.stats.Timings.Record([]string{BlplQuery, c.addr}, time.Now())

	if !c.InTransaction {
		c.queries = []string{query}
	} else {
		c.queries = append(c.queries, query)
	}

	mqr, err := c.dbConn.ExecuteFetch(query, maxrows, true)
	if err != nil {
		log.Trace("ExecuteFetch failed w/ error %v", err)
		c.handleError(err)
		return nil, err
	}

	return mqr, nil
}

// Execute is ExecuteFetch without the maxrows.
func (c *DBClient) Execute(query string) (*sqltypes.Result, error) {
	// Number of rows should never exceed relayLogMaxItems.
	return c.ExecuteFetch(query, *relayLogMaxItems)
}

func (c *DBClient) ExecuteWithRetry(ctx context.Context, query string) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	var err error
	switch query {
	case "begin":
		err = c.Begin()
	case "commit":
		err = c.Commit()
	default:
		qr, err = c.Execute(query)
	}
	for err != nil {
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.ERLockDeadlock || sqlErr.Number() == mysql.ERLockWaitTimeout || sqlErr.Number() == mysql.CRServerGone {
			log.Trace("retryable error: %v, waiting for %v and retrying", sqlErr, dbLockRetryDelay)
			if err := c.Rollback(); err != nil {
				return nil, err
			}
			time.Sleep(dbLockRetryDelay)
			// Check context here. Otherwise this can become an infinite loop.
			select {
			case <-ctx.Done():
				return nil, io.EOF
			default:
			}
			qr, err = c.Retry()
			continue
		}

		log.Warn("ExecuteFetch failed w/ error %v", err)
		return qr, err
	}
	return qr, nil
}

func (c *DBClient) Retry() (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	for _, q := range c.queries {
		if q == "begin" {
			if err := c.Begin(); err != nil {
				return nil, err
			}
			continue
		}
		// Number of rows should never exceed relayLogMaxItems.
		result, err := c.ExecuteFetch(q, *relayLogMaxItems)
		if err != nil {
			return nil, err
		}
		qr = result
	}
	return qr, nil
}

func (c *DBClient) handleError(err error) {
	if mysql.IsConnErr(err) {
		c.Close()
		c.Connect()
	}
}

func (c *DBClient) DBName() string {
	return c.connParams.DbName
}

func (c *DBClient) CreateState(source *mysql.ConnParams, dest *mysql.ConnParams, kafkaParams *KafkaParams, filter *proto.Filter, serviceAddr string, rl *vo.RateLimit) (int, error) {
	sourceConnectParams, err := json.Marshal(source)
	if err != nil {
		return 0, err
	}
	destConnectParams, err := json.Marshal(dest)
	if err != nil {
		return 0, err
	}

	filterBytes, err := json.Marshal(filter)
	if err != nil {
		return 0, err
	}

	kafkaParamsBytes, err := json.Marshal(kafkaParams)
	if err != nil {
		return 0, err
	}

	rlBytes, err := json.Marshal(rl)
	if err != nil {
		return 0, err
	}

	var destAddr string
	if dest != nil {
		destAddr = fmt.Sprintf("%s:%d", dest.Host, dest.Port)
	}

	query := "set sql_mode='NO_BACKSLASH_ESCAPES'"
	if _, err = c.Execute(query); err != nil {
		return 0, fmt.Errorf("could not set sql_mode to NO_BACKSLASH_ESCAPES: %s: %v", query, err)
	}

	query = fmt.Sprintf("insert into replicator.replication (source_addr,dest_addr,transaction_time,state,source_connect_params,dest_connect_params,filter,kafka_params,service_addr,rate_limit) values('%s:%d','%s',%d,'%s','%s','%s','%s','%s','%s','%s')",
		source.Host, source.Port, destAddr, 0, ReplicationInit, string(sourceConnectParams), string(destConnectParams), string(filterBytes), string(kafkaParamsBytes), serviceAddr, string(rlBytes))

	var qr *sqltypes.Result
	if qr, err = c.ExecuteFetch(query, 1); err != nil {
		return 0, fmt.Errorf("could not create state: %s: %v", query, err)
	}

	if qr.InsertID == 0 {
		return 0, errors.New("create replicator.replication error, no id returned")
	}
	return int(qr.InsertID), err
}

func (c *DBClient) ListLocalTaskIds(addr string) (*sqltypes.Result, error) {
	query := fmt.Sprintf("select id from replicator.replication where service_addr='%s'", addr)
	return c.ExecuteFetch(query, maxLocalTasks)
}

func (c *DBClient) ListTasks(ctx context.Context, perPage, page int) (*sqltypes.Result, error) {
	query := fmt.Sprintf("select id,source_addr,dest_addr,pos,updated_at,transaction_time,state,message,source_connect_params,dest_connect_params,filter,kafka_params,service_addr,rate_limit from replicator.replication limit %d, %d", perPage*(page-1), perPage)
	return c.ExecuteWithRetry(ctx, query)
}

func (c *DBClient) ListLocalTasks(ctx context.Context, addr string) (*sqltypes.Result, error) {
	query := fmt.Sprintf("select id,source_addr,dest_addr,pos,updated_at,transaction_time,state,message,source_connect_params,dest_connect_params,filter,kafka_params,service_addr,rate_limit from replicator.replication where service_addr='%s'", addr)
	return c.ExecuteWithRetry(ctx, query)
}

func (c *DBClient) GetTask(ctx context.Context, id int) (*sqltypes.Result, error) {
	query := fmt.Sprintf("select id,source_addr,dest_addr,pos,updated_at,transaction_time,state,message,source_connect_params,dest_connect_params,filter,kafka_params,service_addr,rate_limit from replicator.replication where id=%d", id)
	return c.ExecuteWithRetry(ctx, query)
}

func (c *DBClient) DeleteTask(ctx context.Context, id int) error {
	query := fmt.Sprintf("delete from replicator.replication where id=%d", id)
	_, err := c.ExecuteWithRetry(ctx, query)
	return err
}

func (c *DBClient) ReadStats(uid int) (mysql.Position, string, bool, error) {
	query := fmt.Sprintf("select pos, state from replicator.replication where id=%v", uid)
	qr, err := c.ExecuteFetch(query, 1)
	if err != nil {
		return mysql.Position{}, "", false, fmt.Errorf("error %v in selecting replication settings %v", err, query)
	}

	if len(qr.Rows) == 0 {
		return mysql.Position{}, "", false, nil
	}
	vrRow := qr.Rows[0]

	startPos, err := mysql.DecodePosition(vrRow[0].ToString())
	if err != nil {
		return mysql.Position{}, "", false, fmt.Errorf("failed to parse pos column: %v", err)
	}

	return startPos, vrRow[1].ToString(), true, nil
}

func (c *DBClient) ReadCopyStats(uid int) (int64, error) {
	query := fmt.Sprintf("select count(*) from replicator.copy_state where repl_id=%d", uid)
	qr, err := c.ExecuteFetch(query, 1)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return 0, fmt.Errorf("unexpected result from %s: %v", query, qr)
	}
	numTablesToCopy, err := evalengine.ToInt64(qr.Rows[0][0])
	if err != nil {
		return 0, err
	}

	return numTablesToCopy, nil
}

func (c *DBClient) SetMessage(id int, message string) error {
	query := fmt.Sprintf("update replicator.replication set message=%v where id=%v", encodeString(MessageTruncate(message)), id)
	if _, err := c.Execute(query); err != nil {
		return fmt.Errorf("could not set message: %v: %v", query, err)
	}
	return nil
}

func (c *DBClient) SetState(id int, state, message string) error {
	query := fmt.Sprintf("update replicator.replication set state='%v', message=%v where id=%v", state, encodeString(MessageTruncate(message)), id)
	if _, err := c.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set state: %v: %v", query, err)
	}
	return nil
}

func (c *DBClient) Connect() error {
	var err error
	ctx := context.Background()
	c.dbConn, err = mysql.Connect(ctx, c.connParams)
	if err != nil {
		return fmt.Errorf("error in connecting to mysql db with connection %v, err %v", c.dbConn, err)
	}
	return nil
}

func (c *DBClient) Close() {
	c.dbConn.Close()
}
