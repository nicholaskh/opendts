package replicator

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sync"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
)

var serverIDPool = NewIDPool(getRandomInitialServerID())

func getRandomInitialServerID() uint32 {
	// Leave some breathing room below MaxInt32 to generate IDs into
	max := big.NewInt(math.MaxInt32 - 10000)
	id, _ := crand.Int(crand.Reader, max)
	return uint32(id.Int64())
}

type BinlogConnection struct {
	*mysql.Conn
	cp       *mysql.ConnParams
	serverID uint32
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewBinlogConnection(cp *mysql.ConnParams) (*BinlogConnection, error) {
	conn, err := connectForReplication(cp)
	if err != nil {
		return nil, err
	}

	bc := &BinlogConnection{
		Conn:     conn,
		cp:       cp,
		serverID: serverIDPool.Get(),
	}
	log.Trace("new binlog connection: serverID=%d", bc.serverID)
	return bc, nil
}

func connectForReplication(params *mysql.ConnParams) (*mysql.Conn, error) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		return nil, err
	}

	// TODO learn master_binlog_checksum
	if _, err := conn.ExecuteFetch("SET @master_binlog_checksum=@@global.binlog_checksum", 0, false); err != nil {
		return nil, fmt.Errorf("failed to set @master_binlog_checksum=@@global.binlog_checksum: %v", err)
	}

	return conn, nil
}

func (bc *BinlogConnection) SwitchOffMasterArg() error {
	_, err := bc.Conn.ExecuteFetch("CHANGE MASTER TO MASTER_AUTO_POSITION = 0", 0, false)
	return err
}

func (bc *BinlogConnection) StartBinlogDumpFromCurrent(ctx context.Context) (mysql.Position, <-chan mysql.BinlogEvent, <-chan error, error) {
	ctx, bc.cancel = context.WithCancel(ctx)

	masterPosition, err := bc.Conn.MasterPosition()
	if err != nil {
		return mysql.Position{}, nil, nil, fmt.Errorf("failed to get master position: %v", err)
	}

	c, ec, err := bc.StartBinlogDumpFromPosition(ctx, masterPosition)
	return masterPosition, c, ec, err
}

func (bc *BinlogConnection) StartBinlogDumpFromPosition(ctx context.Context, startPos mysql.Position) (<-chan mysql.BinlogEvent, <-chan error, error) {
	ctx, bc.cancel = context.WithCancel(ctx)

	log.Trace("sending binlog dump command: startPos=%v, serverID=%v", startPos, bc.serverID)
	if err := bc.SendBinlogDumpCommand(bc.serverID, startPos); err != nil {
		log.Warn("couldn't send binlog dump command: %v", err)
		return nil, nil, err
	}

	c, ec := bc.streamEvents(ctx)
	return c, ec, nil
}

func (bc *BinlogConnection) streamEvents(ctx context.Context) (chan mysql.BinlogEvent, chan error) {
	// FIXME(alainjobart) I think we can use a buffered channel for better performance.
	eventChan := make(chan mysql.BinlogEvent)
	errChan := make(chan error, 1)

	bc.wg.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Warn("%v", err)
			}
		}()
		defer func() {
			close(eventChan)
			close(errChan)
			bc.wg.Done()
		}()
		for {
			// TODO -- need timeout
			event, err := bc.Conn.ReadBinlogEvent()
			if err != nil {
				if sqlErr, ok := err.(*mysql.SQLError); ok {
					if sqlErr.Number() == mysql.CRServerLost {
						// CRServerLost = Lost connection to MySQL server during query
						// This is not necessarily an error. It could just be that we closed
						// the connection from outside.
						log.Warn("connection closed during binlog stream (possibly intentional): %v", err)
						return
					} else if sqlErr.Number() == mysql.ERMasterHasPurgedRequiredGtids {
						log.Warn("mysql.ERMasterHasPurgedRequiredGtids: %v", err)
						errChan <- err
						return
					}
				}
				log.Warn("read error while streaming binlog events: %v", err)
				return
			}

			select {
			case eventChan <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return eventChan, errChan
}

func (bc *BinlogConnection) Close() {
	if bc != nil && bc.Conn != nil {
		log.Trace("closing binlog socket to unblock reads")
		bc.Conn.Close()

		// bc.cancel is set at the beginning of the StartBinlogDump*
		// methods. If we error out before then, it's nil.
		// Note we also may error out before adding 1 to bc.wg,
		// but then the Wait() still works.
		if bc.cancel != nil {
			log.Trace("waiting for binlog dump thread to end")
			bc.cancel()
			bc.wg.Wait()
			bc.cancel = nil
		}

		log.Trace("closing binlog MySQL client with serverID %v. Will recycle ID.", bc.serverID)
		bc.Conn = nil
		serverIDPool.Put(bc.serverID)
	}
}
