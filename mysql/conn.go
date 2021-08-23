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

package mysql

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqlparser"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/util/bucketpool"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/util/sync2"
)

var mysqlServerFlushDelay = flag.Duration("mysql_server_flush_delay", 100*time.Millisecond, "Delay after which buffered response will be flushed to the client.")

const (
	// connBufferSize is how much we buffer for reading and
	// writing. It is also how much we allocate for ephemeral buffers.
	connBufferSize = 16 * 1024

	// packetHeaderSize is the 4 bytes of header per MySQL packet
	// sent over
	packetHeaderSize = 4
)

// Constants for how ephemeral buffers were used for reading / writing.
const (
	// ephemeralUnused means the ephemeral buffer is not in use at this
	// moment. This is the default value, and is checked so we don't
	// read or write a packet while one is already used.
	ephemeralUnused = iota

	// ephemeralWrite means we currently in process of writing from  currentEphemeralBuffer
	ephemeralWrite

	// ephemeralRead means we currently in process of reading into currentEphemeralBuffer
	ephemeralRead
)

// Conn is a connection between a client and a server, using the MySQL
// binary protocol. It is built on top of an existing net.Conn, that
// has already been established.
//
// Use Connect on the client side to create a connection.
// Use NewListener to create a server side and listen for connections.
type Conn struct {
	// fields contains the fields definitions for an on-going
	// streaming query. It is set by ExecuteStreamFetch, and
	// cleared by the last FetchNext().  It is nil if no streaming
	// query is in progress.  If the streaming query returned no
	// fields, this is set to an empty array (but not nil).
	fields []*proto.Field

	// schemaName is the default database name to use. It is set
	// during handshake, and by ComInitDb packets. Both client and
	// servers maintain it. This member is private because it's
	// non-authoritative: the client can change the schema name
	// through the 'USE' statement, which will bypass this variable.
	schemaName string

	// ClientData is a place where an application can store any
	// connection-related data. Mostly used on the server side, to
	// avoid maps indexed by ConnectionID for instance.
	ClientData interface{}

	// conn is the underlying network connection.
	// Calling Close() on the Conn will close this connection.
	// If there are any ongoing reads or writes, they may get interrupted.
	conn net.Conn

	// flavor contains the auto-detected flavor for this client
	// connection. It is unused for server-side connections.
	flavor flavor

	// ServerVersion is set during Connect with the server
	// version.  It is not changed afterwards. It is unused for
	// server-side connections.
	ServerVersion string

	// User is the name used by the client to connect.
	// It is set during the initial handshake.
	User string // For server-side connections, listener points to the server object.

	bufferedReader *bufio.Reader
	flushTimer     *time.Timer

	// Keep track of how and of the buffer we allocated for an
	// ephemeral packet on the read and write sides.
	// These fields are used by:
	// - startEphemeralPacketWithHeader / writeEphemeralPacket methods for writes.
	// - readEphemeralPacket / recycleReadPacket methods for reads.
	currentEphemeralPolicy int
	// currentEphemeralBuffer for tracking allocated temporary buffer for writes and reads respectively.
	// It can be allocated from bufPool or heap and should be recycled in the same manner.
	currentEphemeralBuffer *[]byte

	// Buffered writing has a timer which flushes on inactivity.
	bufferedWriter *bufio.Writer

	// PrepareData is the map to use a prepared statement.
	PrepareData map[uint32]*PrepareData

	// protects the bufferedWriter and bufferedReader
	bufMu sync.Mutex

	// Capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	Capabilities uint32

	// closed is set to true when Close() is called on the connection.
	closed sync2.AtomicBool

	// ConnectionID is set:
	// - at Connect() time for clients, with the value returned by
	// the server.
	// - at accept time for the server.
	ConnectionID uint32

	// StatementID is the prepared statement ID.
	StatementID uint32

	// StatusFlags are the status flags we will base our returned flags on.
	// This is a bit field, with values documented in constants.go.
	// An interesting value here would be ServerStatusAutocommit.
	// It is only used by the server. These flags can be changed
	// by Handler methods.
	StatusFlags uint16

	// CharacterSet is the character set used by the other side of the
	// connection.
	// It is set during the initial handshake.
	// See the values in constants.go.
	CharacterSet uint8

	// Packet encoding variables.
	sequence uint8
}

// splitStatementFunciton is the function that is used to split the statement in cas ef a multi-statement query.
var splitStatementFunction func(blob string) (pieces []string, err error) = sqlparser.SplitStatementToPieces

// PrepareData is a buffer used for store prepare statement meta data
type PrepareData struct {
	ParamsType  []int32
	ColumnNames []string
	PrepareStmt string
	BindVars    map[string]*proto.BindVariable
	StatementID uint32
	ParamsCount uint16
}

// execResult is an enum signifying the result of executing a query
type execResult byte

const (
	execSuccess execResult = iota
	execErr
	connErr
)

// bufPool is used to allocate and free buffers in an efficient way.
var bufPool = bucketpool.New(connBufferSize, MaxPacketSize)

// writersPool is used for pooling bufio.Writer objects.
var writersPool = sync.Pool{New: func() interface{} { return bufio.NewWriterSize(nil, connBufferSize) }}

// newConn is an internal method to create a Conn. Used by client and server
// side for common creation code.
func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn:           conn,
		closed:         sync2.NewAtomicBool(false),
		bufferedReader: bufio.NewReaderSize(conn, connBufferSize),
	}
}

// startWriterBuffering starts using buffered writes. This should
// be terminated by a call to endWriteBuffering.
func (c *Conn) startWriterBuffering() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	c.bufferedWriter = writersPool.Get().(*bufio.Writer)
	c.bufferedWriter.Reset(c.conn)
}

// endWriterBuffering must be called to terminate startWriteBuffering.
func (c *Conn) endWriterBuffering() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter == nil {
		return nil
	}

	defer func() {
		c.bufferedWriter.Reset(nil)
		writersPool.Put(c.bufferedWriter)
		c.bufferedWriter = nil
	}()

	c.stopFlushTimer()
	return c.bufferedWriter.Flush()
}

// getWriter returns the current writer. It may be either
// the original connection or a wrapper. The returned unget
// function must be invoked after the writing is finished.
// In buffered mode, the unget starts a timer to flush any
// buffered data.
func (c *Conn) getWriter() (w io.Writer, unget func()) {
	c.bufMu.Lock()
	if c.bufferedWriter != nil {
		return c.bufferedWriter, func() {
			c.startFlushTimer()
			c.bufMu.Unlock()
		}
	}
	c.bufMu.Unlock()
	return c.conn, func() {}
}

// startFlushTimer must be called while holding lock on bufMu.
func (c *Conn) startFlushTimer() {
	c.stopFlushTimer()
	c.flushTimer = time.AfterFunc(*mysqlServerFlushDelay, func() {
		c.bufMu.Lock()
		defer c.bufMu.Unlock()

		if c.bufferedWriter == nil {
			return
		}
		c.stopFlushTimer()
		c.bufferedWriter.Flush()
	})
}

// stopFlushTimer must be called while holding lock on bufMu.
func (c *Conn) stopFlushTimer() {
	if c.flushTimer != nil {
		c.flushTimer.Stop()
		c.flushTimer = nil
	}
}

// getReader returns reader for connection. It can be *bufio.Reader or net.Conn
// depending on which buffer size was passed to newServerConn.
func (c *Conn) getReader() io.Reader {
	if c.bufferedReader != nil {
		return c.bufferedReader
	}
	return c.conn
}

func (c *Conn) readHeaderFrom(r io.Reader) (int, error) {
	var header [packetHeaderSize]byte
	// Note io.ReadFull will return two different types of errors:
	// 1. if the socket is already closed, and the go runtime knows it,
	//   then ReadFull will return an error (different than EOF),
	//   something like 'read: connection reset by peer'.
	// 2. if the socket is not closed while we start the read,
	//   but gets closed after the read is started, we'll get io.EOF.
	if _, err := io.ReadFull(r, header[:]); err != nil {
		// The special casing of propagating io.EOF up
		// is used by the server side only, to suppress an error
		// message if a client just disconnects.
		if err == io.EOF {
			return 0, err
		}
		if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("io.ReadFull(header size) failed, err: %v", err)
	}

	sequence := uint8(header[3])
	if sequence != c.sequence {
		return 0, fmt.Errorf("invalid sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	return int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16), nil
}

// readEphemeralPacket attempts to read a packet into buffer from sync.Pool.  Do
// not use this method if the contents of the packet needs to be kept
// after the next readEphemeralPacket.
//
// Note if the connection is closed already, an error will be
// returned, and it may not be io.EOF. If the connection closes while
// we are stuck waiting for data, an error will also be returned, and
// it most likely will be io.EOF.
func (c *Conn) readEphemeralPacket() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(fmt.Errorf("readEphemeralPacket: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	r := c.getReader()

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	// Use the bufPool.
	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, fmt.Errorf("io.ReadFull(packet body of length %v) failed, err: %v", length, err)
		}
		return *c.currentEphemeralBuffer, nil
	}

	// Much slower path, revert to allocating everything from scratch.
	// We're going to concatenate a lot of data anyway, can't really
	// optimize this code path easily.
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("io.ReadFull(packet body of length %v) failed, err: %v", length, err)
	}
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < MaxPacketSize {
			break
		}
	}

	return data, nil
}

// readEphemeralPacketDirect attempts to read a packet from the socket directly.
// It needs to be used for the first handshake packet the server receives,
// so we do't buffer the SSL negotiation packet. As a shortcut, only
// packets smaller than MaxPacketSize can be read here.
// This function usually shouldn't be used - use readEphemeralPacket.
func (c *Conn) readEphemeralPacketDirect() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(fmt.Errorf("readEphemeralPacketDirect: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	var r io.Reader = c.conn

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, fmt.Errorf("io.ReadFull(packet body of length %v) failed, err: %v", length, err)
		}
		return *c.currentEphemeralBuffer, nil
	}

	return nil, errors.New("readEphemeralPacketDirect doesn't support more than one packet")
}

// recycleReadPacket recycles the read packet. It needs to be called
// after readEphemeralPacket was called.
func (c *Conn) recycleReadPacket() {
	if c.currentEphemeralPolicy != ephemeralRead {
		// Programming error.
		panic(fmt.Errorf("trying to call recycleReadPacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	if c.currentEphemeralBuffer != nil {
		// We are using the pool, put the buffer back in.
		bufPool.Put(c.currentEphemeralBuffer)
		c.currentEphemeralBuffer = nil
	}
	c.currentEphemeralPolicy = ephemeralUnused
}

// readOnePacket reads a single packet into a newly allocated buffer.
func (c *Conn) readOnePacket() ([]byte, error) {
	r := c.getReader()
	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("io.ReadFull(packet body of length %v) failed, err: %v", length, err)
	}
	return data, nil
}

// readPacket reads a packet from the underlying connection.
// It re-assembles packets that span more than one message.
// This method returns a generic error, not a SQLError.
func (c *Conn) readPacket() ([]byte, error) {
	// Optimize for a single packet case.
	data, err := c.readOnePacket()
	if err != nil {
		return nil, err
	}

	// This is a single packet.
	if len(data) < MaxPacketSize {
		return data, nil
	}

	// There is more than one packet, read them all.
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < MaxPacketSize {
			break
		}
	}

	return data, nil
}

// ReadPacket reads a packet from the underlying connection.
// it is the public API version, that returns a SQLError.
// The memory for the packet is always allocated, and it is owned by the caller
// after this function returns.
func (c *Conn) ReadPacket() ([]byte, error) {
	result, err := c.readPacket()
	if err != nil {
		return nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	return result, err
}

// writePacket writes a packet, possibly cutting it into multiple
// chunks.  Note this is not very efficient, as the client probably
// has to build the []byte and that makes a memory copy.
// Try to use startEphemeralPacketWithHeader/writeEphemeralPacket instead.
//
// This method returns a generic error, not a SQLError.
func (c *Conn) writePacket(data []byte) error {
	index := 0
	dataLength := len(data) - packetHeaderSize

	w, unget := c.getWriter()
	defer unget()

	var header [packetHeaderSize]byte
	for {
		// toBeSent is capped to MaxPacketSize.
		toBeSent := dataLength
		if toBeSent > MaxPacketSize {
			toBeSent = MaxPacketSize
		}

		// save the first 4 bytes of the payload, we will overwrite them with the
		// header below
		copy(header[0:packetHeaderSize], data[index:index+packetHeaderSize])

		// Compute and write the header.
		data[index] = byte(toBeSent)
		data[index+1] = byte(toBeSent >> 8)
		data[index+2] = byte(toBeSent >> 16)
		data[index+3] = c.sequence

		// Write the body.
		if n, err := w.Write(data[index : index+toBeSent+packetHeaderSize]); err != nil {
			return fmt.Errorf("Write(packet) failed, err: %v", err)
		} else if n != (toBeSent + packetHeaderSize) {
			return fmt.Errorf("Write(packet) returned a short write: %v < %v", n, (toBeSent + packetHeaderSize))
		}

		// restore the first 4 bytes once the network send is done
		copy(data[index:index+packetHeaderSize], header[0:packetHeaderSize])

		// Update our state.
		c.sequence++
		dataLength -= toBeSent
		if dataLength == 0 {
			if toBeSent == MaxPacketSize {
				// The packet we just sent had exactly
				// MaxPacketSize size, we need to
				// sent a zero-size packet too.
				header[0] = 0
				header[1] = 0
				header[2] = 0
				header[3] = c.sequence
				if n, err := w.Write(header[:]); err != nil {
					return fmt.Errorf("Write(empty header) failed, err: %v", err)
				} else if n != packetHeaderSize {
					return fmt.Errorf("Write(empty header) returned a short write: %v < 4", n)
				}
				c.sequence++
			}
			return nil
		}
		index += toBeSent
	}
}

func (c *Conn) startEphemeralPacketWithHeader(length int) ([]byte, int) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic("startEphemeralPacketWithHeader cannot be used while a packet is already started.")
	}

	c.currentEphemeralPolicy = ephemeralWrite
	// get buffer from pool or it'll be allocated if length is too big
	c.currentEphemeralBuffer = bufPool.Get(length + packetHeaderSize)
	return *c.currentEphemeralBuffer, packetHeaderSize
}

// writeEphemeralPacket writes the packet that was allocated by
// startEphemeralPacketWithHeader.
func (c *Conn) writeEphemeralPacket() error {
	defer c.recycleWritePacket()

	switch c.currentEphemeralPolicy {
	case ephemeralWrite:
		if err := c.writePacket(*c.currentEphemeralBuffer); err != nil {
			return fmt.Errorf("conn %v, err: %v", c.ID(), err)
		}
	case ephemeralUnused, ephemeralRead:
		// Programming error.
		panic(fmt.Errorf("conn %v: trying to call writeEphemeralPacket while currentEphemeralPolicy is %v", c.ID(), c.currentEphemeralPolicy))
	}

	return nil
}

// recycleWritePacket recycles the write packet. It needs to be called
// after writeEphemeralPacket was called.
func (c *Conn) recycleWritePacket() {
	if c.currentEphemeralPolicy != ephemeralWrite {
		// Programming error.
		panic(fmt.Errorf("trying to call recycleWritePacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	// Release our reference so the buffer can be gced
	bufPool.Put(c.currentEphemeralBuffer)
	c.currentEphemeralBuffer = nil
	c.currentEphemeralPolicy = ephemeralUnused
}

// writeComQuit writes a Quit message for the server, to indicate we
// want to close the connection.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) writeComQuit() error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0

	data, pos := c.startEphemeralPacketWithHeader(1)
	data[pos] = ComQuit
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, err.Error())
	}
	return nil
}

// RemoteAddr returns the underlying socket RemoteAddr().
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ID returns the MySQL connection ID for this connection.
func (c *Conn) ID() int64 {
	return int64(c.ConnectionID)
}

// Ident returns a useful identification string for error logging
func (c *Conn) String() string {
	return fmt.Sprintf("client %v (%s)", c.ConnectionID, c.RemoteAddr().String())
}

// Close closes the connection. It can be called from a different go
// routine to interrupt the current connection.
func (c *Conn) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.conn.Close()
	}
}

// IsClosed returns true if this connection was ever closed by the
// Close() method.  Note if the other side closes the connection, but
// Close() wasn't called, this will return false.
func (c *Conn) IsClosed() bool {
	return c.closed.Get()
}

func getLenEncString(value []byte) []byte {
	data := getLenEncInt(uint64(len(value)))
	return append(data, value...)
}

func getLenEncInt(i uint64) []byte {
	var data []byte
	switch {
	case i < 251:
		data = append(data, byte(i))
	case i < 1<<16:
		data = append(data, 0xfc)
		data = append(data, byte(i))
		data = append(data, byte(i>>8))
	case i < 1<<24:
		data = append(data, 0xfd)
		data = append(data, byte(i))
		data = append(data, byte(i>>8))
		data = append(data, byte(i>>16))
	default:
		data = append(data, 0xfe)
		data = append(data, byte(i))
		data = append(data, byte(i>>8))
		data = append(data, byte(i>>16))
		data = append(data, byte(i>>24))
		data = append(data, byte(i>>32))
		data = append(data, byte(i>>40))
		data = append(data, byte(i>>48))
		data = append(data, byte(i>>56))
	}
	return data
}

//
// Packet parsing methods, for generic packets.
//

// isEOFPacket determines whether or not a data packet is a "true" EOF. DO NOT blindly compare the
// first byte of a packet to EOFPacket as you might do for other packet types, as 0xfe is overloaded
// as a first byte.
//
// Per https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html, a packet starting with 0xfe
// but having length >= 9 (on top of 4 byte header) is not a true EOF but a LengthEncodedInteger
// (typically preceding a LengthEncodedString). Thus, all EOF checks must validate the payload size
// before exiting.
//
// More specifically, an EOF packet can have 3 different lengths (1, 5, 7) depending on the client
// flags that are set. 7 comes from server versions of 5.7.5 or greater where ClientDeprecateEOF is
// set (i.e. uses an OK packet starting with 0xfe instead of 0x00 to signal EOF). Regardless, 8 is
// an upper bound otherwise it would be ambiguous w.r.t. LengthEncodedIntegers.
//
// More docs here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_response_packets.html
func isEOFPacket(data []byte) bool {
	return data[0] == EOFPacket && len(data) < 9
}

// parseEOFPacket returns the warning count and a boolean to indicate if there
// are more results to receive.
//
// Note: This is only valid on actual EOF packets and not on OK packets with the EOF
// type code set, i.e. should not be used if ClientDeprecateEOF is set.
func parseEOFPacket(data []byte) (warnings uint16, more bool, err error) {
	// The warning count is in position 2 & 3
	warnings, _, _ = readUint16(data, 1)

	// The status flag is in position 4 & 5
	statusFlags, _, ok := readUint16(data, 3)
	if !ok {
		return 0, false, fmt.Errorf("invalid EOF packet statusFlags: %v", data)
	}
	return warnings, (statusFlags & ServerMoreResultsExists) != 0, nil
}

// PacketOK contains the ok packet details
type PacketOK struct {
	affectedRows uint64
	lastInsertID uint64
	statusFlags  uint16
	warnings     uint16
	info         string

	// at the moment, we only store GTID information in this field
	sessionStateData string
}

func (c *Conn) parseOKPacket(in []byte) (*PacketOK, error) {
	data := &coder{
		data: in,
		pos:  1, // We already read the type.
	}
	packetOK := &PacketOK{}

	fail := func(format string, args ...interface{}) (*PacketOK, error) {
		return nil, fmt.Errorf(format, args...)
	}

	// Affected rows.
	affectedRows, ok := data.readLenEncInt()
	if !ok {
		return fail("invalid OK packet affectedRows: %v", data)
	}
	packetOK.affectedRows = affectedRows

	// Last Insert ID.
	lastInsertID, ok := data.readLenEncInt()
	if !ok {
		return fail("invalid OK packet lastInsertID: %v", data)
	}
	packetOK.lastInsertID = lastInsertID

	// Status flags.
	statusFlags, ok := data.readUint16()
	if !ok {
		return fail("invalid OK packet statusFlags: %v", data)
	}
	packetOK.statusFlags = statusFlags

	// assuming CapabilityClientProtocol41
	// Warnings.
	warnings, ok := data.readUint16()
	if !ok {
		return fail("invalid OK packet warnings: %v", data)
	}
	packetOK.warnings = warnings

	if c.Capabilities&uint32(CapabilityClientSessionTrack) == CapabilityClientSessionTrack {
		// info
		info, _ := data.readLenEncInfo()
		packetOK.info = info
		// session tracking
		if statusFlags&ServerSessionStateChanged == ServerSessionStateChanged {
			_, ok := data.readLenEncInt()
			if !ok {
				return fail("invalid OK packet session state change length: %v", data)
			}
			sscType, ok := data.readByte()
			if !ok || sscType != SessionTrackGtids {
				return fail("invalid OK packet session state change type: %v", sscType)
			}

			// Move past the total length of the changed entity: 1 byte
			_, ok = data.readByte()
			if !ok {
				return fail("invalid OK packet gtids length: %v", data)
			}
			// read (and ignore for now) the GTIDS encoding specification code: 1 byte
			_, ok = data.readByte()
			if !ok {
				return fail("invalid OK packet gtids type: %v", data)
			}
			gtids, ok := data.readLenEncString()
			if !ok {
				return fail("invalid OK packet gtids: %v", data)
			}
			packetOK.sessionStateData = gtids
		}
	} else {
		// info
		info, _ := data.readLenEncInfo()
		packetOK.info = info
	}

	return packetOK, nil
}

// isErrorPacket determines whether or not the packet is an error packet. Mostly here for
// consistency with isEOFPacket
func isErrorPacket(data []byte) bool {
	return data[0] == ErrPacket
}

// ParseErrorPacket parses the error packet and returns a SQLError.
func ParseErrorPacket(data []byte) error {
	// We already read the type.
	pos := 1

	// Error code is 2 bytes.
	code, pos, ok := readUint16(data, pos)
	if !ok {
		return NewSQLError(CRUnknownError, SSUnknownSQLState, "invalid error packet code: %v", data)
	}

	// '#' marker of the SQL state is 1 byte. Ignored.
	pos++

	// SQL state is 5 bytes
	sqlState, pos, ok := readBytesCopy(data, pos, 5)
	if !ok {
		return NewSQLError(CRUnknownError, SSUnknownSQLState, "invalid error packet sqlState: %v", data)
	}

	// Human readable error message is the rest.
	msg := string(data[pos:])

	return NewSQLError(int(code), string(sqlState), "%v", msg)
}

// GetTLSClientCerts gets TLS certificates.
func (c *Conn) GetTLSClientCerts() []*x509.Certificate {
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		return tlsConn.ConnectionState().PeerCertificates
	}
	return nil
}
