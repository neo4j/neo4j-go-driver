/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

const (
	bolt3_ready        = iota // Ready for use
	bolt3_streaming           // Receiving result from auto commit query
	bolt3_pendingtx           // Transaction has been requested but not applied
	bolt3_tx                  // Transaction pending
	bolt3_streamingtx         // Receiving result from a query within a transaction
	bolt3_failed              // Recoverable error, needs reset
	bolt3_dead                // Non recoverable protocol or connection error
	bolt3_unauthorized        // Initial state, not sent hello message with authentication
)

type internalTx3 struct {
	mode      db.AccessMode
	bookmarks []string
	timeout   time.Duration
	txMeta    map[string]interface{}
}

func (i *internalTx3) toMeta() map[string]interface{} {
	meta := map[string]interface{}{}
	if i.mode == db.ReadMode {
		meta["mode"] = "r"
	}
	if len(i.bookmarks) > 0 {
		meta["bookmarks"] = i.bookmarks
	}
	ms := int(i.timeout.Nanoseconds() / 1e6)
	if ms > 0 {
		meta["tx_timeout"] = ms
	}
	if len(i.txMeta) > 0 {
		meta["tx_metadata"] = i.txMeta
	}
	return meta
}

type bolt3 struct {
	state         int
	txId          int64
	streamId      int64
	streamKeys    []string
	conn          net.Conn
	serverName    string
	chunker       *chunker
	packer        *packstream.Packer
	unpacker      *packstream.Unpacker
	connId        string
	logId         string
	serverVersion string
	tfirst        int64        // Time that server started streaming
	pendingTx     *internalTx3 // Stashed away when tx started explcitly
	bookmark      string       // Last bookmark
	birthDate     time.Time
	log           log.Logger
	receiveBufer  []byte
	err           error // Last fatal error
}

func NewBolt3(serverName string, conn net.Conn, log log.Logger) *bolt3 {
	return &bolt3{
		state:        bolt3_unauthorized,
		conn:         conn,
		serverName:   serverName,
		chunker:      newChunker(),
		receiveBufer: make([]byte, 4096),
		packer:       &packstream.Packer{},
		unpacker:     &packstream.Unpacker{},
		birthDate:    time.Now(),
		log:          log,
	}
}

func (b *bolt3) ServerName() string {
	return b.serverName
}

func (b *bolt3) ServerVersion() string {
	return b.serverVersion
}

func (b *bolt3) appendMsg(tag packstream.StructTag, field ...interface{}) {
	b.chunker.beginMessage()
	// Setup the message and let packstream write the packed bytes to the chunk
	b.chunker.buf, b.err = b.packer.PackStruct(b.chunker.buf, dehydrate, tag, field...)
	if b.err != nil {
		// At this point we do not know the state of what has been written to the chunks.
		// Either we should support rolling back whatever that has been written or just
		// bail out this session.
		b.log.Error(log.Bolt3, b.logId, b.err)
		b.state = bolt3_dead
		return
	}
	b.chunker.endMessage()
}

func (b *bolt3) sendMsg(tag packstream.StructTag, field ...interface{}) {
	if b.appendMsg(tag, field...); b.err != nil {
		return
	}
	if b.err = b.chunker.send(b.conn); b.err != nil {
		b.log.Error(log.Bolt3, b.logId, b.err)
		b.state = bolt3_dead
	}
}

func (b *bolt3) receiveMsg() interface{} {
	b.receiveBufer, b.err = dechunkMessage(b.conn, b.receiveBufer)
	if b.err != nil {
		b.log.Error(log.Bolt3, b.logId, b.err)
		b.state = bolt3_dead
		return nil
	}

	msg, err := b.unpacker.UnpackStruct(b.receiveBufer, hydrate)
	if err != nil {
		b.log.Error(log.Bolt3, b.logId, err)
		b.state = bolt3_dead
		b.err = err
		return nil
	}

	return msg
}

// Receives a message that is assumed to be a success response or a failure in response
// to a sent command.
func (b *bolt3) receiveSuccess() *successResponse {
	msg := b.receiveMsg()
	if b.err != nil {
		return nil
	}

	switch v := msg.(type) {
	case *successResponse:
		return v
	case *db.Neo4jError:
		b.state = bolt3_failed
		b.err = v
		if v.Classification() == "ClientError" {
			// These could include potentially large cypher statement, only log to debug
			b.log.Debugf(log.Bolt3, b.logId, "%s", v)
		} else {
			b.log.Error(log.Bolt3, b.logId, v)
		}
		return nil
	}
	b.state = bolt3_dead
	b.err = errors.New("Expected success or database error")
	b.log.Error(log.Bolt3, b.logId, b.err)
	return nil
}

func (b *bolt3) connect(auth map[string]interface{}, userAgent string) error {
	if err := b.assertState(bolt3_unauthorized); err != nil {
		return err
	}

	// Merge authentication info into hello message
	hello := map[string]interface{}{
		"user_agent": userAgent,
	}
	for k, v := range auth {
		_, exists := hello[k]
		if exists {
			continue
		}
		hello[k] = v
	}

	// Send hello message and wait for confirmation
	if b.sendMsg(msgHello, hello); b.err != nil {
		return b.err
	}
	succRes := b.receiveSuccess()
	if b.err != nil {
		return b.err
	}

	helloRes := succRes.hello()
	if helloRes == nil {
		b.state = bolt3_dead
		b.err = errors.New(fmt.Sprintf("Unexpected server response: %+v", succRes))
		return b.err
	}
	b.connId = helloRes.connectionId
	b.logId = fmt.Sprintf("%s@%s", b.connId, b.serverName)
	b.serverVersion = helloRes.server

	// Transition into ready state
	b.state = bolt3_ready
	b.log.Infof(log.Bolt3, b.logId, "Connected")
	return nil
}

func (b *bolt3) TxBegin(
	mode db.AccessMode, bookmarks []string, timeout time.Duration, txMeta map[string]interface{}) (db.Handle, error) {

	// Ok, to begin transaction while streaming auto-commit, just empty the stream and continue.
	if b.state == bolt3_streaming {
		if err := b.consumeStream(); err != nil {
			return nil, err
		}
	}

	if err := b.assertState(bolt3_ready); err != nil {
		return nil, err
	}

	tx := &internalTx3{
		mode:      mode,
		bookmarks: bookmarks,
		timeout:   timeout,
		txMeta:    txMeta,
	}

	// If there are bookmarks, begin the transaction immediately for backwards compatible
	// reasons, otherwise delay it to save a round-trip
	if len(bookmarks) > 0 {
		if b.sendMsg(msgBegin, tx.toMeta()); b.err != nil {
			return nil, b.err
		}
		if b.receiveSuccess(); b.err != nil {
			return nil, b.err
		}
		b.txId = time.Now().Unix()
		b.state = bolt3_tx
	} else {
		// Stash this into pending internal tx
		b.pendingTx = tx
		b.txId = time.Now().Unix()
		b.state = bolt3_pendingtx
	}
	return b.txId, nil
}

// Should NOT set b.err or change b.state as this is used to guard from
// misuse from clients that stick to their connections when they shouldn't.
func (b *bolt3) assertHandle(id int64, h db.Handle) error {
	hid, ok := h.(int64)
	if !ok || hid != id {
		err := errors.New("Invalid handle")
		b.log.Error(log.Bolt3, b.logId, err)
		return err
	}
	return nil
}

// Should NOT set b.err or b.state since the connection is still valid
func (b *bolt3) assertState(allowed ...int) error {
	// Forward prior error instead, this former error is probably the
	// root cause of any state error. Like a call to Run with malformed
	// cypher causes an error and another call to Commit would cause the
	// state to be wrong. Do not log this.
	if b.err != nil {
		return b.err
	}
	for _, a := range allowed {
		if b.state == a {
			return nil
		}
	}
	err := errors.New(fmt.Sprintf("Invalid state %d, expected: %+v", b.state, allowed))
	b.log.Error(log.Bolt3, b.logId, err)
	return err
}

func (b *bolt3) TxCommit(txh db.Handle) error {
	if err := b.assertHandle(b.txId, txh); err != nil {
		return err
	}

	// Nothing to do, a transaction started but no commands were issued on it, server is unaware
	if b.state == bolt3_pendingtx {
		b.state = bolt3_ready
		return nil
	}

	// Consume pending stream if any to turn state from streamingtx to tx
	if b.state == bolt3_streamingtx {
		if err := b.consumeStream(); err != nil {
			return err
		}
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt3_tx); err != nil {
		return err
	}

	// Send request to server to commit
	if b.sendMsg(msgCommit); b.err != nil {
		return b.err
	}

	// Evaluate server response
	succRes := b.receiveSuccess()
	if b.err != nil {
		return b.err
	}
	commitSuccess := succRes.commit()
	if commitSuccess == nil {
		b.state = bolt3_dead
		b.err = errors.New(fmt.Sprintf("Failed to parse commit response: %+v", succRes))
		b.log.Error(log.Bolt3, b.logId, b.err)
		return b.err
	}

	// Keep track of bookmark
	if len(commitSuccess.bookmark) > 0 {
		b.bookmark = commitSuccess.bookmark
	}

	// Transition into ready state
	b.state = bolt3_ready
	return nil
}

func (b *bolt3) TxRollback(txh db.Handle) error {
	if err := b.assertHandle(b.txId, txh); err != nil {
		return err
	}

	// Nothing to do, a transaction started but no commands were issued on it
	if b.state == bolt3_pendingtx {
		b.state = bolt3_ready
		return nil
	}

	// Can not send rollback while still streaming, consume to turn state into tx
	if b.state == bolt3_streamingtx {
		if err := b.consumeStream(); err != nil {
			return err
		}
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt3_tx); err != nil {
		return err
	}

	// Send rollback request to server
	if b.sendMsg(msgRollback); b.err != nil {
		return b.err
	}

	// Receive rollback confirmation
	if b.receiveSuccess(); b.err != nil {
		return b.err
	}

	b.state = bolt3_ready
	return nil
}

// Discards all records, keeps bookmark
func (b *bolt3) consumeStream() error {
	if b.state != bolt3_streaming && b.state != bolt3_streamingtx {
		// Nothing to do
		return nil
	}

	for {
		_, sum, err := b.Next(b.streamId)
		if err != nil {
			return err
		}
		if sum != nil {
			break
		}
	}
	return nil
}

func (b *bolt3) run(cypher string, params map[string]interface{}, tx *internalTx3) (*db.Stream, error) {
	// If already streaming, consume the whole thing first
	if err := b.consumeStream(); err != nil {
		return nil, err
	}

	if err := b.assertState(bolt3_tx, bolt3_ready, bolt3_pendingtx); err != nil {
		return nil, err
	}

	var meta map[string]interface{}
	if tx != nil {
		meta = tx.toMeta()
	}

	// Append lazy begin transaction message
	if b.state == bolt3_pendingtx {
		if b.appendMsg(msgBegin, meta); b.err != nil {
			return nil, b.err
		}
		meta = nil
	}

	// Append run message
	if b.appendMsg(msgRun, cypher, params, meta); b.err != nil {
		return nil, b.err
	}

	// Append pull all message and send it along with other pending messages
	if b.sendMsg(msgPullAll); b.err != nil {
		return nil, b.err
	}

	// Process server responses
	// Receive confirmation of transaction begin if it was started above
	if b.state == bolt3_pendingtx {
		if b.receiveSuccess(); b.err != nil {
			return nil, b.err
		}
		b.state = bolt3_tx
	}

	// Receive confirmation of run message
	res := b.receiveSuccess()
	if b.err != nil {
		return nil, b.err
	}
	// Extract the RUN response from success response
	runRes := res.run()
	if runRes == nil {
		b.state = bolt3_dead
		b.err = errors.New(fmt.Sprintf("Failed to parse RUN response: %+v", res))
		b.log.Error(log.Bolt3, b.logId, b.err)
		return nil, b.err
	}
	b.tfirst = runRes.t_first
	b.streamKeys = runRes.fields
	// Change state to streaming
	if b.state == bolt3_ready {
		b.state = bolt3_streaming
	} else {
		b.state = bolt3_streamingtx
	}

	b.streamId = time.Now().Unix()
	stream := &db.Stream{Keys: b.streamKeys, Handle: b.streamId}
	return stream, nil
}

func (b *bolt3) Run(
	cypher string, params map[string]interface{}, mode db.AccessMode,
	bookmarks []string, timeout time.Duration, txMeta map[string]interface{}) (*db.Stream, error) {

	if err := b.assertState(bolt3_streaming, bolt3_ready); err != nil {
		return nil, err
	}

	tx := internalTx3{
		mode:      mode,
		bookmarks: bookmarks,
		timeout:   timeout,
		txMeta:    txMeta,
	}
	return b.run(cypher, params, &tx)
}

func (b *bolt3) RunTx(txh db.Handle, cypher string, params map[string]interface{}) (*db.Stream, error) {
	if err := b.assertHandle(b.txId, txh); err != nil {
		return nil, err
	}

	stream, err := b.run(cypher, params, b.pendingTx)
	b.pendingTx = nil
	return stream, err
}

// Reads one record from the stream.
func (b *bolt3) Next(shandle db.Handle) (*db.Record, *db.Summary, error) {
	if err := b.assertHandle(b.streamId, shandle); err != nil {
		return nil, nil, err
	}

	if err := b.assertState(bolt3_streaming, bolt3_streamingtx); err != nil {
		return nil, nil, err
	}

	res := b.receiveMsg()
	if b.err != nil {
		return nil, nil, b.err
	}

	switch x := res.(type) {
	case *db.Record:
		x.Keys = b.streamKeys
		return x, nil, nil
	case *successResponse:
		// End of stream
		// Parse summary
		sum := x.summary()
		if sum == nil {
			b.state = bolt3_dead
			b.err = errors.New("Failed to parse summary")
			b.log.Error(log.Bolt3, b.logId, b.err)
			return nil, nil, b.err
		}
		if b.state == bolt3_streamingtx {
			b.state = bolt3_tx
		} else {
			b.state = bolt3_ready
			// Keep bookmark for auto-commit tx
			if len(sum.Bookmark) > 0 {
				b.bookmark = sum.Bookmark
			}
		}
		b.streamId = 0
		// Add some extras to the summary
		sum.ServerVersion = b.serverVersion
		sum.ServerName = b.serverName
		sum.TFirst = b.tfirst
		return nil, sum, nil
	case *db.Neo4jError:
		b.err = x
		b.state = bolt3_failed
		if x.Classification() == "ClientError" {
			// These could include potentially large cypher statement, only log to debug
			b.log.Debugf(log.Bolt3, b.logId, "%s", x)
		} else {
			b.log.Error(log.Bolt3, b.logId, x)
		}
		return nil, nil, x
	default:
		b.state = bolt3_dead
		b.err = errors.New("Unknown response")
		b.log.Error(log.Bolt3, b.logId, b.err)
		return nil, nil, b.err
	}
}

func (b *bolt3) Bookmark() string {
	return b.bookmark
}

func (b *bolt3) IsAlive() bool {
	return b.state != bolt3_dead
}

func (b *bolt3) Birthdate() time.Time {
	return b.birthDate
}

func (b *bolt3) Reset() {
	defer func() {
		// Reset internal state
		b.txId = 0
		b.streamId = 0
		b.streamKeys = []string{}
		b.bookmark = ""
		b.pendingTx = nil
		b.err = nil
	}()

	if b.state == bolt3_ready || b.state == bolt3_dead {
		// No need for reset
		return
	}

	// Will consume ongoing stream if any
	b.consumeStream()
	if b.state == bolt3_ready || b.state == bolt3_dead {
		// No need for reset
		return
	}

	// Send the reset message to the server
	if b.sendMsg(msgReset); b.err != nil {
		return
	}

	// Should receive x number of ignores until we get a success
	for {
		msg := b.receiveMsg()
		if b.err != nil {
			return
		}
		switch msg.(type) {
		case *ignoredResponse:
			// Command ignored
		case *successResponse:
			// Reset confirmed
			b.state = bolt3_ready
			return
		default:
			b.state = bolt3_dead
			return
		}
	}
}

func (b *bolt3) GetRoutingTable(database string, context map[string]string) (*db.RoutingTable, error) {
	if err := b.assertState(bolt3_ready); err != nil {
		return nil, err
	}

	if database != db.DefaultDatabase {
		return nil, errors.New("Bolt 3 does not support routing to a specifiec database name")
	}

	// Only available when Neo4j is setup with clustering
	const query = "CALL dbms.cluster.routing.getRoutingTable($context)"
	stream, err := b.Run(query, map[string]interface{}{"context": context}, db.ReadMode, nil, 0, nil)
	if err != nil {
		// Give a better error
		dbError, isDbError := err.(*db.Neo4jError)
		if isDbError && dbError.Code == "Neo.ClientError.Procedure.ProcedureNotFound" {
			return nil, &db.RoutingNotSupportedError{Server: b.serverName}
		}
		return nil, err
	}

	rec, _, err := b.Next(stream.Handle)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, errors.New("No routing table record")
	}
	// Just empty the stream, ignore the summary should leave the connecion in ready state
	b.Next(stream.Handle)

	table := parseRoutingTableRecord(rec)
	if table == nil {
		return nil, errors.New("Unable to parse routing table")
	}

	return table, nil
}

// Beware, could be called on another thread when driver is closed.
func (b *bolt3) Close() {
	b.log.Infof(log.Bolt3, b.logId, "Disconnected")
	if b.state != bolt3_dead {
		b.sendMsg(msgGoodbye)
	}
	b.conn.Close()
	b.state = bolt3_dead
}
