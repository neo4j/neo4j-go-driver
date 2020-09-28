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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

const (
	bolt4_ready        = iota // Ready for use
	bolt4_streaming           // Receiving result from auto commit query
	bolt4_pendingtx           // Transaction has been requested but not applied
	bolt4_tx                  // Transaction pending
	bolt4_streamingtx         // Receiving result from a query within a transaction
	bolt4_failed              // Recoverable error, needs reset
	bolt4_dead                // Non recoverable protocol or connection error
	bolt4_unauthorized        // Initial state, not sent hello message with authentication
)

type internalTx4 struct {
	mode         db.AccessMode
	bookmarks    []string
	timeout      time.Duration
	txMeta       map[string]interface{}
	databaseName string
}

func (i *internalTx4) toMeta() map[string]interface{} {
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
	if i.databaseName != db.DefaultDatabase {
		meta["db"] = i.databaseName
	}
	return meta
}

type bolt4 struct {
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
	pendingTx     *internalTx4 // Stashed away when tx started explcitly
	bookmark      string       // Last bookmark
	birthDate     time.Time
	log           log.Logger
	databaseName  string
	receiveBuffer []byte
	err           error // Last fatal error
}

func NewBolt4(serverName string, conn net.Conn, log log.Logger) *bolt4 {
	return &bolt4{
		state:         bolt4_unauthorized,
		conn:          conn,
		serverName:    serverName,
		chunker:       newChunker(),
		receiveBuffer: make([]byte, 4096),
		packer:        &packstream.Packer{},
		unpacker:      &packstream.Unpacker{},
		birthDate:     time.Now(),
		log:           log,
	}
}

func (b *bolt4) ServerName() string {
	return b.serverName
}

func (b *bolt4) ServerVersion() string {
	return b.serverVersion
}

// Sets b.err and b.state on failure
func (b *bolt4) appendMsg(tag packstream.StructTag, field ...interface{}) {
	b.chunker.beginMessage()
	// Setup the message and let packstream write the packed bytes to the chunk
	b.chunker.buf, b.err = b.packer.PackStruct(b.chunker.buf, dehydrate, tag, field...)
	if b.err != nil {
		// At this point we do not know the state of what has been written to the chunks.
		// Either we should support rolling back whatever that has been written or just
		// bail out this session.
		b.log.Error(log.Bolt4, b.logId, b.err)
		b.state = bolt4_dead
		return
	}
	b.chunker.endMessage()
}

// Sets b.err and b.state on failure
func (b *bolt4) sendMsg(tag packstream.StructTag, field ...interface{}) {
	if b.appendMsg(tag, field...); b.err != nil {
		return
	}
	if b.err = b.chunker.send(b.conn); b.err != nil {
		b.log.Error(log.Bolt4, b.logId, b.err)
		b.state = bolt4_dead
	}
}

// Sets b.err and b.state on failure
func (b *bolt4) receiveMsg() interface{} {
	b.receiveBuffer, b.err = dechunkMessage(b.conn, b.receiveBuffer)
	if b.err != nil {
		b.log.Error(log.Bolt4, b.logId, b.err)
		b.state = bolt4_dead
		return nil
	}

	msg, err := b.unpacker.UnpackStruct(b.receiveBuffer, hydrate)
	if err != nil {
		b.log.Error(log.Bolt4, b.logId, err)
		b.state = bolt4_dead
		b.err = err
		return nil
	}

	return msg
}

// Receives a message that is assumed to be a success response or a failure in response
// to a sent command.
// Sets b.err and b.state on failure
func (b *bolt4) receiveSuccess() *successResponse {
	msg := b.receiveMsg()
	if b.err != nil {
		return nil
	}

	switch v := msg.(type) {
	case *successResponse:
		return v
	case *db.Neo4jError:
		b.state = bolt4_failed
		b.err = v
		if v.Classification() == "ClientError" {
			// These could include potentially large cypher statement, only log to debug
			b.log.Debugf(log.Bolt4, b.logId, "%s", v)
		} else {
			b.log.Error(log.Bolt4, b.logId, v)
		}
		return nil
	}
	b.state = bolt4_dead
	b.err = errors.New("Expected success or database error")
	b.log.Error(log.Bolt4, b.logId, b.err)
	return nil
}

func (b *bolt4) connect(auth map[string]interface{}, userAgent string) error {
	if err := b.assertState(bolt4_unauthorized); err != nil {
		return err
	}

	hello := map[string]interface{}{
		"user_agent": userAgent,
	}
	// Merge authentication info into hello message
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
		b.state = bolt4_dead
		b.err = errors.New(fmt.Sprintf("Unexpected server response: %+v", succRes))
		return b.err
	}
	b.connId = helloRes.connectionId
	b.logId = fmt.Sprintf("%s@%s", b.connId, b.serverName)
	b.serverVersion = helloRes.server

	// Transition into ready state
	b.state = bolt4_ready
	b.log.Infof(log.Bolt4, b.logId, "Connected")
	return nil
}

func (b *bolt4) TxBegin(
	mode db.AccessMode, bookmarks []string, timeout time.Duration, txMeta map[string]interface{}) (db.Handle, error) {

	// Ok, to begin transaction while streaming auto-commit, just empty the stream and continue.
	if b.state == bolt4_streaming {
		if err := b.consumeStream(); err != nil {
			return nil, err
		}
	}

	if err := b.assertState(bolt4_ready); err != nil {
		return nil, err
	}

	tx := &internalTx4{
		mode:         mode,
		bookmarks:    bookmarks,
		timeout:      timeout,
		txMeta:       txMeta,
		databaseName: b.databaseName,
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
		b.state = bolt4_tx
	} else {
		// Stash this into pending internal tx
		b.pendingTx = tx
		b.txId = time.Now().Unix()
		b.state = bolt4_pendingtx
	}
	return b.txId, nil
}

// Should NOT set b.err or change b.state as this is used to guard from
// misuse from clients that stick to their connections when they shouldn't.
func (b *bolt4) assertHandle(id int64, h db.Handle) error {
	hid, ok := h.(int64)
	if !ok || hid != id {
		err := errors.New("Invalid handle")
		b.log.Error(log.Bolt4, b.logId, err)
		return err
	}
	return nil
}

// Should NOT set b.err or b.state since the connection is still valid
func (b *bolt4) assertState(allowed ...int) error {
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
	b.log.Error(log.Bolt4, b.logId, err)
	return err
}

func (b *bolt4) TxCommit(txh db.Handle) error {
	if err := b.assertHandle(b.txId, txh); err != nil {
		return err
	}

	// Nothing to do, a transaction started but no commands were issued on it, server is unaware
	if b.state == bolt4_pendingtx {
		b.state = bolt4_ready
		return nil
	}

	// Consume pending stream if any to turn state from streamingtx to tx
	if b.state == bolt4_streamingtx {
		if err := b.consumeStream(); err != nil {
			return err
		}
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt4_tx); err != nil {
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
		b.state = bolt4_dead
		b.err = errors.New(fmt.Sprintf("Failed to parse commit response: %+v", succRes))
		b.log.Error(log.Bolt4, b.logId, b.err)
		return b.err
	}

	// Keep track of bookmark
	if len(commitSuccess.bookmark) > 0 {
		b.bookmark = commitSuccess.bookmark
	}

	// Transition into ready state
	b.state = bolt4_ready
	return nil
}

func (b *bolt4) TxRollback(txh db.Handle) error {
	if err := b.assertHandle(b.txId, txh); err != nil {
		return err
	}

	// Nothing to do, a transaction started but no commands were issued on it
	if b.state == bolt4_pendingtx {
		b.state = bolt4_ready
		return nil
	}

	// Can not send rollback while still streaming, consume to turn state into tx
	if b.state == bolt4_streamingtx {
		if err := b.consumeStream(); err != nil {
			return err
		}
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt4_tx); err != nil {
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

	b.state = bolt4_ready
	return nil
}

// Discards all records, keeps bookmark
func (b *bolt4) consumeStream() error {
	if b.state != bolt4_streaming && b.state != bolt4_streamingtx {
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

func (b *bolt4) run(cypher string, params map[string]interface{}, tx *internalTx4) (*db.Stream, error) {
	b.log.Debugf(log.Bolt4, b.logId, "run")
	// If already streaming, consume the whole thing first
	if err := b.consumeStream(); err != nil {
		return nil, err
	}

	if err := b.assertState(bolt4_tx, bolt4_ready, bolt4_pendingtx); err != nil {
		return nil, err
	}

	var meta map[string]interface{}
	if tx != nil {
		meta = tx.toMeta()
	}

	// Append lazy begin transaction message
	if b.state == bolt4_pendingtx {
		if b.appendMsg(msgBegin, meta); b.err != nil {
			return nil, b.err
		}
		meta = nil
	}

	// Append run message
	if b.appendMsg(msgRun, cypher, params, meta); b.err != nil {
		return nil, b.err
	}

	// Append pull message and send it along with other pending messages
	if b.sendMsg(msgPullN, map[string]interface{}{"n": -1}); b.err != nil {
		return nil, b.err
	}

	// Process server responses
	// Receive confirmation of transaction begin if it was started above
	if b.state == bolt4_pendingtx {
		if b.receiveSuccess(); b.err != nil {
			return nil, b.err
		}
		b.state = bolt4_tx
	}

	// Receive confirmation of run message
	res := b.receiveSuccess()
	if b.err != nil {
		return nil, b.err
	}
	// Extract the RUN response from success response
	runRes := res.run()
	if runRes == nil {
		b.state = bolt4_dead
		b.err = errors.New(fmt.Sprintf("Failed to parse RUN response: %+v", res))
		b.log.Error(log.Bolt4, b.logId, b.err)
		return nil, b.err
	}
	b.tfirst = runRes.t_first
	b.streamKeys = runRes.fields
	// Change state to streaming
	if b.state == bolt4_ready {
		b.state = bolt4_streaming
	} else {
		b.state = bolt4_streamingtx
	}

	b.streamId = time.Now().Unix()
	stream := &db.Stream{Keys: b.streamKeys, Handle: b.streamId}
	return stream, nil
}

func (b *bolt4) Run(
	cypher string, params map[string]interface{}, mode db.AccessMode,
	bookmarks []string, timeout time.Duration, txMeta map[string]interface{}) (*db.Stream, error) {

	if err := b.assertState(bolt4_streaming, bolt4_ready); err != nil {
		return nil, err
	}

	tx := internalTx4{
		mode:         mode,
		bookmarks:    bookmarks,
		timeout:      timeout,
		txMeta:       txMeta,
		databaseName: b.databaseName,
	}
	return b.run(cypher, params, &tx)
}

func (b *bolt4) RunTx(txh db.Handle, cypher string, params map[string]interface{}) (*db.Stream, error) {
	if err := b.assertHandle(b.txId, txh); err != nil {
		return nil, err
	}

	stream, err := b.run(cypher, params, b.pendingTx)
	b.pendingTx = nil
	return stream, err
}

// Reads one record from the stream.
func (b *bolt4) Next(shandle db.Handle) (*db.Record, *db.Summary, error) {
	if err := b.assertHandle(b.streamId, shandle); err != nil {
		return nil, nil, err
	}

	if err := b.assertState(bolt4_streaming, bolt4_streamingtx); err != nil {
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
			b.state = bolt4_dead
			b.err = errors.New("Failed to parse summary")
			b.log.Error(log.Bolt4, b.logId, b.err)
			return nil, nil, b.err
		}
		if b.state == bolt4_streamingtx {
			b.state = bolt4_tx
		} else {
			b.state = bolt4_ready
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
		b.state = bolt4_failed
		if x.Classification() == "ClientError" {
			// These could include potentially large cypher statement, only log to debug
			b.log.Debugf(log.Bolt4, b.logId, "%s", x)
		} else {
			b.log.Error(log.Bolt4, b.logId, x)
		}
		return nil, nil, x
	default:
		b.state = bolt4_dead
		b.err = errors.New("Unknown response")
		b.log.Error(log.Bolt4, b.logId, b.err)
		return nil, nil, b.err
	}
}

func (b *bolt4) Bookmark() string {
	return b.bookmark
}

func (b *bolt4) IsAlive() bool {
	return b.state != bolt4_dead
}

func (b *bolt4) Birthdate() time.Time {
	return b.birthDate
}

func (b *bolt4) Reset() {
	defer func() {
		// Reset internal state
		b.txId = 0
		b.streamId = 0
		b.streamKeys = []string{}
		b.bookmark = ""
		b.pendingTx = nil
		b.databaseName = db.DefaultDatabase
		b.err = nil
	}()

	if b.state == bolt4_ready || b.state == bolt4_dead {
		// No need for reset
		return
	}

	// Will consume ongoing stream if any
	b.consumeStream()
	if b.state == bolt4_ready || b.state == bolt4_dead {
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
			b.state = bolt4_ready
			return
		default:
			b.state = bolt4_dead
			return
		}
	}
}

func (b *bolt4) GetRoutingTable(database string, context map[string]string) (*db.RoutingTable, error) {
	if err := b.assertState(bolt4_ready); err != nil {
		return nil, err
	}

	// The query should run in system database, preserve current setting and restore it when
	// done.
	originalDatabaseName := b.databaseName
	b.databaseName = "system"
	defer func() { b.databaseName = originalDatabaseName }()

	// Query for the users default database or a specific database
	const (
		queryDefault  = "CALL dbms.routing.getRoutingTable($context)"
		queryDatabase = "CALL dbms.routing.getRoutingTable($context, $db)"
	)
	query := queryDefault
	params := map[string]interface{}{"context": context}
	if database != db.DefaultDatabase {
		query = queryDatabase
		params["db"] = database
	}

	stream, err := b.Run(query, params, db.ReadMode, nil, 0, nil)
	if err != nil {
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
func (b *bolt4) Close() {
	if b.state != bolt4_dead {
		b.sendMsg(msgGoodbye)
	}
	b.conn.Close()
	b.state = bolt4_dead
	b.log.Infof(log.Bolt4, b.logId, "Disconnected")
}

func (b *bolt4) SelectDatabase(database string) {
	b.databaseName = database
}
