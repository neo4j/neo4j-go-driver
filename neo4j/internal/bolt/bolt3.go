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

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
)

const (
	msgV3Reset      packstream.StructTag = 0x0f
	msgV3Run        packstream.StructTag = 0x10
	msgV3DiscardAll packstream.StructTag = 0x2f
	msgV3PullAll    packstream.StructTag = 0x3f
	msgV3Record     packstream.StructTag = 0x71
	msgV3Success    packstream.StructTag = 0x70
	msgV3Ignored    packstream.StructTag = 0x7e
	msgV3Failure    packstream.StructTag = 0x7f
	msgV3Hello      packstream.StructTag = 0x01
	msgV3Goodbye    packstream.StructTag = 0x02
	msgV3Begin      packstream.StructTag = 0x11
	msgV3Commit     packstream.StructTag = 0x12
	msgV3Rollback   packstream.StructTag = 0x13
)

const userAgent = "Go Driver/1.8"

const (
	bolt3_ready        = iota // After connect and reset
	bolt3_disconnected        // Lost connection to server
	bolt3_failed              // Recoverable, needs reset
	bolt3_corrupt             // Non recoverable protocol error
	bolt3_streaming           // Receiving result from auto commit query
	bolt3_pendingtx           // Begin transaction has been requested but not applied
	bolt3_tx                  // In a transaction
	bolt3_streamingtx         // Receiving result from a query within a transaction
)

func log(msg string) {
	//fmt.Printf("bolt3: %s\n", msg)
}

type internalTx struct {
	mode      db.AccessMode
	bookmarks []string
	timeout   time.Duration
	txMeta    map[string]interface{}
}

func (i *internalTx) toMeta() map[string]interface{} {
	mode := "w"
	if i.mode == db.ReadMode {
		mode = "r"
	}
	meta := map[string]interface{}{
		"mode": mode,
	}
	if len(i.bookmarks) > 0 {
		meta["bookmarks"] = i.bookmarks
	}
	ts := int(i.timeout.Seconds())
	if ts > 0 {
		meta["tx_timeout"] = ts
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
	dechunker     *dechunker
	packer        *packstream.Packer
	unpacker      *packstream.Unpacker
	connId        string
	serverVersion string
	tfirst        int64       // Time that server started streaming
	pendingTx     *internalTx // Stashed away when tx started explcitly
	bookmark      string      // Last bookmark
}

func NewBolt3(serverName string, conn net.Conn) *bolt3 {
	// Wrap connection reading/writing in chunk reader/writer
	chunker := newChunker(conn)
	dechunker := newDechunker(conn)

	return &bolt3{
		state:      bolt3_disconnected,
		conn:       conn,
		serverName: serverName,
		chunker:    chunker,
		dechunker:  dechunker,
		packer:     packstream.NewPacker(chunker, dehydrate),
		unpacker:   packstream.NewUnpacker(dechunker),
	}
}

func (b *bolt3) ServerName() string {
	return b.serverName
}

func (b *bolt3) appendMsg(tag packstream.StructTag, field ...interface{}) error {
	log(fmt.Sprintf("appending: {Tag: %d, Fields: %+v }", tag, field))
	b.chunker.beginMessage()
	// Setup the message and let packstream write the packed bytes to the chunk
	err := b.packer.PackStruct(tag, field...)
	if err != nil {
		// At this point we do not know the state of what has been written to the chunks.
		// Either we should support rolling back whatever that has been written or just
		// bail out this session.
		b.state = bolt3_corrupt
		return err
	}
	b.chunker.endMessage()
	return nil
}

func (b *bolt3) sendMsg(tag packstream.StructTag, field ...interface{}) error {
	err := b.appendMsg(tag, field...)
	if err != nil {
		return err
	}
	log("sending")
	return b.chunker.send()
}

func (b *bolt3) invalidStateError(expected []int) error {
	return errors.New(fmt.Sprintf("Invalid state %d, expected: %+v", b.state, expected))
}

func (b *bolt3) assertHandle(id int64, h db.Handle) error {
	hid, ok := h.(int64)
	if !ok || hid != id {
		return errors.New("Invalid handle")
	}
	return nil
}

func (b *bolt3) receive() (interface{}, error) {
	if err := b.dechunker.beginMessage(); err != nil {
		b.state = bolt3_corrupt
		return nil, err
	}
	res, err := b.unpacker.UnpackStruct(hydrate)
	if err != nil {
		log(fmt.Sprintf("receive error: %s", err))
		b.state = bolt3_corrupt
		if _, isIoError := err.(*packstream.IoError); isIoError {
			b.state = bolt3_disconnected
		}
		return nil, err
	}
	log(fmt.Sprintf("received: %T:%+v", res, res))
	if err = b.dechunker.endMessage(); err != nil {
		b.state = bolt3_corrupt
		return nil, err
	}
	return res, err
}

func (b *bolt3) receiveSuccessResponse() (*successResponse, error) {
	res, err := b.receive()
	if err != nil {
		return nil, err
	}
	switch v := res.(type) {
	case *successResponse:
		return v, nil
	case *db.DatabaseError:
		return nil, v
	}
	return nil, errors.New("Unknown response")
}

func (b *bolt3) connect(auth map[string]interface{}) error {
	// Only allowed to connect when in disconnected state
	if b.state != bolt3_disconnected {
		return b.invalidStateError([]int{bolt3_disconnected})
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

	// Send hello message
	err := b.sendMsg(msgV3Hello, hello)
	if err != nil {
		return err
	}

	succRes, err := b.receiveSuccessResponse()
	if err != nil {
		return err
	}
	helloRes := succRes.hello()
	if helloRes == nil {
		return errors.New("proto error")
	}
	b.connId = helloRes.connectionId
	b.serverVersion = helloRes.server

	// Transition into ready state
	b.state = bolt3_ready
	log("connected")
	return nil
}

func (b *bolt3) TxBegin(
	mode db.AccessMode, bookmarks []string, timeout time.Duration, txMeta map[string]interface{}) (db.Handle, error) {

	log("txBegin try")

	// If streaming auto commit, consume the whole thing. Hard to do discard here since we don't
	// know if server has sent everything on the wire already and the server doesn't
	// like us sending a discard when it is ready.
	switch b.state {
	case bolt3_streaming:
		for {
			_, sum, err := b.Next(b.streamId)
			if sum != nil || err != nil {
				break
			}
		}
	case bolt3_ready:
		// Continue
	default:
		return nil, b.invalidStateError([]int{bolt3_streaming, bolt3_ready})
	}

	// Only allowed to begin a transaction from ready state
	if b.state != bolt3_ready {
		return nil, b.invalidStateError([]int{bolt3_ready})
	}

	// Stash this into pending internal tx
	b.pendingTx = &internalTx{
		mode:      mode,
		bookmarks: bookmarks,
		timeout:   timeout,
		txMeta:    txMeta,
	}

	b.txId = time.Now().Unix()
	// Transition into tx state
	b.state = bolt3_pendingtx

	log("txBegin ok")
	return b.txId, nil
}

func (b *bolt3) TxCommit(txh db.Handle) error {
	log("txCommit try")
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		log("txCommit fail 1")
		return err
	}

	// Nothing to do, a transaction started but no commands were issued on it
	if b.state == bolt3_pendingtx {
		b.state = bolt3_ready
		return nil
	}

	// Can only commit when in tx state or streaming in tx
	if b.state != bolt3_tx && b.state != bolt3_streamingtx {
		log("txCommit fail 2")
		return b.invalidStateError([]int{bolt3_tx})
	}

	err = b.consumeStream()
	if err != nil {
		return err
	}

	err = b.sendMsg(msgV3Commit)
	if err != nil {
		log("txCommit fail 3")
		return err
	}

	succRes, err := b.receiveSuccessResponse()
	if err != nil {
		return err
	}
	commitSuccess := succRes.commit()
	if commitSuccess == nil {
		return errors.New("Parser error")
	}
	if len(commitSuccess.bookmark) > 0 {
		b.bookmark = commitSuccess.bookmark
	}

	// Transition into ready state
	b.state = bolt3_ready

	log("txCommit ok")
	return nil
}

func (b *bolt3) TxRollback(txh db.Handle) error {
	log("txRollback try")
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		log("txRollback fail 1")
		return err
	}

	// Nothing to do, a transaction started but no commands were issued on it
	if b.state == bolt3_pendingtx {
		b.state = bolt3_ready
		return nil
	}

	// Can only rollback when in tx state or streaming in tx
	if b.state != bolt3_tx && b.state != bolt3_streamingtx {
		log("txRollback fail 2")
		return b.invalidStateError([]int{bolt3_tx})
	}

	err = b.consumeStream()
	if err != nil {
		return err
	}

	err = b.sendMsg(msgV3Rollback)
	if err != nil {
		log("txRollback fail 3")
		return err
	}

	_, err = b.receiveSuccessResponse()
	if err != nil {
		log("txRollback fail 4")
		return err
	}

	// Transition into ready state
	b.state = bolt3_ready

	//  *bolt.successResponse: &{m:map[]}
	log("txRollback ok")
	return nil
}

// Discards all records, keeps bookmark
func (b *bolt3) consumeStream() error {
	// Anything to do?
	if b.state != bolt3_streaming && b.state != bolt3_streamingtx {
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

func (b *bolt3) run(cypher string, params map[string]interface{}, tx *internalTx) (*db.Stream, error) {
	log(fmt.Sprintf("run try:%s", cypher))

	// If streaming, consume the whole thing. Hard to do discard here since we don't
	// know if server has sent everything on the wire already and the server doesn't
	// like us sending a discard when it is ready.
	err := b.consumeStream()
	if err != nil {
		return nil, err
	}

	if b.state != bolt3_tx && b.state != bolt3_ready && b.state != bolt3_pendingtx {
		return nil, b.invalidStateError([]int{bolt3_tx, bolt3_ready, bolt3_pendingtx})
	}

	var meta map[string]interface{}
	if tx != nil {
		meta = tx.toMeta()
	}

	if b.state == bolt3_pendingtx {
		err := b.appendMsg(msgV3Begin, meta)
		if err != nil {
			return nil, err
		}
		meta = nil
	}

	// Run
	err = b.appendMsg(msgV3Run, cypher, params, meta)
	if err != nil {
		return nil, err
	}

	// Begin pull
	err = b.appendMsg(msgV3PullAll)
	if err != nil {
		return nil, err
	}

	// Send all
	log("sending")
	err = b.chunker.send()
	if err != nil {
		return nil, err
	}

	if b.state == bolt3_pendingtx {
		// Receive confirmation of BEGIN
		_, err := b.receiveSuccessResponse()
		if err != nil {
			b.state = bolt3_corrupt
			return nil, err
		}
		b.state = bolt3_tx
	}

	// Receive confirmation of RUN
	res, err := b.receive()
	if err != nil {
		return nil, err
	}
	switch v := res.(type) {
	case *successResponse:
		// Extract the RUN response from success response
		runRes := v.run()
		if runRes == nil {
			b.state = bolt3_corrupt
			return nil, errors.New("parse fail, proto error")
		}
		// Succesful, change state to streaming and let Next receive the records.
		b.tfirst = runRes.t_first
		switch b.state {
		case bolt3_ready:
			b.state = bolt3_streaming
		case bolt3_tx:
			b.state = bolt3_streamingtx
		}
		b.streamKeys = runRes.fields
	case *db.DatabaseError:
		b.state = bolt3_failed
		return nil, v
	default:
		b.state = bolt3_corrupt
		return nil, errors.New("Unknown message")
	}

	b.streamId = time.Now().Unix()
	stream := &db.Stream{Keys: b.streamKeys, Handle: b.streamId}
	log(fmt.Sprintf("run ok, streaming: %d", b.state))
	return stream, nil
}

func (b *bolt3) Run(
	cypher string, params map[string]interface{}, mode db.AccessMode, bookmarks []string, timeout time.Duration, txMeta map[string]interface{}) (*db.Stream, error) {

	log("Run no tx")
	if b.state != bolt3_streaming && b.state != bolt3_ready {
		return nil, b.invalidStateError([]int{bolt3_streaming, bolt3_ready})
	}
	tx := internalTx{
		mode:      mode,
		bookmarks: bookmarks,
		timeout:   timeout,
		txMeta:    txMeta,
	}
	return b.run(cypher, params, &tx)
}

func (b *bolt3) RunTx(txh db.Handle, cypher string, params map[string]interface{}) (*db.Stream, error) {
	log("Run tx")

	err := b.assertHandle(b.txId, txh)
	if err != nil {
		return nil, err
	}

	stream, err := b.run(cypher, params, b.pendingTx)
	b.pendingTx = nil
	return stream, err
}

// Reads one record from the stream.
func (b *bolt3) Next(shandle db.Handle) (*db.Record, *db.Summary, error) {
	if b.state != bolt3_streaming && b.state != bolt3_streamingtx {
		return nil, nil, b.invalidStateError([]int{bolt3_streaming, bolt3_streamingtx})
	}

	err := b.assertHandle(b.streamId, shandle)
	if err != nil {
		return nil, nil, err
	}

	res, err := b.receive()
	if err != nil {
		b.state = bolt3_corrupt
		return nil, nil, err
	}

	switch x := res.(type) {
	case *recordResponse:
		rec := &db.Record{Keys: b.streamKeys, Values: x.values}
		log("got record")
		return rec, nil, nil
	case *successResponse:
		log("got end of stream")
		// End of stream
		// Parse summary
		sum := x.summary()
		if sum == nil {
			b.state = bolt3_corrupt
			log(fmt.Sprintf("failed to parse summary: %+v", x))
			return nil, nil, errors.New("Failed to parse summary")
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
	case *db.DatabaseError:
		log(fmt.Sprintf("got failure: %s", x))
		return nil, nil, x
	default:
		return nil, nil, errors.New("Unknown response")
	}
}

func (b *bolt3) Bookmark() string {
	return b.bookmark
}

func (b *bolt3) IsAlive() bool {
	switch b.state {
	case bolt3_disconnected, bolt3_corrupt:
		log("IsAlive response: dead")
		return false
	}
	log("IsAlive response: alive")
	return true
}

func (b *bolt3) Reset() {
	log("Reset try")
	defer func() {
		// Reset internal state
		b.txId = 0
		b.streamId = 0
		b.streamKeys = []string{}
		b.bookmark = ""
	}()

	switch b.state {
	case bolt3_ready, bolt3_disconnected, bolt3_corrupt:
		// No need for reset
		log("NOP")
		return
	}

	// Send the reset message to the server
	err := b.sendMsg(msgV3Reset)
	if err != nil {
		b.state = bolt3_corrupt
		return
	}

	// If the server was streaming we need to clean everything that
	// might have been sent by the server before it received the reset.
	log(fmt.Sprintf("Draining %d", b.state))
	drained := false
	for !drained {
		res, err := b.receive()
		if err != nil {
			b.state = bolt3_corrupt
			return
		}
		switch x := res.(type) {
		case *recordResponse, *ignoredResponse:
			// Just throw away. We should only get record responses while in streaming mode.
		case *db.DatabaseError:
			// This could mean that the reset command failed for some reason, could also
			// mean some other command that failed but as long as we never have unconfirmed
			// commands out of the handling functions this should mean that the reset failed.
			b.state = bolt3_corrupt
			return
		case *successResponse:
			// This could indicate either end of a stream that have been sent right before
			// the reset or it could be a confirmation of the reset.
			sum := x.summary()
			drained = sum == nil
		}
	}

	b.state = bolt3_ready
}

func (b *bolt3) Close() {
	log("Close")
	if b.state == bolt3_disconnected {
		return
	}

	b.sendMsg(msgV3Goodbye)
	b.conn.Close()
	b.state = bolt3_disconnected
}
