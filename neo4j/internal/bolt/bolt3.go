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

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
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
	ready        = iota // After connect and reset
	disconnected        // Lost connection to server
	corrupt             // Non recoverable protocol error
	streaming           // Receiving result from auto commit query
	tx                  // In a transaction
	streamingtx         // Receiving result from a query within a transaction
)

func log(msg string) {
	fmt.Printf("bolt3: %s\n", msg)
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
	serverVersion string
}

func NewBolt3(serverName string, conn net.Conn) *bolt3 {
	// Wrap connection reading/writing in chunk reader/writer
	chunker := newChunker(conn, 4096)
	dechunker := newDechunker(conn)

	return &bolt3{
		state:      disconnected,
		conn:       conn,
		serverName: serverName,
		chunker:    chunker,
		packer:     packstream.NewPacker(chunker, dehydrate),
		unpacker:   packstream.NewUnpacker(dechunker),
	}
}

func (b *bolt3) ServerName() string {
	return b.serverName
}

func (b *bolt3) appendMsg(tag packstream.StructTag, field ...interface{}) error {
	log(fmt.Sprintf("appending tag: %d", tag))
	// Each message in it's own chunk
	b.chunker.add()
	// Setup the message and let packstream write the packed bytes to the chunk
	err := b.packer.PackStruct(tag, field...)
	if err != nil {
		// At this point we do not know the state of what has been written to the chunks.
		// Either we should support rolling back whatever that has been written or just
		// bail out this session.
		b.state = corrupt
		return err
	}
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

func (b *bolt3) assertHandle(id int64, h conn.Handle) error {
	hid, ok := h.(int64)
	if !ok || hid != id {
		return errors.New("Invalid handle")
	}
	return nil
}

func (b *bolt3) receive() (interface{}, error) {
	res, err := b.unpacker.UnpackStruct(hydrate)
	if err != nil {
		log(fmt.Sprintf("receive error: %s", err))
	}
	if res != nil {
		log(fmt.Sprintf("received: %+v", res))
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
	case *failureResponse:
		return nil, v
	}
	return nil, errors.New("Unknown response")
}

// TODO: Error types!
func (b *bolt3) connect(auth map[string]interface{}) error {
	// Only allowed to connect when in disconnected state
	if b.state != disconnected {
		return b.invalidStateError([]int{disconnected})
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
	b.state = ready
	log("connected")
	return nil
}

func (b *bolt3) TxBegin(
	mode conn.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (conn.Handle, error) {

	log("txBegin try")

	// If streaming auto commit, consume the whole thing. Hard to do discard here since we don't
	// know if server has sent everything on the wire already and the server doesn't
	// like us sending a discard when it is ready.
	switch b.state {
	case streaming:
		for {
			_, sum, err := b.Next(b.streamId)
			if sum != nil || err != nil {
				break
			}
		}
	case ready:
		// Continue
	default:
		return nil, b.invalidStateError([]int{streaming, ready})
	}

	// Only allowed to begin a transaction from ready state
	if b.state != ready {
		return nil, b.invalidStateError([]int{ready})
	}

	// TODO: Lazy begin, defer this to when Run is called
	smode := "w"
	if mode == conn.ReadMode {
		smode = "r"
	}
	params := map[string]interface{}{
		"mode": smode,
	}
	if len(bookmarks) > 0 {
		params["bookmarks"] = bookmarks
	}
	ts := int(timeout.Seconds())
	if ts > 0 {
		params["tx_timeout"] = ts
	}
	if len(meta) > 0 {
		params["tx_metadata"] = meta
	}
	err := b.sendMsg(msgV3Begin, params)
	if err != nil {
		return nil, err
	}

	_, err = b.receiveSuccessResponse()
	if err != nil {
		return nil, err
	}

	b.txId = time.Now().Unix()
	// Transition into tx state
	b.state = tx

	log("txBegin ok")
	return b.txId, nil
}

func (b *bolt3) TxCommit(txh conn.Handle) error {
	log("txCommit try")
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		log("txCommit fail 1")
		return err
	}

	// Can only commit when in tx state or streaming in tx
	if b.state != tx && b.state != streamingtx {
		log("txCommit fail 2")
		return b.invalidStateError([]int{tx})
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

	_, err = b.receiveSuccessResponse()
	if err != nil {
		log("txCommit fail 4")
		return err
	}

	// Transition into ready state
	b.state = ready
	// TODO: Keep track of bookmark!
	// bolt.successResponse: &{m:map[bookmark:neo4j:bookmark:v1:tx35]}
	log("txCommit ok")
	return nil
}

func (b *bolt3) TxRollback(txh conn.Handle) error {
	log("txRollback try")
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		log("txRollback fail 1")
		return err
	}

	// Can only rollback when in tx state or streaming in tx
	if b.state != tx && b.state != streamingtx {
		log("txRollback fail 2")
		return b.invalidStateError([]int{tx})
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
	b.state = ready

	//  *bolt.successResponse: &{m:map[]}
	log("txRollback ok")
	return nil
}

// Discards all records, keeps bookmark
func (b *bolt3) consumeStream() error {
	// Anything to do?
	if b.state != streaming && b.state != streamingtx {
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

func (b *bolt3) run(cypher string, params map[string]interface{}) (*conn.Stream, error) {
	// Send request to run query along with request to stream the result
	// Same format as tx begin
	// TODO: bookmarks
	// TODO: txTimeout
	// TODO: accessMode
	// TODO: txMetadata
	log(fmt.Sprintf("run try:%s", cypher))

	// If streaming, consume the whole thing. Hard to do discard here since we don't
	// know if server has sent everything on the wire already and the server doesn't
	// like us sending a discard when it is ready.
	err := b.consumeStream()
	if err != nil {
		return nil, err
	}

	if b.state != tx && b.state != ready {
		return nil, b.invalidStateError([]int{tx, ready})
	}

	// Run
	err = b.appendMsg(msgV3Run,
		cypher,
		params,
		map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	// Begin pull
	err = b.appendMsg(msgV3PullAll)
	if err != nil {
		return nil, err
	}

	// Send all
	err = b.chunker.send()
	if err != nil {
		return nil, err
	}

	// Receive RUN response
	succRes, err := b.receiveSuccessResponse()
	if err != nil {
		return nil, err
	}
	runRes := succRes.run()
	if runRes == nil {
		b.state = corrupt
		return nil, errors.New("parse fail, proto error")
	}

	switch b.state {
	case ready:
		b.state = streaming
	case tx:
		b.state = streamingtx
	}

	b.streamKeys = runRes.fields
	b.streamId = time.Now().Unix()
	stream := &conn.Stream{Keys: b.streamKeys, Handle: b.streamId}
	log(fmt.Sprintf("run ok, streaming: %d", b.state))
	return stream, nil
}

func (b *bolt3) Run(cypher string, params map[string]interface{}) (*conn.Stream, error) {
	log("Run no tx")
	return b.run(cypher, params)
}

func (b *bolt3) RunTx(txh conn.Handle, cypher string, params map[string]interface{}) (*conn.Stream, error) {
	log("Run tx")

	err := b.assertHandle(b.txId, txh)
	if err != nil {
		return nil, err
	}

	return b.run(cypher, params)
}

// Reads one record from the stream.
func (b *bolt3) Next(shandle conn.Handle) (*conn.Record, *conn.Summary, error) {
	log("next try")
	if b.state != streaming && b.state != streamingtx {
		return nil, nil, b.invalidStateError([]int{streaming, streamingtx})
	}

	err := b.assertHandle(b.streamId, shandle)
	if err != nil {
		return nil, nil, err
	}

	res, err := b.receive()
	if err != nil {
		log(fmt.Sprintf("Next unpack err: %s", err))
		b.state = corrupt
		return nil, nil, err
	}

	switch x := res.(type) {
	case *recordResponse:
		rec := &conn.Record{Keys: b.streamKeys, Values: x.values}
		log("got record")
		return rec, nil, nil
	case *successResponse:
		log("got end of stream")
		// End of stream
		if b.state == streamingtx {
			b.state = tx
		} else {
			b.state = ready
		}
		b.streamId = 0
		// Parse summary
		sum := x.summary()
		if sum == nil {
			b.state = corrupt
			log(fmt.Sprintf("failed to parse summary: %+v", x))
			return nil, nil, errors.New("Failed to parse summary")
		}
		// TODO: Keep bookmark!
		// Add some extras to the summary (move this elsewhere?)
		sum.ServerVersion = b.serverVersion
		return nil, sum, nil
	case *failureResponse:
		log(fmt.Sprintf("got failure: %s", x))
		return nil, nil, x
	default:
		return nil, nil, errors.New("Unknown response")
	}
}

func (b *bolt3) IsAlive() bool {
	switch b.state {
	case disconnected, corrupt:
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
	}()

	switch b.state {
	case ready, disconnected, corrupt:
		// No need for reset
		log("NOP")
		return
	}

	// Send the reset message to the server
	err := b.sendMsg(msgV3Reset)
	if err != nil {
		b.state = corrupt
		return
	}

	// If the server was streaming we need to clean everything that
	// might have been sent by the server before it received the reset.
	log(fmt.Sprintf("Draining %d", b.state))
	drained := false
	for !drained {
		res, err := b.receive()
		if err != nil {
			b.state = corrupt
			return
		}
		switch x := res.(type) {
		case *recordResponse, *ignoredResponse:
			// Just throw away. We should only get record responses while in streaming mode.
		case *failureResponse:
			// This could mean that the reset command failed for some reason, could also
			// mean some other command that failed but as long as we never have unconfirmed
			// commands out of the handling functions this should mean that the reset failed.
			b.state = corrupt
			return
		case *successResponse:
			// This could indicate either end of a stream that have been sent right before
			// the reset or it could be a confirmation of the reset.
			sum := x.summary()
			drained = sum == nil
		}
	}

	b.state = ready
}

func (b *bolt3) Close() {
	log("Close")
	if b.state == disconnected {
		return
	}

	b.sendMsg(msgV3Goodbye)
	b.conn.Close()
	b.state = disconnected
}
