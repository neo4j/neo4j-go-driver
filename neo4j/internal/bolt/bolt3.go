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

type bolt3 struct {
	state         int
	txId          int64
	streamId      int64
	steamKeys     []string
	conn          net.Conn
	chunker       *chunker
	packer        *packstream.Packer
	unpacker      *packstream.Unpacker
	connId        string
	serverVersion string
}

func NewBolt3(conn net.Conn) *bolt3 {
	// Wrap connection reading/writing in chunk reader/writer
	chunker := newChunker(conn, 4096)
	dechunker := newDechunker(conn)

	return &bolt3{
		state:    disconnected,
		conn:     conn,
		chunker:  chunker,
		packer:   packstream.NewPacker(chunker),
		unpacker: packstream.NewUnpacker(dechunker),
	}
}

func (b *bolt3) appendMsg(tag packstream.StructTag, field ...interface{}) error {
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

func (b *bolt3) receiveSuccessResponse() (*successResponse, error) {
	res, err := b.unpacker.UnpackStruct(b)
	if err != nil {
		return nil, err
	}
	switch v := res.(type) {
	case *successResponse:
		return v, nil
	case *failureResponse:
		return nil, errors.New("Received failure from server")
	}
	return nil, errors.New("Unknown response")
}

// TODO: Error types!
func (b *bolt3) connect() error {
	// Only allowed to connect when in disconnected state
	if b.state != disconnected {
		return b.invalidStateError([]int{disconnected})
	}

	// Send hello message with proper authentication
	// TODO: Authentication
	err := b.sendMsg(
		msgV3Hello,
		map[string]interface{}{
			"user_agent":  userAgent,
			"scheme":      "basic",
			"principal":   "neo4j",
			"credentials": "pass",
		})
	if err != nil {
		return err
	}

	succRes, err := b.receiveSuccessResponse()
	if err != nil {
		return err
	}
	b.connId, b.serverVersion, err = b.successResponseToConnectionInfo(succRes)
	if err != nil {
		return err
	}
	// Transition into ready state
	b.state = ready
	return nil
}

func (b *bolt3) TxBegin(
	mode conn.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (conn.Handle, error) {

	// Only allowed to begin a transaction from ready state
	if b.state != ready {
		return nil, b.invalidStateError([]int{ready})
	}

	// TODO: Lazy begin, defer this to when Run is called
	smode := "WRITE"
	if mode == conn.ReadMode {
		smode = "READ"
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

	return b.txId, nil
}

func (b *bolt3) TxCommit(txh conn.Handle) error {
	// Can only commit when in tx state
	if b.state != tx {
		return b.invalidStateError([]int{tx})
	}
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		return err
	}

	err = b.sendMsg(msgV3Commit)
	if err != nil {
		return err
	}

	_, err = b.receiveSuccessResponse()
	if err != nil {
		return err
	}

	// Transition into ready state
	b.state = ready
	// TODO: Keep track of bookmark!
	// bolt.successResponse: &{m:map[bookmark:neo4j:bookmark:v1:tx35]}
	//fmt.Printf("Got commit response %T: %+v\n", res, res)
	return nil
}

func (b *bolt3) TxRollback(txh conn.Handle) error {
	// Can only commit when in tx state
	if b.state != tx {
		return b.invalidStateError([]int{tx})
	}
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		return err
	}

	err = b.sendMsg(msgV3Rollback)
	if err != nil {
		return err
	}

	_, err = b.receiveSuccessResponse()
	if err != nil {
		return err
	}

	// Transition into ready state
	b.state = ready

	//  *bolt.successResponse: &{m:map[]}
	return nil
}

func (b *bolt3) run(successState int, cypher string, params map[string]interface{}) (*conn.Stream, error) {
	// Send request to run query along with request to stream the result
	// TODO: Meta data
	err := b.appendMsg(msgV3Run,
		cypher,
		params,
		map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	err = b.appendMsg(msgV3PullAll)
	if err != nil {
		return nil, err
	}

	err = b.chunker.send()
	if err != nil {
		return nil, err
	}

	// Receive run response
	succRes, err := b.receiveSuccessResponse()
	if err != nil {
		return nil, err
	}

	err = b.successResponseStreamStart(succRes)
	if err != nil {
		b.state = corrupt
		return nil, err
	}

	b.streamId = time.Now().Unix()
	b.state = successState
	stream := &conn.Stream{Keys: b.steamKeys, Handle: b.streamId}
	return stream, nil
}

func (b *bolt3) Run(cypher string, params map[string]interface{}) (*conn.Stream, error) {
	if b.state != ready {
		return nil, b.invalidStateError([]int{ready})
	}

	return b.run(streaming, cypher, params)
}

func (b *bolt3) RunTx(txh conn.Handle, cypher string, params map[string]interface{}) (*conn.Stream, error) {
	if b.state != tx {
		return nil, b.invalidStateError([]int{tx})
	}
	err := b.assertHandle(b.txId, txh)
	if err != nil {
		return nil, err
	}

	return b.run(streamingtx, cypher, params)
}

// Try to create a response from a success response.
func (b *bolt3) successResponseStreamStart(r *successResponse) error {
	m := r.Map()
	// Should be a list of keys returned from query
	keysx, ok := m["fields"].([]interface{})
	if !ok {
		return errors.New("Missing fields in success response")
	}
	// Transform keys to proper format
	keys := make([]string, len(keysx))
	for i, x := range keysx {
		keys[i], ok = x.(string)
		if !ok {
			return errors.New("Field is not string")
		}
	}
	b.steamKeys = keys
	return nil
}

func (b *bolt3) successResponseToConnectionInfo(r *successResponse) (string, string, error) {
	m := r.Map()
	id, ok := m["connection_id"].(string)
	if !ok {
		return "", "", errors.New("Missing connection id in success response")
	}
	server, ok := m["server"].(string)
	if !ok {
		return "", "", errors.New("Missing server in success response")
	}
	return id, server, nil
}

func (b *bolt3) successResponseToSummary(r *successResponse) (*conn.Summary, error) {
	m := r.Map()
	sum := &conn.Summary{
		ServerVersion: b.serverVersion,
	}
	// In tx
	// TODO: Counters
	// &{m:map[stats:map[labels-added:1 nodes-created:1 properties-set:1] t_last:0 type:w]}

	// Should be a bookmark
	bm, ok := m["bookmark"].(string)
	if ok {
		sum.Bookmark = &bm
	}
	// Should be a statement type
	/*
		st, ok := m["type"].(string)
		if !ok {
			return nil, errors.New("Missing stmnt type")
		}
	*/
	return sum, nil
}

// Reads one record from the stream.
func (b *bolt3) Next(shandle conn.StreamHandle) (*conn.Record, *conn.Summary, error) {
	if b.state != streaming && b.state != streamingtx {
		return nil, nil, b.invalidStateError([]int{streaming, streamingtx})
	}

	err := b.assertHandle(b.streamId, shandle)
	if err != nil {
		return nil, nil, err
	}

	res, err := b.unpacker.UnpackStruct(b)
	if err != nil {
		return nil, nil, err
	}

	switch x := res.(type) {
	case *recordResponse:
		rec := &conn.Record{Keys: b.steamKeys, Values: x.fields[0].([]interface{})}
		return rec, nil, nil
	case *successResponse:
		sum, err := b.successResponseToSummary(x)
		if err != nil {
			b.state = corrupt
			return nil, nil, err
		}
		// End of stream
		if b.state == streamingtx {
			b.state = tx
		} else {
			b.state = ready
		}
		return nil, sum, nil
	case *failureResponse:
		// Returning here assumes that we don't get any response on the pull message
		return nil, nil, x
	default:
		return nil, nil, errors.New("Unknown response")
	}
}

/*
func (b *bolt3) IsAlive() bool {
	return b.connected && !b.state.isMessedUp
}
*/
func (b *bolt3) Close() error {
	// Already closed?
	if b.state == disconnected {
		return nil
	}

	// TODO: Active stream?
	// TODO: Active tx?

	var errs []error

	// Append goodbye message to existing chunks
	err := b.sendMsg(msgV3Goodbye)
	if err != nil {
		// Still need to close the connection and send all pending messages that might be critical.
		// So just remember the error and continue. Should return this error to client!
		errs = append(errs, err)
	}

	// Close the connection
	err = b.conn.Close()
	if err != nil {
		errs = append(errs, err)
	}
	b.state = disconnected

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Hydrator implements packstream HydratorFactory which gives this connection full control
// over how structs are hydrated during stream unpacking.
func (b *bolt3) Hydrator(tag packstream.StructTag, numFields int) (packstream.Hydrator, error) {
	switch tag {
	case msgV3Success:
		return &successResponse{}, nil
	case msgV3Ignored:
		return &ignoredResponse{}, nil
	case msgV3Failure:
		return &failureResponse{}, nil
	case msgV3Record:
		return &recordResponse{}, nil
	case 'N':
		return &node{}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unknown tag: %02x", tag))
	}
}
