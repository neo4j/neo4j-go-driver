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

	"github.com/neo4j/neo4j-go-driver/neo4j/api"
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

// State that belongs to a certain session. Upon reset this state is wiped.
// All members should have a default value that gets a proper value with empty init.
type sessionState struct {
	//isInTx     bool
	isMessedUp bool
	result     *result
	cypher     string
	params     map[string]interface{}
}

type bolt3 struct {
	conn          net.Conn
	chunker       *chunker
	connected     bool
	packer        *packstream.Packer
	unpacker      *packstream.Unpacker
	state         sessionState
	connId        string
	serverVersion string
}

func NewBolt3(conn net.Conn) *bolt3 {
	// Wrap connection reading/writing in chunk reader/writer
	chunker := newChunker(conn, 4096)
	dechunker := newDechunker(conn)

	return &bolt3{
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
		b.state.isMessedUp = true
		return err
	}
	return nil
}

// TODO: Error types!
func (b *bolt3) connect() error {
	// Server is assumed to be in CONNECTED state, send hello message with proper authentication
	// info to the server to make it transition into READY
	err := b.appendMsg(
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
	err = b.chunker.send()
	if err != nil {
		return err
	}

	// Read response from server
	res, err := b.unpacker.UnpackStruct(b)
	if err != nil {
		return err
	}
	switch v := res.(type) {
	case *successResponse:
		b.connId, b.serverVersion, err = b.successResponseToConnectionInfo(v)
		if err != nil {
			return err
		}
		b.connected = true
		return nil
	case *failureResponse:
		return errors.New("Received failure from server")
	case *ignoredResponse:
		return errors.New("Received ignored from server")
	default:
		return errors.New("Unknown response")
	}
}

func (b *bolt3) RunAutoCommit(cypher string, params map[string]interface{} /*, timeout time.Duration, metadata map[string]interface{}*/) (api.Result, error) {
	if !b.connected || b.state.isMessedUp {
		return nil, errors.New("Not alive")
	}

	// TODO: Ensure no transaction open already

	// If there is a pending result, ask the result to pull and discard all records before proceeding.
	// TODO: Use DiscardAll message?
	if b.state.result != nil {
		b.state.result.ConsumeAll()
		b.state.result = nil
	}

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

	// Read Run response from server
	res, err := b.unpacker.UnpackStruct(b)
	if err != nil {
		return nil, err
	}
	switch v := res.(type) {
	case *successResponse:
		b.state.result, err = b.successResponseToResult(v)
		if err != nil {
			b.state.isMessedUp = true
			return nil, err
		}
		b.state.cypher = cypher
		b.state.params = params
		return b.state.result, nil
	case *failureResponse:
		// Returning here assumes that we don't get any response on the pull message
		// Check code to determine if we're messed up, retry logic should be handled in session.
		return nil, v
	default:
		b.state.isMessedUp = true
		return nil, errors.New("Unexpected response")
	}
}

// Try to create a response from a success response.
func (b *bolt3) successResponseToResult(r *successResponse) (*result, error) {
	m := r.Map()
	// Should be a list of keys returned from query
	keysx, ok := m["fields"].([]interface{})
	if !ok {
		return nil, errors.New("Missing fields in success response")
	}
	// Transform keys to proper format
	keys := make([]string, len(keysx))
	for i, x := range keysx {
		keys[i], ok = x.(string)
		if !ok {
			return nil, errors.New("Field is not string")
		}
	}
	return newResult(keys, b.readRecord), nil
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

func (b *bolt3) successResponseToSummary(r *successResponse) (*summary, error) {
	m := r.Map()
	// Should be a bookmark
	bm, ok := m["bookmark"].(string)
	if !ok {
		return nil, errors.New("Missing bookmark")
	}
	// Should be a statement type
	st, ok := m["type"].(string)
	if !ok {
		return nil, errors.New("Missing stmnt type")
	}
	return &summary{
		bookmark:      bm,
		stmntType:     st,
		cypher:        b.state.cypher,
		params:        b.state.params,
		serverVersion: b.serverVersion,
	}, nil
}

// Reads one record from the stream.
func (b *bolt3) readRecord() (*record, *summary, error) {
	res, err := b.unpacker.UnpackStruct(b)
	if err != nil {
		return nil, nil, err
	}

	switch x := res.(type) {
	case *recordResponse:
		rec := &record{keys: b.state.result.keys, values: x.fields[0].([]interface{})}
		return rec, nil, nil
	case *successResponse:
		sum, err := b.successResponseToSummary(x)
		if err != nil {
			b.state.isMessedUp = true
			return nil, nil, err
		}
		return nil, sum, nil
	case *failureResponse:
		// Returning here assumes that we don't get any response on the pull message
		return nil, nil, x
	default:
		return nil, nil, errors.New("Unknown response")
	}
}

func (b *bolt3) IsAlive() bool {
	return b.connected && !b.state.isMessedUp
}

func (b *bolt3) Close() error {
	// Already closed?
	if !b.connected {
		return nil
	}

	// TODO: Pending result?

	var errs []error

	// Append goodbye message to existing chunks
	err := b.packer.PackStruct(msgV3Goodbye)
	if err != nil {
		// Still need to close the connection and send all pending messages that might be critical.
		// So just remember the error and continue. Should return this error to client!
		errs = append(errs, err)
	}

	// Send all pending messages
	err = b.chunker.send()
	if err != nil {
		errs = append(errs, err)
	}

	// Close the connection
	err = b.conn.Close()
	b.connected = false
	if err != nil {
		errs = append(errs, err)
	}

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
