/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bolt

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
)

// Fake of bolt5 server.
// Utility to test bolt5 protocol implementation.
// Use panic upon errors, simplifies output when server is running within a go thread
// in the test.
type bolt5server struct {
	conn     net.Conn
	unpacker *packstream.Unpacker
	out      *outgoing
}

func newBolt5Server(conn net.Conn) *bolt5server {
	return &bolt5server{
		unpacker: &packstream.Unpacker{},
		conn:     conn,
		out: &outgoing{
			chunker: newChunker(),
			packer:  packstream.Packer{},
		},
	}
}

func (s *bolt5server) waitForHandshake() []byte {
	handshake := make([]byte, 4*5)
	_, err := io.ReadFull(s.conn, handshake)
	if err != nil {
		panic(err)
	}
	return handshake
}

func (s *bolt5server) assertStructType(msg *testStruct, t byte) {
	if msg.tag != t {
		panic(fmt.Sprintf("Got wrong type of message expected %d but got %d (%+v)", t, msg.tag, msg))
	}
}

func (s *bolt5server) sendFailureMsg(code, msg string) {
	f := map[string]any{
		"code":    code,
		"message": msg,
	}
	s.send(msgFailure, f)
}

func (s *bolt5server) sendIgnoredMsg() {
	s.send(msgIgnored)
}

// Returns the first hello field
func (s *bolt5server) waitForHello() map[string]any {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgHello)
	m := msg.fields[0].(map[string]any)
	// Hello should contain some musts
	_, exists := m["scheme"]
	if !exists {
		s.sendFailureMsg("?", "Missing scheme in hello")
	}
	_, exists = m["user_agent"]
	if !exists {
		s.sendFailureMsg("?", "Missing user_agent in hello")
	}
	return m
}

// Returns the first hello field
func (s *bolt5server) waitForHelloWithoutAuthToken() map[string]any {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgHello)
	m := msg.fields[0].(map[string]any)
	// Hello should contain some musts
	_, exists := m["user_agent"]
	if !exists {
		s.sendFailureMsg("?", "Missing user_agent in hello")
	}
	return m
}

// Returns the first hello field
func (s *bolt5server) waitForLogon() map[string]any {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgLogon)
	m := msg.fields[0].(map[string]any)
	// Hello should contain some musts
	_, exists := m["scheme"]
	if !exists {
		s.sendFailureMsg("?", "Missing scheme in logon")
	}
	return m
}

func (s *bolt5server) receiveMsg() *testStruct {
	_, buf, err := dechunkMessage(context.Background(), s.conn, []byte{}, -1)
	if err != nil {
		panic(err)
	}
	s.unpacker.Reset(buf)
	s.unpacker.Next()
	n := s.unpacker.Len()
	t := s.unpacker.StructTag()

	fields := make([]any, n)
	for i := uint32(0); i < n; i++ {
		s.unpacker.Next()
		fields[i] = serverHydrator(s.unpacker)
	}
	return &testStruct{tag: t, fields: fields}
}

func (s *bolt5server) waitForRun(assertFields func(fields []any)) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRun)
	if assertFields != nil {
		assertFields(msg.fields)
	}
}

func (s *bolt5server) waitForReset() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgReset)
}

func (s *bolt5server) waitForTxBegin(assertFields func(fields []any)) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgBegin)
	if assertFields != nil {
		assertFields(msg.fields)
	}
}

func (s *bolt5server) waitForTxCommit() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgCommit)
}

func (s *bolt5server) waitForTxRollback() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRollback)
}

func (s *bolt5server) waitForPullN(n int) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgPullN)
	extra := msg.fields[0].(map[string]any)
	sentN := int(extra["n"].(int64))
	if sentN != n {
		panic(fmt.Sprintf("Expected PULL n:%d but got PULL %d", n, sentN))
	}
	_, hasQid := extra["qid"]
	if hasQid {
		panic("Expected PULL without qid")
	}
}

func (s *bolt5server) waitForDiscardN(n int) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgDiscardN)
	extra := msg.fields[0].(map[string]any)
	sentN := int(extra["n"].(int64))
	if sentN != n {
		panic(fmt.Sprintf("Expected DISCARD n:%d but got DISCARD %d", n, sentN))
	}
	_, hasQid := extra["qid"]
	if hasQid {
		panic("Expected DISCARD without qid")
	}
}

func (s *bolt5server) waitForRoute(assertRoute func(fields []any)) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRoute)
	if assertRoute != nil {
		assertRoute(msg.fields)
	}
}

func (s *bolt5server) acceptVersion(major, minor byte) {
	acceptedVer := []byte{0x00, 0x00, minor, major}
	_, err := s.conn.Write(acceptedVer)
	if err != nil {
		panic(err)
	}
}

func (s *bolt5server) closeConnection() {
	_ = s.conn.Close()
}

func (s *bolt5server) send(tag byte, field ...any) {
	s.out.appendX(tag, field...)
	s.out.send(context.Background(), s.conn)
}

func (s *bolt5server) sendSuccess(m map[string]any) {
	s.send(msgSuccess, m)
}

func (s *bolt5server) acceptHello() {
	s.send(msgSuccess, map[string]any{
		"connection_id": "cid",
		"server":        "fake/4.5",
	})
}
func (s *bolt5server) acceptLogon() {
	s.sendSuccess(nil)
}

func (s *bolt5server) acceptHelloWithHints(hints map[string]any) {
	s.send(msgSuccess, map[string]any{
		"connection_id": "cid",
		"server":        "fake/4.5",
		"hints":         hints,
	})
}

func (s *bolt5server) rejectHelloUnauthorized() {
	s.send(msgFailure, map[string]any{
		"code":    "Neo.ClientError.Security.Unauthorized",
		"message": "",
	})
}

// Utility when something else but connect is to be tested
func (s *bolt5server) accept(ver byte) {
	s.waitForHandshake()
	s.acceptVersion(ver, 0)
	s.waitForHello()
	s.acceptHello()
}

func (s *bolt5server) acceptWithMinor(major, minor byte) {
	s.waitForHandshake()
	s.acceptVersion(major, minor)
	if minor >= 1 {
		s.waitForHelloWithoutAuthToken()
		s.acceptHello()
		s.waitForLogon()
		s.acceptLogon()
	} else {
		s.waitForHello()
		s.acceptHello()
	}
}

// Utility to wait and serve an auto commit query
func (s *bolt5server) serveRun(stream []testStruct, assertRun func([]any)) {
	s.waitForRun(assertRun)
	s.waitForPullN(bolt5FetchSize)
	for _, x := range stream {
		s.send(x.tag, x.fields...)
	}
}

func (s *bolt5server) serveRunTx(stream []testStruct, commit bool, bookmark string) {
	s.waitForTxBegin(nil)
	s.send(msgSuccess, map[string]any{})
	s.waitForRun(nil)
	s.waitForPullN(bolt5FetchSize)
	for _, x := range stream {
		s.send(x.tag, x.fields...)
	}
	if commit {
		s.waitForTxCommit()
		s.send(msgSuccess, map[string]any{
			"bookmark": bookmark,
		})
	} else {
		s.waitForTxRollback()
		s.send(msgSuccess, map[string]any{})
	}
}

func (s *bolt5server) rejectLogonWithoutAuthToken() {
	s.send(msgFailure, map[string]any{
		"code":    "Neo.ClientError.Security.Unauthorized",
		"message": "",
	})
}

func setupBolt5Pipe(t *testing.T) (net.Conn, *bolt5server, func()) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}

	addr := l.Addr()
	clientConn, _ := net.Dial(addr.Network(), addr.String())

	srvConn, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept error: %s", err)
	}
	srv := newBolt5Server(srvConn)

	return clientConn, srv, func() {
		_ = l.Close()
	}
}
