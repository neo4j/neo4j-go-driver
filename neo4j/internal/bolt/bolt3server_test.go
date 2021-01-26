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
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

// Fake of bolt3 server.
// Utility to test bolt3 protocol implemntation.
// Use panic upon errors, simplifies output when server is running within a go thread
// in the test.
type bolt3server struct {
	conn     net.Conn
	unpacker *packstream.Unpacker
	out      *outgoing
}

func newBolt3Server(conn net.Conn) *bolt3server {
	return &bolt3server{
		unpacker: &packstream.Unpacker{},
		conn:     conn,
		out: &outgoing{
			chunker: newChunker(),
			packer:  &packstream.Packer{},
		},
	}
}

func (s *bolt3server) waitForHandshake() []byte {
	handshake := make([]byte, 4*5)
	_, err := io.ReadFull(s.conn, handshake)
	if err != nil {
		panic(err)
	}
	return handshake
}

func (s *bolt3server) assertStructType(msg *testStruct, t byte) {
	if msg.tag != t {
		panic(fmt.Sprintf("Got wrong type of message expected %d but got %d (%+v)", t, msg.tag, msg))
	}
}

func (s *bolt3server) sendFailureMsg(code, msg string) {
	s.send(msgFailure, map[string]interface{}{
		"code":    code,
		"message": msg,
	})
}

func (s *bolt3server) sendIgnoredMsg() {
	s.send(msgIgnored)
}

func (s *bolt3server) waitForHello() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgHello)
	m := msg.fields[0].(map[string]interface{})
	// Hello should contain some musts
	_, exists := m["scheme"]
	if !exists {
		s.sendFailureMsg("?", "Missing scheme in hello")
	}
	_, exists = m["user_agent"]
	if !exists {
		s.sendFailureMsg("?", "Missing user_agent in hello")
	}
}

func (s *bolt3server) receiveMsg() *testStruct {
	_, buf, err := dechunkMessage(s.conn, []byte{})
	if err != nil {
		panic(err)
	}

	s.unpacker.Reset(buf)
	s.unpacker.Next()
	n := s.unpacker.Len()
	t := s.unpacker.StructTag()

	fields := make([]interface{}, n)
	for i := uint32(0); i < n; i++ {
		s.unpacker.Next()
		fields[i] = serverHydrator(s.unpacker)
	}
	return &testStruct{tag: t, fields: fields}
}

func (s *bolt3server) waitForRun() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRun)
}

func (s *bolt3server) waitForReset() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgReset)
}

func (s *bolt3server) waitForTxBegin() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgBegin)
}

func (s *bolt3server) waitForTxCommit() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgCommit)
}

func (s *bolt3server) waitForTxRollback() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRollback)
}

func (s *bolt3server) waitForPullAll() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgPullAll)
}

func (s *bolt3server) acceptVersion(ver byte) {
	acceptedVer := []byte{0x00, 0x00, 0x00, ver}
	_, err := s.conn.Write(acceptedVer)
	if err != nil {
		panic(err)
	}
}

func (s *bolt3server) rejectVersions() {
	_, err := s.conn.Write([]byte{0x00, 0x00, 0x00, 0x00})
	if err != nil {
		panic(err)
	}
}

func (s *bolt3server) closeConnection() {
	s.conn.Close()
}

func (s *bolt3server) send(tag byte, field ...interface{}) {
	s.out.appendX(byte(tag), field...)
	s.out.send(s.conn)
}

func (s *bolt3server) sendSuccess(m map[string]interface{}) {
	s.send(msgSuccess, m)
}

func (s *bolt3server) acceptHello() {
	s.send(msgSuccess, map[string]interface{}{
		"connection_id": "cid",
		"server":        "fake/3.5",
	})
}

func (s *bolt3server) rejectHelloUnauthorized() {
	s.send(msgFailure, map[string]interface{}{
		"code":    "Neo.ClientError.Security.Unauthorized",
		"message": "",
	})
}

// Utility when something else but connect is to be tested
func (s *bolt3server) accept(ver byte) {
	s.waitForHandshake()
	s.acceptVersion(ver)
	s.waitForHello()
	s.acceptHello()
}

// Utility to wait and serve a auto commit query
func (s *bolt3server) serveRun(stream []testStruct) {
	s.waitForRun()
	s.waitForPullAll()
	for _, x := range stream {
		s.send(x.tag, x.fields...)
	}
}

func (s *bolt3server) serveRunTx(stream []testStruct, commit bool, bookmark string) {
	s.waitForTxBegin()
	s.send(msgSuccess, map[string]interface{}{})
	s.waitForRun()
	s.waitForPullAll()
	for _, x := range stream {
		s.send(x.tag, x.fields...)
	}
	if commit {
		s.waitForTxCommit()
		s.out.appendX(byte(msgSuccess), map[string]interface{}{
			"bookmark": bookmark,
		})
		s.out.send(s.conn)
	} else {
		s.waitForTxRollback()
		s.send(msgSuccess, map[string]interface{}{})
	}
}

func setupBolt3Pipe(t *testing.T) (net.Conn, *bolt3server, func()) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}

	addr := l.Addr()
	clientConn, err := net.Dial(addr.Network(), addr.String())

	srvConn, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept error: %s", err)
	}
	srv := newBolt3Server(srvConn)

	return clientConn, srv, func() {
		l.Close()
	}
}
