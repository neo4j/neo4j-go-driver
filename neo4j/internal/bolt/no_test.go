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
	"io"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
)

type testStruct packstream.Struct

func passthroughHydrator(tag packstream.StructTag, fields []interface{}) (interface{}, error) {
	return &testStruct{Tag: tag, Fields: fields}, nil
}

type neo4jServer struct {
	t         *testing.T
	conn      net.Conn
	hyd       packstream.Hydrate
	dechunker *dechunker
	unpacker  *packstream.Unpacker
	chunker   *chunker
	packer    *packstream.Packer
}

func newNeo4jServer(t *testing.T, conn net.Conn) *neo4jServer {
	dechunker := newDechunker(conn)
	unpacker := packstream.NewUnpacker(dechunker)
	chunker := newChunker(conn, 4096)
	packer := packstream.NewPacker(chunker, nil)
	return &neo4jServer{
		t:         t,
		conn:      conn,
		dechunker: dechunker,
		hyd:       passthroughHydrator,
		unpacker:  unpacker,
		packer:    packer,
		chunker:   chunker,
	}
}

func (s *neo4jServer) waitForHandshake() []byte {
	handshake := make([]byte, 4*5)
	_, err := io.ReadFull(s.conn, handshake)
	if err != nil {
		s.t.Fatalf("Failed to read while waiting for handshake: %s", err)
	}
	return handshake
}

func (s *neo4jServer) assertStructType(msg *testStruct, t packstream.StructTag) {
	if msg.Tag != t {
		s.t.Fatalf("Got wrong type of message expected %d but got %d (%+v)", t, msg.Tag, msg)
	}
}

func (s *neo4jServer) sendFailureMsg(code, msg string) {
	f := map[string]interface{}{
		"code":    code,
		"message": msg,
	}
	s.packer.PackStruct(msgV3Failure, f)
	s.chunker.send()
}

func (s *neo4jServer) waitForHello() {
	msg := s.wait()
	s.assertStructType(msg, msgV3Hello)
	m := msg.Fields[0].(map[string]interface{})
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

func (s *neo4jServer) wait() *testStruct {
	x, err := s.unpacker.UnpackStruct(s.hyd)
	if err != nil {
		s.t.Fatalf("Server couldn't parse message: %s", err)
	}
	return x.(*testStruct)
}

func (s *neo4jServer) waitForRun() {
	msg := s.wait()
	s.assertStructType(msg, msgV3Run)
}

func (s *neo4jServer) waitForTxBegin() {
	msg := s.wait()
	s.assertStructType(msg, msgV3Begin)
}

func (s *neo4jServer) waitForTxCommit() {
	msg := s.wait()
	s.assertStructType(msg, msgV3Commit)
}

func (s *neo4jServer) waitForTxRollback() {
	msg := s.wait()
	s.assertStructType(msg, msgV3Rollback)
}

func (s *neo4jServer) waitForPullAll() {
	msg := s.wait()
	s.assertStructType(msg, msgV3PullAll)
}

func (s *neo4jServer) acceptVersion(ver byte) {
	ver3 := []byte{0x00, 0x00, 0x00, ver}
	_, err := s.conn.Write(ver3)
	if err != nil {
		s.t.Fatalf("Failed to send version")
	}
}

func (s *neo4jServer) send(tag packstream.StructTag, field ...interface{}) {
	s.chunker.add()
	err := s.packer.PackStruct(tag, field...)
	if err != nil {
		s.t.Fatalf("Failed to pack: %s", err)
	}
	s.chunker.send()
}

func (s *neo4jServer) acceptHello() {
	err := s.packer.PackStruct(msgV3Success, map[string]interface{}{
		"connection_id": "cid",
		"server":        "fake/3.5",
	})
	if err != nil {
		s.t.Fatalf("Failed to pack: %s", err)
	}
	err = s.chunker.send()
	if err != nil {
		s.t.Fatalf("Failed to send acceptance of hello: %s", err)
	}
}

// Utility when something else but connect is to be tested
func (s *neo4jServer) accept() {
	s.waitForHandshake()
	s.acceptVersion(3)
	s.waitForHello()
	s.acceptHello()
}

// Utility to wait and serve a auto commit query
func (s *neo4jServer) waitAndServeAutoCommit(stream []testStruct) {
	s.waitForRun()
	s.waitForPullAll()
	for _, x := range stream {
		s.send(x.Tag, x.Fields...)
	}
	//s.acceptRun(keys)
}

func (s *neo4jServer) waitAndServeTransRun(stream []testStruct, commit bool) {
	s.waitForTxBegin()
	// Until lazy tx begin:
	r := makeTxBeginResp()
	s.send(r.Tag, r.Fields...)
	s.waitForRun()
	s.waitForPullAll()
	for _, x := range stream {
		s.send(x.Tag, x.Fields...)
	}
	if commit {
		s.waitForTxCommit()
		r = makeTxCommitResp()
		s.send(r.Tag, r.Fields...)
	} else {
		s.waitForTxRollback()
		r = makeTxRollbackResp()
		s.send(r.Tag, r.Fields...)
	}
}

type cleanSetup func()

func setupBoltPipe(t *testing.T) (net.Conn, *neo4jServer, cleanSetup) {
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
	srv := newNeo4jServer(t, srvConn)

	return clientConn, srv, func() {
		l.Close()
	}
}

type serverStream struct {
	stream []testStruct
}

func makeTestRunResp(keys []interface{}) testStruct {
	return testStruct{
		Tag: msgV3Success,
		Fields: []interface{}{
			map[string]interface{}{
				"fields":  keys,
				"t_first": int64(1),
			},
		},
	}
}

func makeTxBeginResp() testStruct {
	return testStruct{
		Tag: msgV3Success,
		Fields: []interface{}{
			map[string]interface{}{},
		},
	}
}

func makeTxCommitResp() testStruct {
	return testStruct{
		Tag: msgV3Success,
		Fields: []interface{}{
			map[string]interface{}{
				"bookmark": "abookmark",
			},
		},
	}
}

func makeTxRollbackResp() testStruct {
	return testStruct{
		Tag: msgV3Success,
		Fields: []interface{}{
			map[string]interface{}{},
		},
	}
}
func makeTestRec(values []interface{}) testStruct {
	return testStruct{Tag: msgV3Record, Fields: []interface{}{values}}
}

func makeTestSum(bookmark string) testStruct {
	m := map[string]interface{}{
		"bookmark": bookmark,
		"type":     "r",
	}
	return testStruct{Tag: msgV3Success, Fields: []interface{}{m}}
}
