/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
)

// Fake of bolt6 server.
// Utility to test bolt6 protocol implementation.
// Use panic upon errors, simplifies output when server is running within a go thread
// in the test.
type bolt6server struct {
	conn     net.Conn
	unpacker *packstream.Unpacker
	out      *outgoing
}

func newBolt6Server(conn net.Conn) *bolt6server {
	return &bolt6server{
		unpacker: &packstream.Unpacker{},
		conn:     conn,
		out: &outgoing{
			chunker: newChunker(),
			packer:  packstream.Packer{},
			onPackErr: func(err error) {
				panic(err)
			},
			onIoErr: func(_ context.Context, err error) {
				panic(err)
			},
		},
	}
}

func (s *bolt6server) waitForHandshake() []byte {
	handshake := make([]byte, 4*5)
	_, err := io.ReadFull(s.conn, handshake)
	if err != nil {
		panic(err)
	}
	return handshake
}

func (s *bolt6server) assertStructType(msg *testStruct, t byte) {
	if msg.tag != t {
		panic(fmt.Sprintf("Got wrong type of message expected %d but got %d (%+v)", t, msg.tag, msg))
	}
}

func (s *bolt6server) sendFailureMsg(code, msg string) {
	f := map[string]any{
		"code":    code,
		"message": msg,
	}
	s.send(msgFailure, f)
}

func (s *bolt6server) sendIgnoredMsg() {
	s.send(msgIgnored)
}

// Returns the first hello field
func (s *bolt6server) waitForHelloWithoutAuthToken() map[string]any {
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
func (s *bolt6server) waitForLogon() map[string]any {
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

func (s *bolt6server) receiveMsg() *testStruct {
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

func (s *bolt6server) waitForRun(assertFields func(fields []any)) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRun)
	if assertFields != nil {
		assertFields(msg.fields)
	}
}

func (s *bolt6server) waitForReset() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgReset)
}

func (s *bolt6server) waitForTxBegin(assertFields func(fields []any)) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgBegin)
	if assertFields != nil {
		assertFields(msg.fields)
	}
}

func (s *bolt6server) waitForTxCommit() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgCommit)
}

func (s *bolt6server) waitForTxRollback() {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRollback)
}

func (s *bolt6server) waitForPullN(n int) {
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

func (s *bolt6server) waitForDiscardN(n int) {
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

func (s *bolt6server) waitForRoute(assertRoute func(fields []any)) {
	msg := s.receiveMsg()
	s.assertStructType(msg, msgRoute)
	if assertRoute != nil {
		assertRoute(msg.fields)
	}
}

// acceptManifestVersion responds with manifest marker to trigger manifest negotiation
func (s *bolt6server) acceptManifestVersion() {
	manifestMarker := []byte{0x00, 0x00, 0x01, 0xFF}
	_, err := s.conn.Write(manifestMarker)
	if err != nil {
		panic(err)
	}
}

// sendManifestOfferings sends protocol offerings for manifest negotiation
func (s *bolt6server) sendManifestOfferings(offerings []protocolVersion) {
	// Send count of offerings
	count := len(offerings)
	var countBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(countBuf[:], uint64(count))
	_, err := s.conn.Write(countBuf[:n])
	if err != nil {
		panic(err)
	}

	// Send each offering
	for _, offering := range offerings {
		versionBytes := []byte{0x00, offering.back, offering.minor, offering.major}
		_, err := s.conn.Write(versionBytes)
		if err != nil {
			panic(err)
		}
	}

	// Send capability mask (0 for now)
	var capBuf [binary.MaxVarintLen64]byte
	n = binary.PutUvarint(capBuf[:], 0)
	_, err = s.conn.Write(capBuf[:n])
	if err != nil {
		panic(err)
	}
}

// waitForManifestConfirmation waits for the client's handshake confirmation
func (s *bolt6server) waitForManifestConfirmation() (byte, byte) {
	// Read chosen version (4 bytes)
	versionBytes := make([]byte, 4)
	_, err := io.ReadFull(s.conn, versionBytes)
	if err != nil {
		panic(err)
	}

	// Read capability mask (varint)
	capBytes := make([]byte, 1)
	_, err = io.ReadFull(s.conn, capBytes)
	if err != nil {
		panic(err)
	}

	return versionBytes[3], versionBytes[2] // major, minor
}

func (s *bolt6server) closeConnection() {
	_ = s.conn.Close()
}

func (s *bolt6server) send(tag byte, field ...any) {
	s.out.appendX(tag, field...)
	s.out.send(context.Background(), s.conn)
}

func (s *bolt6server) sendSuccess(m map[string]any) {
	s.send(msgSuccess, m)
}

func (s *bolt6server) acceptHello() {
	s.send(msgSuccess, map[string]any{
		"connection_id": "cid",
		"server":        "fake/4.5",
	})
}
func (s *bolt6server) acceptLogon() {
	s.sendSuccess(nil)
}

func (s *bolt6server) acceptHelloWithHints(hints map[string]any) {
	s.send(msgSuccess, map[string]any{
		"connection_id": "cid",
		"server":        "fake/4.5",
		"hints":         hints,
	})
}

// Utility to wait and serve an auto commit query
func (s *bolt6server) serveRun(stream []testStruct, assertRun func([]any)) {
	s.waitForRun(assertRun)
	s.waitForPullN(bolt6FetchSize)
	for _, x := range stream {
		s.send(x.tag, x.fields...)
	}
}

func (s *bolt6server) serveRunTx(stream []testStruct, commit bool, bookmark string) {
	s.waitForTxBegin(nil)
	s.send(msgSuccess, map[string]any{})
	s.waitForRun(nil)
	s.waitForPullN(bolt6FetchSize)
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

func (s *bolt6server) rejectLogonWithoutAuthToken() {
	s.send(msgFailure, map[string]any{
		"code":    "Neo.ClientError.Security.Unauthorized",
		"message": "",
	})
}

func setupBolt6Pipe(t *testing.T) (net.Conn, *bolt6server, func()) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}

	addr := l.Addr()
	fmt.Println("Test server listening on", addr.String())
	clientConn, _ := net.Dial(addr.Network(), addr.String())

	srvConn, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept error: %s", err)
	}
	srv := newBolt6Server(srvConn)

	return clientConn, srv, func() {
		_ = l.Close()
	}
}

// acceptBolt6WithManifest handles the complete Bolt 6 manifest negotiation setup
func (s *bolt6server) acceptBolt6WithManifest() {
	s.acceptBolt6ManifestOnly()
	// For Bolt 6, we expect a hello message after manifest negotiation
	s.waitForHelloWithoutAuthToken()
	s.acceptHello()
	s.waitForLogon()
	s.acceptLogon()
}

// acceptBolt6WithManifestAndHints handles the complete Bolt 6 manifest negotiation setup with timeout hints
func (s *bolt6server) acceptBolt6WithManifestAndHints(hints map[string]any) {
	s.acceptBolt6ManifestOnly()
	// For Bolt 6, we expect a hello message after manifest negotiation
	s.waitForHelloWithoutAuthToken()
	s.acceptHelloWithHints(hints)
	s.waitForLogon()
	s.acceptLogon()
}

// acceptBolt6ManifestOnly handles only the manifest negotiation without hello/logon
func (s *bolt6server) acceptBolt6ManifestOnly() {
	s.waitForHandshake()
	s.acceptManifestVersion()
	// Send protocol offerings including Bolt 6
	offerings := []protocolVersion{
		{major: 6, minor: 0, back: 0},
		{major: 5, minor: 8, back: 8},
		{major: 4, minor: 4, back: 2},
	}
	s.sendManifestOfferings(offerings)
	// Wait for client's choice
	major, minor := s.waitForManifestConfirmation()
	if major != 6 || minor != 0 {
		panic(fmt.Sprintf("Expected client to choose Bolt 6.0, but got %d.%d", major, minor))
	}
}
