package bolt

import (
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
)

type testHydrator struct {
}

type testStruct struct {
	tag    packstream.StructTag
	fields []interface{}
}

func (r *testStruct) HydrateField(field interface{}) error {
	r.fields = append(r.fields, field)
	return nil
}

func (r *testStruct) HydrationComplete() error {
	return nil
}

func (h *testHydrator) Hydrator(tag packstream.StructTag, numFields int) (packstream.Hydrator, error) {
	return &testStruct{tag: tag}, nil
}

type neo4jServer struct {
	t         *testing.T
	conn      net.Conn
	hyd       *testHydrator
	dechunker *dechunker
	unpacker  *packstream.Unpacker
	chunker   *chunker
	packer    *packstream.Packer
}

func newNeo4jServer(t *testing.T, conn net.Conn) *neo4jServer {
	dechunker := newDechunker(conn)
	unpacker := packstream.NewUnpacker(dechunker)
	chunker := newChunker(conn, 4096)
	packer := packstream.NewPacker(chunker)
	return &neo4jServer{
		t:         t,
		conn:      conn,
		dechunker: dechunker,
		hyd:       &testHydrator{},
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
	if msg.tag != t {
		s.t.Fatalf("Got wrong type of message expected %d but got %d (%+v)", t, msg.tag, msg)
	}
}

func (s *neo4jServer) waitForHello() {
	msg := s.wait()
	s.assertStructType(msg, msgV3Hello)
	m := msg.fields[0].(map[string]interface{})
	// Hello should contain some musts
	_, exists := m["scheme"]
	if !exists {
		s.t.Fatalf("Missing scheme")
	}
	_, exists = m["user_agent"]
	if !exists {
		s.t.Fatalf("Missing user_agent")
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
	fmt.Printf("Sending %+v", tag)
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
func (s *neo4jServer) connect() {
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
		s.send(x.tag, x.fields...)
	}
	//s.acceptRun(keys)
}

func (s *neo4jServer) waitAndServeTransRun(stream []testStruct, commit bool) {
	s.waitForTxBegin()
	// Until lazy tx begin:
	r := makeTxBeginResp()
	s.send(r.tag, r.fields...)
	s.waitForRun()
	s.waitForPullAll()
	for _, x := range stream {
		s.send(x.tag, x.fields...)
	}
	if commit {
		s.waitForTxCommit()
		r = makeTxCommitResp()
		s.send(r.tag, r.fields...)
	} else {
		s.waitForTxRollback()
		r = makeTxRollbackResp()
		s.send(r.tag, r.fields...)
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
		tag: msgV3Success,
		fields: []interface{}{
			map[string]interface{}{
				"fields": keys,
			},
		},
	}
}

func makeTxBeginResp() testStruct {
	return testStruct{
		tag: msgV3Success,
		fields: []interface{}{
			map[string]interface{}{},
		},
	}
}

func makeTxCommitResp() testStruct {
	return testStruct{
		tag: msgV3Success,
		fields: []interface{}{
			map[string]interface{}{
				"bookmark": "abookmark",
			},
		},
	}
}

func makeTxRollbackResp() testStruct {
	return testStruct{
		tag: msgV3Success,
		fields: []interface{}{
			map[string]interface{}{},
		},
	}
}
func makeTestRec(values []interface{}) testStruct {
	return testStruct{tag: msgV3Record, fields: []interface{}{values}}
}

func makeTestSum(bookmark string) testStruct {
	m := map[string]interface{}{
		"bookmark": bookmark,
	}
	return testStruct{tag: msgV3Success, fields: []interface{}{m}}
}
