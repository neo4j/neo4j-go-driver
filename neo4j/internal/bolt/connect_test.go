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
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/neo4j/neo4j-go-driver/neo4j/api"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
)

type testHydrator struct {
}

type testHydrated struct {
	tag    packstream.StructTag
	fields []interface{}
}

func (r *testHydrated) HydrateField(field interface{}) error {
	r.fields = append(r.fields, field)
	return nil
}

func (r *testHydrated) HydrationComplete() error {
	return nil
}

func (h *testHydrator) Hydrator(tag packstream.StructTag, numFields int) (packstream.Hydrator, error) {
	return &testHydrated{tag: tag}, nil
}

func TestConnectBolt3(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	// TODO: Test version handshake failure, connection should close
	// TODO: Test authentication failure, connection should close (bolt3 test?)
	// TODO: Test connect timeout

	// Use a real TCP connection. Alternative is to use net.Pipe but that works a bit different
	// in regards to buffering, chunking causes some problems with pipe since number of reads
	// doesn''t correspond to number of writes.
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}

	// Simulate server with a succesful connect
	go func() {
		conn, err := l.Accept()
		if err != nil {
			t.Fatalf("Accept error: %s", err)
		}

		// Wait for initial handshake
		handshake := make([]byte, 4*5)
		_, err = io.ReadFull(conn, handshake)
		if err != nil {
			t.Fatalf("Server got a read error: %s", err)
		}
		// There should be a version 3 somewhere
		foundV3 := false
		for i := 0; i < 5; i++ {
			ver := handshake[(i * 4) : (i*4)+4]
			if ver[3] == 3 {
				foundV3 = true
			}
		}
		if !foundV3 {
			t.Fatalf("Didn't find version 3 in handshake: %+v", handshake)
		}

		// Accept bolt version 3
		ver3 := []byte{0x00, 0x00, 0x00, 0x03}
		_, err = conn.Write(ver3)
		if err != nil {
			t.Fatalf("Failed to send version")
		}

		// Need unpacker, hydrator and a dechunker now
		dechunker := newDechunker(conn)
		unpacker := packstream.NewUnpacker(dechunker)
		hyd := &testHydrator{}

		// Wait for hello
		x, err := unpacker.UnpackStruct(hyd)
		if err != nil {
			t.Fatalf("Server couldn't parse hello")
		}
		hello := x.(*testHydrated)
		m := hello.fields[0].(map[string]interface{})
		// Hello should contain some musts
		_, exists := m["scheme"]
		if !exists {
			t.Errorf("Missing scheme")
		}
		_, exists = m["user_agent"]
		if !exists {
			t.Errorf("Missing user_agent")
		}

		// Need packer and a chunker now
		chunker := newChunker(conn, 4096)
		packer := packstream.NewPacker(chunker)

		// Accept
		packer.PackStruct(msgV3Success, map[string]interface{}{
			"connection_id": "cid",
			"server":        "fake/3.5",
		})
		chunker.send()

		wg.Done()
	}()

	// Connect to server
	addr := l.Addr()
	conn, err := net.Dial(addr.Network(), addr.String())

	// Pass the connection to bolt connect
	boltconn, err := Connect(conn)
	if err != nil {
		t.Fatalf("Failed to connect bolt: %s", err)
	}

	boltconn.Close()

	wg.Wait()
}

func dumpResult(res api.Result) error {
	for res.Next() {
		rec := res.Record()
		keys := rec.Keys()
		fmt.Println("RECORD")
		for i, x := range rec.Values() {
			fmt.Printf("    %s: %+v\n", keys[i], x)
		}
	}
	err := res.Err()
	if err != nil {
		return err
	}

	sum, err := res.Summary()
	if err != nil {
		return err
	}
	fmt.Printf("Server version: %s\n", sum.Server().Version())

	stmnt := sum.Statement()
	fmt.Printf("Cypher: %s\nParams: %+v\n", stmnt.Text(), stmnt.Params())

	return nil
}

func TestOnRealServer(t *testing.T) {
	t.SkipNow()

	tcpConn, err := connection.OpenTcp()
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Pass ownership of TCP connection to bolt upon success
	boltConn, err := Connect(tcpConn)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err)
	}

	// Run query
	fmt.Println("Match")
	res, err := boltConn.RunAutoCommit("MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	// Run create statement
	fmt.Println("Create")
	params := map[string]interface{}{"rnd": rand.Int()}
	res, err = boltConn.RunAutoCommit("CREATE (n:Random {val: $rnd})", params)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	err = boltConn.Close()
	if err != nil {
		t.Errorf("Failed to close: %s", err)
	}
	if boltConn.IsAlive() {
		t.Errorf("Connection should be dead")
	}

	//t.Fail()
}
