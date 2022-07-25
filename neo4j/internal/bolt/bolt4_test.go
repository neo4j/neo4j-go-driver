/*
 * Copyright (c) "Neo4j"
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
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
)

// bolt4.connect is tested through Connect, no need to test it here
func TestBolt4(ot *testing.T) {
	// Test streams
	// Faked returns from a server
	runKeys := []interface{}{"f1", "f2"}
	runBookmark := "bm"
	runQid := 7
	runResponse := []testStruct{
		{
			tag: msgSuccess,
			fields: []interface{}{
				map[string]interface{}{
					"fields":  runKeys,
					"t_first": int64(1),
					"qid":     int64(runQid),
				},
			},
		},
		{
			tag:    msgRecord,
			fields: []interface{}{[]interface{}{"1v1", "1v2"}},
		},
		{
			tag:    msgRecord,
			fields: []interface{}{[]interface{}{"2v1", "2v2"}},
		},
		{
			tag:    msgRecord,
			fields: []interface{}{[]interface{}{"3v1", "3v2"}},
		},
		{
			tag:    msgSuccess,
			fields: []interface{}{map[string]interface{}{"bookmark": runBookmark, "type": "r"}},
		},
	}

	auth := map[string]interface{}{
		"scheme":      "basic",
		"principal":   "neo4j",
		"credentials": "pass",
	}

	assertBoltState := func(t *testing.T, expected int, bolt *bolt4) {
		t.Helper()
		if expected != bolt.state {
			t.Errorf("Bolt is in unexpected state %d vs %d", expected, bolt.state)
		}
	}

	assertBoltDead := func(t *testing.T, bolt *bolt4) {
		t.Helper()
		if bolt.IsAlive() {
			t.Error("Bolt is alive when it should be dead")
		}
	}

	assertRunResponseOk := func(t *testing.T, bolt *bolt4, stream db.StreamHandle) {
		for i := 1; i < len(runResponse)-1; i++ {
			rec, sum, err := bolt.Next(stream)
			AssertNextOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)
	}

	connectToServer := func(t *testing.T, serverJob func(srv *bolt4server)) (*bolt4, func()) {
		// Connect client+server
		tcpConn, srv, cleanup := setupBolt4Pipe(t)
		go serverJob(srv)

		c, err := Connect("serverName", tcpConn, auth, "007", nil, logger, boltLogger)
		if err != nil {
			t.Fatal(err)
		}

		bolt := c.(*bolt4)
		assertBoltState(t, bolt4_ready, bolt)
		return bolt, cleanup
	}

	// Simple successful connect
	ot.Run("Connect success", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			handshake := srv.waitForHandshake()
			// There should be a version 4 somewhere
			foundV := false
			for i := 0; i < 5; i++ {
				ver := handshake[(i * 4) : (i*4)+4]
				if ver[3] == 4 {
					foundV = true
				}
			}
			if !foundV {
				t.Fatalf("Didn't find version 4 in handshake: %+v", handshake)
			}

			// Accept bolt version 4
			srv.acceptVersion(4, 0)
			srv.waitForHello()
			srv.acceptHello()
		})
		defer cleanup()
		defer bolt.Close()

		// Check Bolt properties
		AssertStringEqual(t, bolt.ServerName(), "serverName")
		AssertTrue(t, bolt.IsAlive())
		AssertTrue(t, reflect.DeepEqual(bolt.in.connReadTimeout, time.Duration(-1)))
		AssertFalse(t, bolt.out.useUtc)
	})

	ot.Run("Connect success with timeout hint", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.waitForHandshake()
			srv.acceptVersion(4, 0)
			srv.waitForHello()
			srv.acceptHelloWithHints(map[string]interface{}{"connection.recv_timeout_seconds": 42})
		})
		defer cleanup()
		defer bolt.Close()

		AssertTrue(t, reflect.DeepEqual(bolt.in.connReadTimeout, 42*time.Second))
	})

	for _, version := range [][]byte{{4, 3}, {4, 4}} {
		major := version[0]
		minor := version[1]
		ot.Run(fmt.Sprintf("[%d.%d] Connect success with UTC patch", major, minor), func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
				srv.waitForHandshake()
				srv.acceptVersion(major, minor)
				srv.waitForHelloWithPatches([]interface{}{"utc"})
				srv.acceptHelloWithPatches([]interface{}{"utc"})
			})
			defer cleanup()
			defer bolt.Close()

			AssertTrue(t, bolt.out.useUtc)
		})

		ot.Run(fmt.Sprintf("[%d.%d] Connect success with unknown patch", major, minor), func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
				srv.waitForHandshake()
				srv.acceptVersion(major, minor)
				srv.waitForHelloWithPatches([]interface{}{"utc"})
				srv.acceptHelloWithPatches([]interface{}{"some-unknown-patch"})
			})
			defer cleanup()
			defer bolt.Close()

			AssertFalse(t, bolt.out.useUtc)
		})
	}

	invalidValues := []interface{}{4.2, "42", -42}
	for _, value := range invalidValues {
		ot.Run(fmt.Sprintf("Connect success with ignored invalid timeout hint %v", value), func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
				srv.waitForHandshake()
				srv.acceptVersion(4, 0)
				srv.waitForHello()
				srv.acceptHelloWithHints(map[string]interface{}{"connection.recv_timeout_seconds": value})
			})
			defer cleanup()
			defer bolt.Close()

			AssertTrue(t, reflect.DeepEqual(bolt.in.connReadTimeout, time.Duration(-1)))
		})
	}

	ot.Run("Routing in hello", func(t *testing.T) {
		routingContext := map[string]string{"some": "thing"}
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(4, 1)
			hmap := srv.waitForHello()
			helloRoutingContext := hmap["routing"].(map[string]interface{})
			if len(helloRoutingContext) != len(routingContext) {
				panic("Routing contexts differ")
			}
			srv.acceptHello()
		}()
		bolt, err := Connect("serverName", conn, auth, "007", routingContext, logger, boltLogger)
		AssertNoError(t, err)
		bolt.Close()
	})

	ot.Run("No routing in hello", func(t *testing.T) {
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(4, 1)
			hmap := srv.waitForHello()
			_, exists := hmap["routing"].(map[string]interface{})
			if exists {
				panic("Should be no routing entry")
			}
			srv.acceptHello()
		}()
		bolt, err := Connect("serverName", conn, auth, "007", nil, logger, boltLogger)
		AssertNoError(t, err)
		bolt.Close()
	})

	ot.Run("No routing in hello on 4.0", func(t *testing.T) {
		routingContext := map[string]string{"some": "thing"}
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(4, 0)
			hmap := srv.waitForHello()
			_, exists := hmap["routing"].(map[string]interface{})
			if exists {
				panic("Should be no routing entry")
			}
			srv.acceptHello()
		}()
		bolt, err := Connect("serverName", conn, auth, "007", routingContext, logger, boltLogger)
		AssertNoError(t, err)
		bolt.Close()
	})

	ot.Run("Failed authentication", func(t *testing.T) {
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()
		defer conn.Close()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(4, 0)
			srv.waitForHello()
			srv.rejectHelloUnauthorized()
		}()
		bolt, err := Connect("serverName", conn, auth, "007", nil, logger, boltLogger)
		AssertNil(t, bolt)
		AssertError(t, err)
		dbErr, isDbErr := err.(*db.Neo4jError)
		if !isDbErr {
			panic(err)
		}
		if !dbErr.IsAuthenticationFailed() {
			t.Errorf("Should be authentication error: %s", dbErr)
		}
	})

	ot.Run("Run auto-commit", func(t *testing.T) {
		cypherText := "MATCH (n)"
		theDb := "thedb"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRun(runResponse, func(fields []interface{}) {
				// fields consists of cypher text, cypher params, meta
				AssertStringEqual(t, fields[0].(string), cypherText)
				meta := fields[2].(map[string]interface{})
				AssertStringEqual(t, meta["db"].(string), theDb)
			})
		})
		defer cleanup()
		defer bolt.Close()

		bolt.SelectDatabase(theDb)
		str, _ := bolt.Run(db.Command{Cypher: cypherText}, db.TxConfig{Mode: db.ReadMode})
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)
		assertBoltState(t, bolt4_streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Run auto-commit with impersonation", func(t *testing.T) {
		cypherText := "MATCH (n)"
		impersonatedUser := "a user"
		theDb := "thedb"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.acceptWithMinor(4, 4)
			// Make sure that impersonation id is sent
			srv.serveRun(runResponse, func(fields []interface{}) {
				// fields consists of cypher text, cypher params, meta
				AssertStringEqual(t, fields[0].(string), cypherText)
				meta := fields[2].(map[string]interface{})
				AssertStringEqual(t, meta["db"].(string), theDb)
				AssertStringEqual(t, meta["imp_user"].(string), impersonatedUser)
			})
		})
		defer cleanup()
		defer bolt.Close()

		bolt.SelectDatabase(theDb)
		str, _ := bolt.Run(db.Command{Cypher: cypherText}, db.TxConfig{Mode: db.ReadMode, ImpersonatedUser: impersonatedUser})
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)
		assertBoltState(t, bolt4_streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Run auto-commit with fetch size 2 of 3", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(2)
			srv.send(runResponse[0].tag, runResponse[0].fields...)
			srv.send(runResponse[1].tag, runResponse[1].fields...)
			srv.send(runResponse[2].tag, runResponse[2].fields...)
			srv.send(msgSuccess, map[string]interface{}{"has_more": true})
			srv.waitForPullN(2)
			srv.send(runResponse[3].tag, runResponse[3].fields...)
			srv.send(runResponse[4].tag, runResponse[4].fields...)
		})
		defer cleanup()
		defer bolt.Close()

		str, _ := bolt.Run(db.Command{Cypher: "cypher", FetchSize: 2}, db.TxConfig{Mode: db.ReadMode})
		assertBoltState(t, bolt4_streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Run transactional commit", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		// Lazy start of transaction when no bookmark
		assertBoltState(t, bolt4_pendingtx, bolt)
		str, err := bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		assertBoltState(t, bolt4_streamingtx, bolt)
		AssertNoError(t, err)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt4_tx, bolt)

		bolt.TxCommit(tx)
		assertBoltState(t, bolt4_ready, bolt)
		AssertStringEqual(t, committedBookmark, bolt.Bookmark())
	})

	// Verifies that current stream is discarded correctly even if it is larger
	// than what is served by a single pull.
	ot.Run("Commit while streaming", func(t *testing.T) {
		qid := int64(2)
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForTxBegin()
			srv.send(msgSuccess, map[string]interface{}{})
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			// Send Pull response
			srv.send(msgSuccess, map[string]interface{}{"fields": []interface{}{"k"}, "t_first": int64(1), "qid": qid})
			// ... and the record
			srv.send(msgRecord, []interface{}{"v1"})
			// ... and the batch summary
			srv.send(msgSuccess, map[string]interface{}{"has_more": true})
			// Wait for the discard message (no need for qid since the last executed query is discarded)
			srv.waitForDiscardN(-1)
			// Respond to discard with has more to indicate that there are more records
			srv.send(msgSuccess, map[string]interface{}{"has_more": true})
			// Wait for the commit
			srv.waitForTxCommit()
			srv.send(msgSuccess, map[string]interface{}{"bookmark": "x"})
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		_, err = bolt.RunTx(tx, db.Command{Cypher: "Whatever", FetchSize: 1})
		AssertNoError(t, err)

		err = bolt.TxCommit(tx)
		AssertNoError(t, err)
		assertBoltState(t, bolt4_ready, bolt)
	})

	// Verifies that current stream is discarded correctly even if it is larger
	// than what is served by a single pull.
	ot.Run("Commit while streams, explicit consume", func(t *testing.T) {
		qid := int64(2)
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForTxBegin()
			srv.send(msgSuccess, map[string]interface{}{})
			// First RunTx
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			// Send Pull response
			srv.send(msgSuccess, map[string]interface{}{"fields": []interface{}{"k"}, "t_first": int64(1), "qid": qid})
			// Driver should discard this stream which is small
			srv.send(msgRecord, []interface{}{"v1"})
			srv.send(msgSuccess, map[string]interface{}{"has_more": false})
			// Second RunTx
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			srv.send(msgSuccess, map[string]interface{}{"fields": []interface{}{"k"}, "t_first": int64(1), "qid": qid})
			// Driver should discard this stream, which is small
			srv.send(msgRecord, []interface{}{"v1"})
			srv.send(msgSuccess, map[string]interface{}{"has_more": false})
			// Wait for the commit
			srv.waitForTxCommit()
			srv.send(msgSuccess, map[string]interface{}{"bookmark": "x"})
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		s, err := bolt.RunTx(tx, db.Command{Cypher: "Whatever", FetchSize: 1})
		AssertNoError(t, err)
		_, err = bolt.Consume(s)
		AssertNoError(t, err)
		s, err = bolt.RunTx(tx, db.Command{Cypher: "Whatever", FetchSize: 1})
		AssertNoError(t, err)
		_, err = bolt.Consume(s)
		AssertNoError(t, err)

		err = bolt.TxCommit(tx)
		AssertNoError(t, err)
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Begin transaction with bookmark success", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode, Bookmarks: []string{"bm1"}})
		AssertNoError(t, err)
		assertBoltState(t, bolt4_tx, bolt)
		bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		assertBoltState(t, bolt4_streamingtx, bolt)
		bolt.TxCommit(tx)
		assertBoltState(t, bolt4_ready, bolt)
		AssertStringEqual(t, committedBookmark, bolt.Bookmark())
	})

	ot.Run("Begin transaction with bookmark failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForTxBegin()
			srv.sendFailureMsg("code", "not synced")
		})
		defer cleanup()
		defer bolt.Close()

		_, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode, Bookmarks: []string{"bm1"}})
		assertBoltState(t, bolt4_failed, bolt)
		AssertError(t, err)
		AssertStringEqual(t, "", bolt.Bookmark())
	})

	ot.Run("Run transactional rollback", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRunTx(runResponse, false, "")
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt4_pendingtx, bolt)
		str, err := bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNoError(t, err)
		assertBoltState(t, bolt4_streamingtx, bolt)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt4_tx, bolt)

		bolt.TxRollback(tx)
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Server close while streaming", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt4_fetchsize)
			// Send response to run and first record as response to pull
			srv.send(msgSuccess, map[string]interface{}{
				"fields":  runKeys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []interface{}{"1v1", "1v2"})
			// Pretty nice towards bolt, a full message is written
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close()

		str, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt4_streaming, bolt)

		// Retrieve the first record
		rec, sum, err := bolt.Next(str)
		AssertNextOnlyRecord(t, rec, sum, err)

		// Next one should fail due to connection closed
		rec, sum, err = bolt.Next(str)
		AssertNextOnlyError(t, rec, sum, err)
		assertBoltDead(t, bolt)
	})

	ot.Run("Server fail on run with reset", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt4_fetchsize)
			srv.sendFailureMsg("code", "msg") // RUN failed
			srv.waitForReset()
			srv.sendIgnoredMsg() // PULL Ignored
			srv.sendSuccess(map[string]interface{}{})
		})
		defer cleanup()
		defer bolt.Close()

		// Fake syntax error that doesn't really matter...
		_, err := bolt.Run(db.Command{Cypher: "MATCH (n RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNeo4jError(t, err)
		assertBoltState(t, bolt4_failed, bolt)

		bolt.Reset()
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Server fail on run continue to commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForTxBegin()
			srv.waitForRun(nil)
			srv.waitForPullN(bolt4_fetchsize)
			srv.sendFailureMsg("code", "msg")
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		_, err = bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNeo4jError(t, err)
		err = bolt.TxCommit(tx)  // This will fail due to above failed
		AssertNeo4jError(t, err) // Should have same error as from run since that is original cause
	})

	ot.Run("Reset while streaming", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt4_fetchsize)
			// Send RUN response and a record
			for i := 0; i < 2; i++ {
				srv.send(runResponse[i].tag, runResponse[i].fields...)
			}
			srv.waitForReset()
			// Acknowledge reset, no fields
			srv.sendSuccess(map[string]interface{}{})
		})
		defer cleanup()
		defer bolt.Close()

		_, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt4_streaming, bolt)

		bolt.Reset()
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Reset in ready state", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRun(runResponse, nil)
		})
		defer cleanup()
		defer bolt.Close()
		s, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		_, err = bolt.Consume(s)
		AssertNoError(t, err)
		// Should be no-op since state already is ready
		bolt.Reset()
	})

	ot.Run("Forces reset in ready state", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForReset()
			srv.sendSuccess(map[string]interface{}{})
		})
		defer cleanup()
		defer bolt.Close()

		err := bolt.ForceReset()
		AssertNoError(t, err)
		assertBoltState(t, bolt4_ready, bolt)
	})

	ot.Run("Forces reset while streaming", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRun(runResponse, nil)
			srv.waitForReset()
			srv.sendSuccess(map[string]interface{}{})
		})
		defer cleanup()
		defer bolt.Close()
		_, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)

		err = bolt.ForceReset()
		AssertNoError(t, err)
		assertBoltState(t, bolt4_ready, bolt)
	})

	// Reset where state is ready

	ot.Run("Buffer stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRun(runResponse, nil)
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher"}, db.TxConfig{Mode: db.ReadMode})
		// This should force all records to be buffered in the stream
		err := bolt.Buffer(stream)
		AssertNoError(t, err)
		// The bookmark should be set
		AssertStringEqual(t, bolt.Bookmark(), runBookmark)

		// Server closed connection and bolt will go into failed state
		_, err = bolt.Run(db.Command{Cypher: "cypher"}, db.TxConfig{Mode: db.ReadMode})
		AssertError(t, err)
		assertBoltState(t, bolt4_dead, bolt)

		// Should still be able to read from the stream even though bolt is dead
		assertRunResponseOk(t, bolt, stream)

		// Buffering again should not affect anything
		err = bolt.Buffer(stream)
		AssertNoError(t, err)
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)
	})

	ot.Run("Buffer stream with fetch size", func(t *testing.T) {
		keys := []interface{}{"k1"}
		bookmark := "x"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(3)
			srv.send(msgSuccess, map[string]interface{}{"fields": keys})
			srv.send(msgRecord, []interface{}{"1"})
			srv.send(msgRecord, []interface{}{"2"})
			srv.send(msgRecord, []interface{}{"3"})
			srv.send(msgSuccess, map[string]interface{}{"has_more": true})
			srv.waitForPullN(-1)
			srv.send(msgRecord, []interface{}{"4"})
			srv.send(msgRecord, []interface{}{"5"})
			srv.send(msgSuccess, map[string]interface{}{"bookmark": bookmark, "type": "r"})
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher", FetchSize: 3}, db.TxConfig{Mode: db.ReadMode})
		// Read one to put it in less comfortable state
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Buffer the rest
		err = bolt.Buffer(stream)
		AssertNoError(t, err)
		// The bookmark should be set
		AssertStringEqual(t, bolt.Bookmark(), bookmark)

		for i := 0; i < 4; i++ {
			rec, sum, err = bolt.Next(stream)
			AssertNextOnlyRecord(t, rec, sum, err)
		}
		rec, sum, err = bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)
		// Buffering again should not affect anything
		err = bolt.Buffer(stream)
		AssertNoError(t, err)
		rec, sum, err = bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)
	})

	ot.Run("Buffer stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt4_fetchsize)
			// Send response to run and first record as response to pull
			srv.send(msgSuccess, map[string]interface{}{
				"fields":  runKeys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []interface{}{"1v1", "1v2"})
			srv.sendFailureMsg("thecode", "themessage")
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher"}, db.TxConfig{Mode: db.ReadMode})
		// This should force all records to be buffered in the stream
		err := bolt.Buffer(stream)
		// Should be no error here since we got one record before the error
		AssertNoError(t, err)
		// Retrieve the one record we got
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Now we should see the error, this is to handle errors happening on a specifiec
		// record, like division by zero.
		rec, sum, err = bolt.Next(stream)
		AssertNextOnlyError(t, rec, sum, err)
		// Should be no bookmark since we failed
		AssertStringEqual(t, bolt.Bookmark(), "")
	})

	ot.Run("Buffer stream with invalid handle", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
		})
		defer cleanup()
		defer bolt.Close()

		err := bolt.Buffer(db.StreamHandle(1))
		AssertError(t, err)
	})

	ot.Run("Consume stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.serveRun(runResponse, nil)
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher"}, db.TxConfig{Mode: db.ReadMode})
		// This should force all records to be buffered in the stream
		sum, err := bolt.Consume(stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
		assertBoltState(t, bolt4_ready, bolt)
		// The bookmark should be set
		AssertStringEqual(t, bolt.Bookmark(), runBookmark)
		AssertStringEqual(t, sum.Bookmark, runBookmark)

		// Should only get the summary from the stream since we consumed everything
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)

		// Consuming again should just return the summary again
		sum, err = bolt.Consume(stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
	})

	ot.Run("Consume stream with fetch size", func(t *testing.T) {
		qid := 3
		keys := []interface{}{"k1"}
		bookmark := "x"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(3)
			srv.send(msgSuccess, map[string]interface{}{"fields": keys, "qid": int64(qid)})
			srv.send(msgRecord, []interface{}{"1"})
			srv.send(msgRecord, []interface{}{"2"})
			srv.send(msgRecord, []interface{}{"3"})
			srv.send(msgSuccess, map[string]interface{}{"has_more": true, "qid": int64(qid)})
			srv.waitForDiscardN(-1)
			srv.send(msgSuccess, map[string]interface{}{"bookmark": bookmark, "type": "r"})
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher", FetchSize: 3}, db.TxConfig{Mode: db.ReadMode})
		// Read one to put it in less comfortable state
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Consume the rest
		sum, err = bolt.Consume(stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
		assertBoltState(t, bolt4_ready, bolt)

		// The bookmark should be set
		AssertStringEqual(t, bolt.Bookmark(), bookmark)
		AssertStringEqual(t, sum.Bookmark, bookmark)

		// Should only get the summary from the stream since we consumed everything
		rec, sum, err = bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)

		// Consuming again should just return the summary again
		sum, err = bolt.Consume(stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
	})

	ot.Run("Consume stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt4_fetchsize)
			// Send response to run and first record as response to pull
			srv.send(msgSuccess, map[string]interface{}{
				"fields":  runKeys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []interface{}{"1v1", "1v2"})
			srv.sendFailureMsg("thecode", "themessage")
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher"}, db.TxConfig{Mode: db.ReadMode})
		// This should force all records to be buffered in the stream
		sum, err := bolt.Consume(stream)
		AssertNeo4jError(t, err)
		AssertNil(t, sum)
		AssertStringEqual(t, bolt.Bookmark(), "")

		// Should not get the summary since there was an error
		rec, sum, err := bolt.Next(stream)
		AssertNeo4jError(t, err)
		AssertNextOnlyError(t, rec, sum, err)
	})

	ot.Run("Consume with invalid stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
		})
		defer cleanup()
		defer bolt.Close()

		sum, err := bolt.Consume(db.StreamHandle(1))
		AssertNil(t, sum)
		AssertError(t, err)
	})

	ot.Run("GetRoutingTable using ROUTE message on 4.3", func(t *testing.T) {
		theDb := "theDb"
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.acceptWithMinor(4, 3)
			srv.waitForRoute(func(fields []interface{}) {
				// Fields contains context(map), bookmarks ([]string), database name(string or nil)

				// No database selected, should be nil
				AssertStringEqual(t, fields[2].(string), theDb)
			})
			srv.sendSuccess(map[string]interface{}{
				"rt": map[string]interface{}{
					"ttl": 1000,
					"servers": []interface{}{
						map[string]interface{}{
							"role":      "ROUTE",
							"addresses": []interface{}{"router1"},
						},
					},
				},
			})
		})
		defer cleanup()
		defer bolt.Close()

		rt, err := bolt.GetRoutingTable(map[string]string{"region": "space"}, nil, theDb, "")
		AssertNoError(t, err)
		ert := &db.RoutingTable{Routers: []string{"router1"}, TimeToLive: 1000, DatabaseName: theDb}
		if !reflect.DeepEqual(rt, ert) {
			t.Fatalf("Expected:\n%+v\n != Actual: \n%+v\n", rt, ert)
		}
	})

	ot.Run("GetRoutingTable using ROUTE message", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.acceptWithMinor(4, 4)
			srv.waitForRoute(func(fields []interface{}) {
				// Fields contains context(map), bookmarks([]string), extras(map)
			})
			srv.sendSuccess(map[string]interface{}{
				"rt": map[string]interface{}{
					"ttl": 1000,
					"db":  "thedb",
					"servers": []interface{}{
						map[string]interface{}{
							"role":      "ROUTE",
							"addresses": []interface{}{"router1"},
						},
					},
				},
			})
		})
		defer cleanup()
		defer bolt.Close()

		rt, err := bolt.GetRoutingTable(map[string]string{"region": "space"}, nil, "thedb", "")
		AssertNoError(t, err)
		ert := &db.RoutingTable{Routers: []string{"router1"}, TimeToLive: 1000, DatabaseName: "thedb"}
		if !reflect.DeepEqual(rt, ert) {
			t.Fatalf("Expected:\n%+v\n != Actual: \n%+v\n", rt, ert)
		}
	})

	ot.Run("Expired authentication error should close connection", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.sendFailureMsg("Status.Security.AuthorizationExpired", "auth token is... expired")
		})
		defer cleanup()

		_, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		assertBoltState(t, bolt4_dead, bolt)
		AssertError(t, err)
	})

	ot.Run("Immediately expired authentication token error triggers a connection failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.sendFailureMsg("Neo.ClientError.Security.TokenExpired", "SSO token is... expired")
		})
		defer cleanup()

		_, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		assertBoltState(t, bolt4_failed, bolt)
		AssertError(t, err)
	})

	ot.Run("Expired authentication token error after run triggers a connection failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt4server) {
			srv.accept(4)
			srv.waitForRun(nil)
			srv.sendFailureMsg("Neo.ClientError.Security.TokenExpired", "SSO token is... expired")
		})
		defer cleanup()

		_, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		assertBoltState(t, bolt4_failed, bolt)
		AssertError(t, err)
	})
}
