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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
)

func TestBolt3(ot *testing.T) {
	// Test streams
	// Faked returns from a server
	runKeys := []interface{}{"f1", "f2"}
	runBookmark := "bm"
	runResponse := []testStruct{
		{
			tag: msgSuccess,
			fields: []interface{}{
				map[string]interface{}{
					"fields":  runKeys,
					"t_first": int64(1),
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

	assertBoltState := func(t *testing.T, expected int, bolt *bolt3) {
		t.Helper()
		if expected != bolt.state {
			t.Errorf("Bolt is in unexpected state %d vs %d", expected, bolt.state)
		}
	}

	assertBoltDead := func(t *testing.T, bolt *bolt3) {
		t.Helper()
		if bolt.IsAlive() {
			t.Error("Bolt is alive when it should be dead")
		}
	}

	assertRunResponseOk := func(t *testing.T, bolt *bolt3, stream db.StreamHandle) {
		for i := 1; i < len(runResponse)-1; i++ {
			rec, sum, err := bolt.Next(stream)
			AssertNextOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)
	}

	connectToServer := func(t *testing.T, serverJob func(srv *bolt3server)) (*bolt3, func()) {
		// Connect client+server
		tcpConn, srv, cleanup := setupBolt3Pipe(t)
		go serverJob(srv)

		c, err := Connect("serverName", tcpConn, auth, "007", nil, logger)
		if err != nil {
			t.Fatal(err)
		}

		bolt := c.(*bolt3)
		assertBoltState(t, bolt3_ready, bolt)
		return bolt, cleanup
	}

	// Simple succesful connect
	ot.Run("Connect success", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			handshake := srv.waitForHandshake()
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
			srv.acceptVersion(3)
			srv.waitForHello()
			srv.acceptHello()
		})
		defer cleanup()
		defer bolt.Close()

		// Check Bolt properties
		AssertStringEqual(t, bolt.ServerName(), "serverName")
		AssertTrue(t, bolt.IsAlive())
	})

	ot.Run("Failed authentication", func(t *testing.T) {
		conn, srv, cleanup := setupBolt3Pipe(t)
		defer cleanup()
		defer conn.Close()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(3)
			srv.waitForHello()
			srv.rejectHelloUnauthorized()
		}()
		bolt, err := Connect("serverName", conn, auth, "007", nil, logger)
		AssertNil(t, bolt)
		AssertError(t, err)
		dbErr := err.(*db.Neo4jError)
		if !dbErr.IsAuthenticationFailed() {
			t.Errorf("Should be authentication error: %s", dbErr)
		}
	})

	ot.Run("Run auto-commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
		})
		defer cleanup()
		defer bolt.Close()

		str, _ := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)
		assertBoltState(t, bolt3_streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Run transactional commit", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		// Lazy start of transaction when no bookmark
		assertBoltState(t, bolt3_pendingtx, bolt)
		str, err := bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		assertBoltState(t, bolt3_streamingtx, bolt)
		AssertNoError(t, err)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt3_tx, bolt)

		bolt.TxCommit(tx)
		assertBoltState(t, bolt3_ready, bolt)
		AssertStringEqual(t, committedBookmark, bolt.Bookmark())
	})

	ot.Run("Begin transaction with bookmark success", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode, Bookmarks: []string{"bm1"}})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_tx, bolt)
		bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		assertBoltState(t, bolt3_streamingtx, bolt)
		bolt.TxCommit(tx)
		assertBoltState(t, bolt3_ready, bolt)
		AssertStringEqual(t, committedBookmark, bolt.Bookmark())
	})

	ot.Run("Begin transaction with bookmark failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForTxBegin()
			srv.sendFailureMsg("code", "not synced")
		})
		defer cleanup()
		defer bolt.Close()

		_, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode, Bookmarks: []string{"bm1"}})
		assertBoltState(t, bolt3_failed, bolt)
		AssertError(t, err)
		AssertStringEqual(t, "", bolt.Bookmark())
	})

	ot.Run("Run transactional rollback", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(runResponse, false, "")
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_pendingtx, bolt)
		str, err := bolt.RunTx(tx, db.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_streamingtx, bolt)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt3_tx, bolt)

		bolt.TxRollback(tx)
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Server close while streaming", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
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
		assertBoltState(t, bolt3_streaming, bolt)

		// Retrieve the first record
		rec, sum, err := bolt.Next(str)
		AssertNextOnlyRecord(t, rec, sum, err)

		// Next one should fail due to connection closed
		rec, sum, err = bolt.Next(str)
		AssertNextOnlyError(t, rec, sum, err)
		assertBoltDead(t, bolt)
	})

	ot.Run("Server fail on run with reset", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
			srv.sendFailureMsg("code", "msg")
			srv.waitForReset()
			srv.sendIgnoredMsg()
			srv.sendIgnoredMsg()
			srv.sendSuccess(map[string]interface{}{})
		})
		defer cleanup()
		defer bolt.Close()

		// Fake syntax error that doesn't really matter...
		_, err := bolt.Run(db.Command{Cypher: "MATCH (n RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNeo4jError(t, err)
		assertBoltState(t, bolt3_failed, bolt)

		bolt.Reset()
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Server fail on run continue to commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForTxBegin()
			srv.waitForRun()
			srv.waitForPullAll()
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

	ot.Run("Reset while streaming ", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
			// Reset message is never sent to server since stream is consumed
		})
		defer cleanup()
		defer bolt.Close()

		_, err := bolt.Run(db.Command{Cypher: "MATCH (n) RETURN n"}, db.TxConfig{Mode: db.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_streaming, bolt)

		bolt.Reset()
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Buffer stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
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
		assertBoltState(t, bolt3_dead, bolt)

		// Should still be able to read from the stream even though bolt is dead
		assertRunResponseOk(t, bolt, stream)

		// Buffering again should not affect anything
		err = bolt.Buffer(stream)
		AssertNoError(t, err)
		rec, sum, err := bolt.Next(stream)
		AssertNextOnlySummary(t, rec, sum, err)
	})

	ot.Run("Buffer stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
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
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
		})
		defer cleanup()
		defer bolt.Close()

		err := bolt.Buffer(db.StreamHandle(1))
		AssertError(t, err)
	})

	ot.Run("Consume stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close()

		stream, _ := bolt.Run(db.Command{Cypher: "cypher"}, db.TxConfig{Mode: db.ReadMode})
		// This should force all records to be buffered in the stream
		sum, err := bolt.Consume(stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
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

	ot.Run("Consume stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
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
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
		})
		defer cleanup()
		defer bolt.Close()

		sum, err := bolt.Consume(db.StreamHandle(1))
		AssertNil(t, sum)
		AssertError(t, err)
	})
}
