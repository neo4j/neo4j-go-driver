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

	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
)

// bolt3.connect is tested through Connect, no need to test it here
func TestBolt3(ot *testing.T) {
	// Test streams
	// Faked returns from a server
	keys := []interface{}{"f1", "f2"}
	recsStrm := []packstream.Struct{
		packstream.Struct{
			Tag: msgSuccess,
			Fields: []interface{}{
				map[string]interface{}{
					"fields":  keys,
					"t_first": int64(1),
				},
			},
		},
		packstream.Struct{
			Tag:    msgRecord,
			Fields: []interface{}{[]interface{}{"1v1", "1v2"}},
		},
		packstream.Struct{
			Tag:    msgRecord,
			Fields: []interface{}{[]interface{}{"2v1", "2v2"}},
		},
		packstream.Struct{
			Tag:    msgRecord,
			Fields: []interface{}{[]interface{}{"3v1", "3v2"}},
		},
		packstream.Struct{
			Tag:    msgSuccess,
			Fields: []interface{}{map[string]interface{}{"bookmark": "bm", "type": "r"}},
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

	connectToServer := func(t *testing.T, serverJob func(srv *bolt3server)) (*bolt3, func()) {
		// Connect client+server
		tcpConn, srv, cleanup := setupBolt3Pipe(t)
		go serverJob(srv)

		c, err := Connect("name", tcpConn, auth, logger)
		if err != nil {
			t.Fatal(err)
		}

		bolt := c.(*bolt3)
		assertBoltState(t, bolt3_ready, bolt)
		return bolt, cleanup
	}

	ot.Run("Run auto-commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(recsStrm)
		})
		defer cleanup()
		defer bolt.Close()

		str, _ := bolt.Run("MATCH (n) RETURN n", nil, connection.ReadMode, nil, 0, nil)
		assertKeys(t, keys, str)
		assertBoltState(t, bolt3_streaming, bolt)

		// Retrieve the records
		for i := 1; i < len(recsStrm)-1; i++ {
			rec, sum, err := bolt.Next(str.Handle)
			assertOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlySummary(t, rec, sum, err)
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Run transactional commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(recsStrm, true)
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(connection.ReadMode, nil, 0, nil)
		assertNoError(t, err)
		assertBoltState(t, bolt3_pendingtx, bolt)
		str, err := bolt.RunTx(tx, "MATCH (n) RETURN n", nil)
		assertBoltState(t, bolt3_streamingtx, bolt)
		assertNoError(t, err)
		assertKeys(t, keys, str)

		// Retrieve the records
		for i := 1; i < len(recsStrm)-1; i++ {
			rec, sum, err := bolt.Next(str.Handle)
			assertOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlySummary(t, rec, sum, err)
		assertBoltState(t, bolt3_tx, bolt)

		bolt.TxCommit(tx)
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Run transactional rollback", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(recsStrm, false)
		})
		defer cleanup()
		defer bolt.Close()

		tx, err := bolt.TxBegin(connection.ReadMode, nil, 0, nil)
		assertNoError(t, err)
		assertBoltState(t, bolt3_pendingtx, bolt)
		str, err := bolt.RunTx(tx, "MATCH (n) RETURN n", nil)
		assertNoError(t, err)
		assertBoltState(t, bolt3_streamingtx, bolt)
		assertKeys(t, keys, str)

		// Retrieve the records
		for i := 1; i < len(recsStrm)-1; i++ {
			rec, sum, err := bolt.Next(str.Handle)
			assertOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlySummary(t, rec, sum, err)
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
				"fields":  keys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []interface{}{"1v1", "1v2"})
			// Pretty nice towards bolt, a full message is written
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close()

		str, err := bolt.Run("MATCH (n) RETURN n", nil, connection.ReadMode, nil, 0, nil)
		assertNoError(t, err)
		assertBoltState(t, bolt3_streaming, bolt)

		// Retrieve the first record
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlyRecord(t, rec, sum, err)

		// Next one should fail due to connection closed
		rec, sum, err = bolt.Next(str.Handle)
		assertOnlyError(t, rec, sum, err)
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
		_, err := bolt.Run("MATCH (n RETURN n", nil, connection.ReadMode, nil, 0, nil)
		assertDatabaseError(t, err)
		assertBoltState(t, bolt3_failed, bolt)

		bolt.Reset()
		assertBoltState(t, bolt3_ready, bolt)
	})

	ot.Run("Reset while streaming ", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(recsStrm)
			// Reset message is never sent to server since stream is consumed
		})
		defer cleanup()
		defer bolt.Close()

		_, err := bolt.Run("MATCH (n) RETURN n", nil, connection.ReadMode, nil, 0, nil)
		assertNoError(t, err)
		assertBoltState(t, bolt3_streaming, bolt)

		bolt.Reset()
		assertBoltState(t, bolt3_ready, bolt)
	})
}
