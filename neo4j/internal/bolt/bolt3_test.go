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

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

// bolt3.connect is tested through Connect, no need to test it here

// TODO:
//       syntax error, RUN fails with client error
//       locking error, RUN fails with retryable error
//       transaction open
//       not alive
//       unconsumed result
//       no response
//       unexpected response
// TODO: test IsAlive
//       different error condtions that can make it not be alive
// TODO: test Close
//       unconsumed result
//       already closed
//       goodbye failure

func TestBolt3(ot *testing.T) {
	assertOnlyRecord := func(t *testing.T, rec *conn.Record, sum *conn.Summary, err error) {
		t.Helper()
		if rec == nil {
			t.Errorf("Expected record")
		}
		if sum != nil {
			t.Errorf("Didn't expect summary")
		}
		if err != nil {
			t.Errorf("Didn't expect error")
		}
	}

	assertOnlySummary := func(t *testing.T, rec *conn.Record, sum *conn.Summary, err error) {
		t.Helper()
		if rec != nil {
			t.Errorf("Didn't expect record")
		}
		if sum == nil {
			t.Errorf("Expected summary")
		}
		if err != nil {
			t.Errorf("Didn't expect error")
		}
	}

	assertKeys := func(t *testing.T, ekeys []interface{}, s *conn.Stream) {
		t.Helper()
		if s == nil {
			t.Fatal("No stream")
		}
		for i, k := range s.Keys {
			if k != ekeys[i] {
				t.Errorf("Stream keys differ")
			}
		}
	}

	// Test streams
	keys := []interface{}{"f1", "f2"}
	// Happy path non transactional stream
	strm1 := []testStruct{
		makeTestRunResp(keys),
		makeTestRec([]interface{}{"1v1", "1v2"}),
		makeTestRec([]interface{}{"2v1", "2v2"}),
		makeTestRec([]interface{}{"3v1", "3v2"}),
		makeTestSum("bm"),
	}
	// Happy path transactional stream
	strm2 := []testStruct{
		makeTestRunResp(keys),
		makeTestRec([]interface{}{"1v1", "1v2"}),
		makeTestRec([]interface{}{"2v1", "2v2"}),
		makeTestRec([]interface{}{"3v1", "3v2"}),
		makeTestSum("bm"),
	}

	auth := map[string]interface{}{
		"scheme":      "basic",
		"principal":   "neo4j",
		"credentials": "pass",
	}

	ot.Run("Run auto-commit, happy path", func(t *testing.T) {
		// Connect client+server
		boltConn, srv, cleanup := setupBoltPipe(t)
		defer cleanup()
		go func() {
			srv.accept(3)
			srv.waitAndServeAutoCommit(strm1)
		}()
		bolt, err := Connect("name", boltConn, auth)
		if err != nil {
			t.Fatal(err)
		}
		defer bolt.Close()

		str, _ := bolt.Run("MATCH (n) RETURN n", nil, conn.ReadMode, nil, 0, nil)
		assertKeys(t, keys, str)

		// Retrieve the records
		for i := 1; i < len(strm1)-1; i++ {
			rec, sum, err := bolt.Next(str.Handle)
			assertOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlySummary(t, rec, sum, err)
	})

	ot.Run("Run transactional, happy path", func(t *testing.T) {
		// Connect client+server
		nconn, srv, cleanup := setupBoltPipe(t)
		defer cleanup()
		go func() {
			srv.accept(3)
			srv.waitAndServeTransRun(strm2, true)
		}()
		bolt, _ := Connect("name", nconn, auth)
		defer bolt.Close()

		tx, err := bolt.TxBegin(conn.ReadMode, nil, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		str, err := bolt.RunTx(tx, "MATCH (n) RETURN n", nil)
		if err != nil {
			t.Fatal(err)
		}
		assertKeys(t, keys, str)

		// Retrieve the records
		for i := 1; i < len(strm2)-1; i++ {
			rec, sum, err := bolt.Next(str.Handle)
			assertOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlySummary(t, rec, sum, err)

		bolt.TxCommit(tx)
	})

	ot.Run("Run transactional, rollback", func(t *testing.T) {
		// Connect client+server
		nconn, srv, cleanup := setupBoltPipe(t)
		defer cleanup()
		go func() {
			srv.accept(3)
			srv.waitAndServeTransRun(strm2, false)
		}()
		bolt, _ := Connect("name", nconn, auth)
		defer bolt.Close()

		tx, err := bolt.TxBegin(conn.ReadMode, nil, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		str, err := bolt.RunTx(tx, "MATCH (n) RETURN n", nil)
		if err != nil {
			t.Fatal(err)
		}
		assertKeys(t, keys, str)

		// Retrieve the records
		for i := 1; i < len(strm2)-1; i++ {
			rec, sum, err := bolt.Next(str.Handle)
			assertOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(str.Handle)
		assertOnlySummary(t, rec, sum, err)

		bolt.TxRollback(tx)
	})
}
