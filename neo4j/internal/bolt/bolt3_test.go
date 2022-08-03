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
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"context"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func TestBolt3(outer *testing.T) {
	// Test streams
	// Faked returns from a server
	runKeys := []any{"f1", "f2"}
	runBookmark := "bm"
	runResponse := []testStruct{
		{
			tag: msgSuccess,
			fields: []any{
				map[string]any{
					"fields":  runKeys,
					"t_first": int64(1),
				},
			},
		},
		{
			tag:    msgRecord,
			fields: []any{[]any{"1v1", "1v2"}},
		},
		{
			tag:    msgRecord,
			fields: []any{[]any{"2v1", "2v2"}},
		},
		{
			tag:    msgRecord,
			fields: []any{[]any{"3v1", "3v2"}},
		},
		{
			tag:    msgSuccess,
			fields: []any{map[string]any{"bookmark": runBookmark, "type": "r"}},
		},
	}

	auth := map[string]any{
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

	assertRunResponseOk := func(t *testing.T, bolt *bolt3,
		stream idb.StreamHandle) {
		for i := 1; i < len(runResponse)-1; i++ {
			rec, sum, err := bolt.Next(context.Background(), stream)
			AssertNextOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)
	}

	connectToServer := func(t *testing.T, serverJob func(srv *bolt3server)) (*bolt3, func()) {
		// Connect client+server
		tcpConn, srv, cleanup := setupBolt3Pipe(t)
		go serverJob(srv)

		c, err := Connect(context.Background(), "serverName", tcpConn, auth, "007", nil, logger, nil)
		if err != nil {
			t.Fatal(err)
		}

		bolt := c.(*bolt3)
		assertBoltState(t, bolt3_ready, bolt)
		if bolt.out.useUtc {
			t.Fatalf("Bolt 3 connections must never send and receive UTC datetimes")
		}
		return bolt, cleanup
	}

	// Simple successful connect
	outer.Run("Connect success", func(t *testing.T) {
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
		defer bolt.Close(context.Background())

		// Check Bolt properties
		AssertStringEqual(t, bolt.ServerName(), "serverName")
		AssertTrue(t, bolt.IsAlive())
	})

	outer.Run("Failed authentication", func(t *testing.T) {
		conn, srv, cleanup := setupBolt3Pipe(t)
		defer cleanup()
		defer conn.Close()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(3)
			srv.waitForHello()
			srv.rejectHelloUnauthorized()
		}()
		bolt, err := Connect(context.Background(), "serverName", conn, auth, "007", nil, logger, nil)
		AssertNil(t, bolt)
		AssertError(t, err)
		dbErr := err.(*db.Neo4jError)
		if !dbErr.IsAuthenticationFailed() {
			t.Errorf("Should be authentication error: %s", dbErr)
		}
	})

	outer.Run("Run auto-commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		str, _ := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)
		assertBoltState(t, bolt3_streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt3_ready, bolt)
	})

	outer.Run("Run transactional commit", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_tx, bolt)
		str, err := bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "MATCH (n) RETURN n"})
		assertBoltState(t, bolt3_streamingtx, bolt)
		AssertNoError(t, err)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt3_tx, bolt)

		bolt.TxCommit(context.Background(), tx)
		assertBoltState(t, bolt3_ready, bolt)
		AssertStringEqual(t, committedBookmark, bolt.Bookmark())
	})

	outer.Run("Begin transaction with bookmark success", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_tx, bolt)
		bolt.RunTx(context.Background(), tx, idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"})
		assertBoltState(t, bolt3_streamingtx, bolt)
		bolt.TxCommit(context.Background(), tx)
		assertBoltState(t, bolt3_ready, bolt)
		AssertStringEqual(t, committedBookmark, bolt.Bookmark())
	})

	outer.Run("Begin transaction with bookmark failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForTxBegin()
			srv.sendFailureMsg("code", "not synced")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		_, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}})
		assertBoltState(t, bolt3_failed, bolt)
		AssertError(t, err)
		AssertStringEqual(t, "", bolt.Bookmark())
	})

	outer.Run("Run transactional rollback", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRunTx(runResponse, false, "")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_tx, bolt)
		str, err := bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_streamingtx, bolt)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt3_tx, bolt)

		bolt.TxRollback(context.Background(), tx)
		assertBoltState(t, bolt3_ready, bolt)
	})

	outer.Run("Server close while streaming", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
			// Send response to run and first record as response to pull
			srv.send(msgSuccess, map[string]any{
				"fields":  runKeys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []any{"1v1", "1v2"})
			// Pretty nice towards bolt, a full message is written
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		str, err := bolt.Run(context.Background(),
			idb.Command{Cypher: "MATCH (n) RETURN n"},
			idb.TxConfig{Mode: idb.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_streaming, bolt)

		// Retrieve the first record
		rec, sum, err := bolt.Next(context.Background(), str)
		AssertNextOnlyRecord(t, rec, sum, err)

		// Next one should fail due to connection closed
		rec, sum, err = bolt.Next(context.Background(), str)
		AssertNextOnlyError(t, rec, sum, err)
		assertBoltDead(t, bolt)
	})

	outer.Run("Server fail on run with reset", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
			srv.sendFailureMsg("code", "msg")
			srv.waitForReset()
			srv.sendIgnoredMsg()
			srv.sendIgnoredMsg()
			srv.sendSuccess(map[string]any{})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		// Fake syntax error that doesn't really matter...
		_, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		AssertNeo4jError(t, err)
		assertBoltState(t, bolt3_failed, bolt)

		bolt.Reset(context.Background())
		assertBoltState(t, bolt3_ready, bolt)
	})

	outer.Run("Server fail on run continue to commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForTxBegin()
			srv.sendSuccess(nil)
			srv.waitForRun()
			srv.waitForPullAll()
			srv.sendFailureMsg("code", "msg")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode})
		AssertNoError(t, err)
		_, err = bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNeo4jError(t, err)
		err = bolt.TxCommit(context.Background(), tx) // This will fail due to above failed
		AssertNeo4jError(t, err)                      // Should have same error as from run since that is original cause
	})

	outer.Run("Reset while streaming ", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
			// Reset message is never sent to server since stream is consumed
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		_, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		AssertNoError(t, err)
		assertBoltState(t, bolt3_streaming, bolt)

		bolt.Reset(context.Background())
		assertBoltState(t, bolt3_ready, bolt)
	})

	outer.Run("Buffer stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		stream, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher"}, idb.TxConfig{Mode: idb.ReadMode})
		// This should force all records to be buffered in the stream
		err := bolt.Buffer(context.Background(), stream)
		AssertNoError(t, err)
		// The bookmark should be set
		AssertStringEqual(t, bolt.Bookmark(), runBookmark)

		// Server closed connection and bolt will go into failed state
		_, err = bolt.Run(context.Background(), idb.Command{Cypher: "cypher"},
			idb.TxConfig{Mode: idb.ReadMode})
		AssertError(t, err)
		assertBoltState(t, bolt3_dead, bolt)

		// Should still be able to read from the stream even though bolt is dead
		assertRunResponseOk(t, bolt, stream)

		// Buffering again should not affect anything
		err = bolt.Buffer(context.Background(), stream)
		AssertNoError(t, err)
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)
	})

	outer.Run("Buffer stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
			// Send response to run and first record as response to pull
			srv.send(msgSuccess, map[string]any{
				"fields":  runKeys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []any{"1v1", "1v2"})
			srv.sendFailureMsg("thecode", "themessage")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		stream, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher"},
			idb.TxConfig{Mode: idb.ReadMode})
		// This should force all records to be buffered in the stream
		err := bolt.Buffer(context.Background(), stream)
		// Should be no error here since we got one record before the error
		AssertNoError(t, err)
		// Retrieve the one record we got
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Now we should see the error, this is to handle errors happening on a specifiec
		// record, like division by zero.
		rec, sum, err = bolt.Next(context.Background(), stream)
		AssertNextOnlyError(t, rec, sum, err)
		// Should be no bookmark since we failed
		AssertStringEqual(t, bolt.Bookmark(), "")
	})

	outer.Run("Buffer stream with invalid handle", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		err := bolt.Buffer(context.Background(), idb.StreamHandle(1))
		AssertError(t, err)
	})

	outer.Run("Consume stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.serveRun(runResponse)
			srv.closeConnection()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		stream, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher"}, idb.TxConfig{Mode: idb.ReadMode})
		// This should force all records to be buffered in the stream
		sum, err := bolt.Consume(context.Background(), stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
		// The bookmark should be set
		AssertStringEqual(t, bolt.Bookmark(), runBookmark)
		AssertStringEqual(t, sum.Bookmark, runBookmark)

		// Should only get the summary from the stream since we consumed everything
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)

		// Consuming again should just return the summary again
		sum, err = bolt.Consume(context.Background(), stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
	})

	outer.Run("Consume stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
			srv.waitForRun()
			srv.waitForPullAll()
			// Send response to run and first record as response to pull
			srv.send(msgSuccess, map[string]any{
				"fields":  runKeys,
				"t_first": int64(1),
			})
			srv.send(msgRecord, []any{"1v1", "1v2"})
			srv.sendFailureMsg("thecode", "themessage")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		stream, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher"}, idb.TxConfig{Mode: idb.ReadMode})
		// This should force all records to be buffered in the stream
		sum, err := bolt.Consume(context.Background(), stream)
		AssertNeo4jError(t, err)
		AssertNil(t, sum)
		AssertStringEqual(t, bolt.Bookmark(), "")

		// Should not get the summary since there was an error
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNeo4jError(t, err)
		AssertNextOnlyError(t, rec, sum, err)
	})

	outer.Run("Consume with invalid stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt3server) {
			srv.accept(3)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		sum, err := bolt.Consume(context.Background(), idb.StreamHandle(1))
		AssertNil(t, sum)
		AssertError(t, err)
	})

	outer.Run("Updates connection idle date on every response", func(inner *testing.T) {
		ctx := context.Background()
		testStart := time.Now()

		callbacks := []struct {
			scenario string
			server   func(*testing.T, *bolt3server)
			client   func(*testing.T, *bolt3)
		}{
			{
				scenario: "after HELLO",
				server:   func(t *testing.T, srv *bolt3server) {},
				client: func(t *testing.T, cli *bolt3) {
					AssertAfter(t, cli.IdleDate(), testStart)
				},
			},
			{
				scenario: "after successful RESET",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForReset()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					cli.ForceReset(ctx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed RESET",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForReset()
					srv.sendFailureMsg("o.o.p.s", "reset failed")
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					cli.ForceReset(ctx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after error on RESET",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForReset()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					cli.ForceReset(ctx)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful RUN/PULLALL",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForRun()
					srv.sendSuccess(map[string]any{})
					srv.waitForPullAll()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed RUN",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForRun()
					srv.sendFailureMsg("o.o.p.s", "run failed")
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored RUN",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForRun()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed PULLALL",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForRun()
					srv.sendSuccess(map[string]any{})
					srv.waitForPullAll()
					srv.sendFailureMsg("o.o.p.s", "pull all failed")
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful BEGIN",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.TxBegin(ctx, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed BEGIN",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendFailureMsg("o.o.p.s", "begin failed")
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.TxBegin(ctx, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored BEGIN",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt3) {
					idleDate := cli.IdleDate()
					_, _ = cli.TxBegin(ctx, idb.TxConfig{})
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful COMMIT",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
					srv.waitForTxCommit()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt3) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{})
					idleDate := cli.IdleDate()
					_ = cli.TxCommit(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed COMMIT",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
					srv.waitForTxCommit()
					srv.sendFailureMsg("o.o.p.s", "commit failed")
				},
				client: func(t *testing.T, cli *bolt3) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{})
					idleDate := cli.IdleDate()
					_ = cli.TxCommit(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored COMMIT",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
					srv.waitForTxCommit()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt3) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{})
					idleDate := cli.IdleDate()
					_ = cli.TxCommit(ctx, tx)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful ROLLBACK",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
					srv.waitForTxRollback()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt3) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{})
					idleDate := cli.IdleDate()
					_ = cli.TxRollback(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed ROLLBACK",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
					srv.waitForTxRollback()
					srv.sendFailureMsg("o.o.p.s", "rollback failed")
				},
				client: func(t *testing.T, cli *bolt3) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{})
					idleDate := cli.IdleDate()
					_ = cli.TxRollback(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored ROLLBACK",
				server: func(t *testing.T, srv *bolt3server) {
					srv.waitForTxBegin()
					srv.sendSuccess(map[string]any{})
					srv.waitForTxRollback()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt3) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{})
					idleDate := cli.IdleDate()
					_ = cli.TxRollback(ctx, tx)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
		}

		for _, callback := range callbacks {
			inner.Run(callback.scenario, func(t *testing.T) {
				bolt, cleanup := connectToServer(inner, func(srv *bolt3server) {
					srv.accept(3)
					callback.server(t, srv)
				})
				defer cleanup()
				defer bolt.Close(ctx)

				callback.client(t, bolt)
			})
		}

	})
}
