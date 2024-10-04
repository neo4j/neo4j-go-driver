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
	"fmt"
	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

type recordingBoltLogger struct {
	clientMessages []string
	serverMessages []string
}

func (r *recordingBoltLogger) LogClientMessage(context string, msg string, args ...any) {
	fmtString := fmt.Sprintf("[%s]", context) + msg
	r.clientMessages = append(r.clientMessages, fmt.Sprintf(fmtString, args...))

}

func (r *recordingBoltLogger) LogServerMessage(context string, msg string, args ...any) {
	fmtString := fmt.Sprintf("[%s]", context) + msg
	r.serverMessages = append(r.serverMessages, fmt.Sprintf(fmtString, args...))
}

// bolt5.Connect is tested through Connect, no need to test it here
func TestBolt5(outer *testing.T) {
	// Test streams
	// Faked returns from a server
	runKeys := []any{"f1", "f2"}
	runBookmark := "bm"
	runQid := 7
	runResponse := []testStruct{
		{
			tag: msgSuccess,
			fields: []any{
				map[string]any{
					"fields":  runKeys,
					"t_first": int64(1),
					"qid":     int64(runQid),
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

	auth := &idb.ReAuthToken{
		FromSession: false,
		Manager: iauth.Token{Tokens: map[string]any{
			"scheme":      "basic",
			"principal":   "neo4j",
			"credentials": "pass",
		}},
	}

	assertBoltState := func(t *testing.T, expected int, bolt *bolt5) {
		t.Helper()
		if expected != bolt.state {
			t.Errorf("Bolt is in unexpected state %d vs %d", expected, bolt.state)
		}
	}

	assertBoltDead := func(t *testing.T, bolt *bolt5) {
		t.Helper()
		if bolt.IsAlive() {
			t.Error("Bolt is alive when it should be dead")
		}
	}

	assertRunResponseOk := func(t *testing.T, bolt *bolt5,
		stream idb.StreamHandle) {
		for i := 1; i < len(runResponse)-1; i++ {
			rec, sum, err := bolt.Next(context.Background(), stream)
			AssertNextOnlyRecord(t, rec, sum, err)
		}
		// Retrieve the summary
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)
	}

	connectToServer := func(t *testing.T, serverJob func(srv *bolt5server)) (*bolt5, func()) {
		// Connect client+server
		tcpConn, srv, cleanup := setupBolt5Pipe(t)
		go serverJob(srv)

		c, err := Connect(
			context.Background(),
			"serverName",
			tcpConn,
			auth,
			"007",
			nil,
			noopErrorListener{},
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		if err != nil {
			t.Fatal(err)
		}

		bolt := c.(*bolt5)
		assertBoltState(t, bolt5Ready, bolt)
		if !bolt.queue.out.useUtc {
			t.Fatalf("Bolt 5+ connections must always send and receive UTC datetimes")
		}
		return bolt, cleanup
	}

	outer.Run("Connect success", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			handshake := srv.waitForHandshake()
			AssertMajorVersionInHandshake(t, handshake, 5)
			srv.acceptVersion(5, 0)
			srv.waitForHello()
			srv.acceptHello()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		AssertStringEqual(t, bolt.ServerName(), "serverName")
		AssertTrue(t, bolt.IsAlive())
		AssertTrue(t, reflect.DeepEqual(bolt.queue.in.connReadTimeout, time.Duration(-1)))
	})

	outer.Run("Connect success in 5.1", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			handshake := srv.waitForHandshake()
			AssertVersionInHandshake(t, handshake, 5, 1)
			srv.acceptVersion(5, 1)
			srv.waitForHelloWithoutAuthToken()
			srv.acceptHello()

			srv.waitForLogon()
			srv.acceptLogon()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		// Check Bolt properties
		AssertStringEqual(t, bolt.ServerName(), "serverName")
		AssertTrue(t, bolt.IsAlive())
		AssertTrue(t, reflect.DeepEqual(bolt.queue.in.connReadTimeout, time.Duration(-1)))
	})

	outer.Run("Connect success in 5.3", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			handshake := srv.waitForHandshake()
			AssertVersionInHandshake(t, handshake, 5, 3)
			srv.acceptVersion(5, 3)
			// 5.3 hello must contain mandatory bolt_agent dictionary and mandatory product field
			hmap := srv.waitForHelloWithoutAuthToken()
			boltAgent, exists := hmap["bolt_agent"]
			AssertTrue(t, exists)
			AssertStringContain(t, boltAgent.(map[string]any)["product"].(string), "neo4j-go/")
			srv.acceptHello()

			srv.waitForLogon()
			srv.acceptLogon()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		// Check Bolt properties
		AssertStringEqual(t, bolt.ServerName(), "serverName")
		AssertTrue(t, bolt.IsAlive())
		AssertTrue(t, reflect.DeepEqual(bolt.queue.in.connReadTimeout, time.Duration(-1)))
	})

	outer.Run("Connect success with timeout hint", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.waitForHandshake()
			srv.acceptVersion(5, 0)
			srv.waitForHello()
			srv.acceptHelloWithHints(map[string]any{"connection.recv_timeout_seconds": 42})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		AssertTrue(t, reflect.DeepEqual(bolt.queue.in.connReadTimeout, 42*time.Second))
	})

	outer.Run("Connect success with timeout hint in 5.1", func(inner *testing.T) {
		bolt, cleanup := connectToServer(inner, func(srv *bolt5server) {
			srv.waitForHandshake()
			srv.acceptVersion(5, 1)
			inner.Run("Run auto-commit", func(t *testing.T) {
				cypherText := "MATCH (n)"
				theDb := "thedb"
				bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
					srv.accept(5)
					srv.serveRun(runResponse, func(fields []any) {
						// fields consist of cypher text, cypher params, meta
						AssertStringEqual(t, fields[0].(string), cypherText)
						meta := fields[2].(map[string]any)
						AssertStringEqual(t, meta["db"].(string), theDb)
					})
				})
				defer cleanup()
				defer bolt.Close(context.Background())

				bolt.SelectDatabase(theDb)
				str, _ := bolt.Run(context.Background(),
					idb.Command{Cypher: cypherText}, idb.TxConfig{Mode: idb.ReadMode})
				skeys, _ := bolt.Keys(str)
				assertKeys(t, runKeys, skeys)
				assertBoltState(t, bolt5Streaming, bolt)

				// Retrieve the records
				assertRunResponseOk(t, bolt, str)
				assertBoltState(t, bolt5Ready, bolt)
			})
			srv.waitForHelloWithoutAuthToken()
			srv.acceptHelloWithHints(map[string]any{"connection.recv_timeout_seconds": 42})
			srv.waitForLogon()
			srv.acceptLogon()
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		AssertTrue(inner, reflect.DeepEqual(bolt.queue.in.connReadTimeout, 42*time.Second))
	})

	invalidValues := []any{4.2, "42", -42}
	for _, value := range invalidValues {
		outer.Run(fmt.Sprintf("Connect success with ignored invalid timeout hint %v", value), func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
				srv.waitForHandshake()
				srv.acceptVersion(5, 0)
				srv.waitForHello()
				srv.acceptHelloWithHints(map[string]any{"connection.recv_timeout_seconds": value})
			})
			defer cleanup()
			defer bolt.Close(context.Background())

			AssertTrue(t, reflect.DeepEqual(bolt.queue.in.connReadTimeout, time.Duration(-1)))
		})
	}

	outer.Run("Routing in hello", func(t *testing.T) {
		routingContext := map[string]string{"some": "thing"}
		conn, srv, cleanup := setupBolt5Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(5, 0)
			hmap := srv.waitForHello()
			helloRoutingContext := hmap["routing"].(map[string]any)
			if len(helloRoutingContext) != len(routingContext) {
				panic("Routing contexts differ")
			}
			srv.acceptHello()
		}()
		bolt, err := Connect(
			context.Background(),
			"serverName",
			conn,
			auth,
			"007",
			routingContext,
			nil,
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		AssertNoError(t, err)
		bolt.Close(context.Background())
	})

	outer.Run("Routing in hello in 5.1", func(t *testing.T) {
		routingContext := map[string]string{"some": "thing"}
		conn, srv, cleanup := setupBolt5Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(5, 1)
			hmap := srv.waitForHelloWithoutAuthToken()
			helloRoutingContext := hmap["routing"].(map[string]any)
			if len(helloRoutingContext) != len(routingContext) {
				panic("Routing contexts differ")
			}
			srv.acceptHello()
			srv.waitForLogon()
			srv.acceptLogon()
		}()
		bolt, err := Connect(
			context.Background(),
			"serverName",
			conn,
			auth,
			"007",
			routingContext,
			nil,
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		AssertNoError(t, err)
		bolt.Close(context.Background())
	})

	outer.Run("No routing in hello", func(t *testing.T) {
		conn, srv, cleanup := setupBolt5Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(5, 0)
			hmap := srv.waitForHello()
			_, exists := hmap["routing"].(map[string]any)
			if exists {
				panic("Should be no routing entry")
			}
			srv.acceptHello()
		}()
		bolt, err := Connect(
			context.Background(),
			"serverName",
			conn,
			auth,
			"007",
			nil,
			nil,
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		AssertNoError(t, err)
		bolt.Close(context.Background())
	})

	outer.Run("No routing in hello 5.1", func(t *testing.T) {
		conn, srv, cleanup := setupBolt5Pipe(t)
		defer cleanup()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(5, 1)
			hmap := srv.waitForHelloWithoutAuthToken()
			_, exists := hmap["routing"].(map[string]any)
			if exists {
				panic("Should be no routing entry")
			}
			srv.acceptHello()
			srv.waitForLogon()
			srv.acceptLogon()
		}()
		bolt, err := Connect(
			context.Background(),
			"serverName",
			conn,
			auth,
			"007",
			nil,
			nil,
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		AssertNoError(t, err)
		bolt.Close(context.Background())
	})

	outer.Run("Failed authentication", func(t *testing.T) {
		conn, srv, cleanup := setupBolt5Pipe(t)
		defer cleanup()
		defer conn.Close()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(5, 0)
			srv.waitForHello()
			srv.rejectHelloUnauthorized()
		}()
		bolt, err := Connect(
			context.Background(),
			"serverName",
			conn,
			auth,
			"007",
			nil,
			noopErrorListener{},
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
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

	outer.Run("Failed authentication in 5.1", func(t *testing.T) {
		conn, srv, cleanup := setupBolt5Pipe(t)
		defer cleanup()
		defer conn.Close()
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(5, 1)
			srv.waitForHelloWithoutAuthToken()
			srv.acceptHello()
			srv.waitForLogon()
			srv.rejectLogonWithoutAuthToken()
		}()
		bolt, err := Connect(
			context.Background(),
			"serverName",
			conn,
			auth,
			"007",
			nil,
			noopErrorListener{},
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
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

	outer.Run("Run auto-commit", func(t *testing.T) {
		cypherText := "MATCH (n)"
		theDb := "thedb"
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRun(runResponse, func(fields []any) {
				// fields consist of cypher text, cypher params, meta
				AssertStringEqual(t, fields[0].(string), cypherText)
				meta := fields[2].(map[string]any)
				AssertStringEqual(t, meta["db"].(string), theDb)
			})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		bolt.SelectDatabase(theDb)
		str, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: cypherText}, idb.TxConfig{Mode: idb.ReadMode})
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)
		assertBoltState(t, bolt5Streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt5Ready, bolt)
	})

	outer.Run("Run auto-commit with impersonation", func(t *testing.T) {
		cypherText := "MATCH (n)"
		impersonatedUser := "a user"
		theDb := "thedb"
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.acceptWithMinor(5, 0)
			// Make sure that impersonation id is sent
			srv.serveRun(runResponse, func(fields []any) {
				// fields consist of cypher text, cypher params, meta
				AssertStringEqual(t, fields[0].(string), cypherText)
				meta := fields[2].(map[string]any)
				AssertStringEqual(t, meta["db"].(string), theDb)
				AssertStringEqual(t, meta["imp_user"].(string), impersonatedUser)
			})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		bolt.SelectDatabase(theDb)
		str, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: cypherText}, idb.TxConfig{Mode: idb.ReadMode,
				ImpersonatedUser: impersonatedUser})
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)
		assertBoltState(t, bolt5Streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt5Ready, bolt)
	})

	outer.Run("Run auto-commit with fetch size 2 of 3", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(2)
			srv.send(runResponse[0].tag, runResponse[0].fields...)
			srv.send(runResponse[1].tag, runResponse[1].fields...)
			srv.send(runResponse[2].tag, runResponse[2].fields...)
			srv.send(msgSuccess, map[string]any{"has_more": true})
			srv.waitForPullN(2)
			srv.send(runResponse[3].tag, runResponse[3].fields...)
			srv.send(runResponse[4].tag, runResponse[4].fields...)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		str, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher", FetchSize: 2},
			idb.TxConfig{Mode: idb.ReadMode})
		assertBoltState(t, bolt5Streaming, bolt)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt5Ready, bolt)
	})

	outer.Run("with notifications", func(inner *testing.T) {
		warningSev := "WARNING"
		type testCase struct {
			description     string
			MinSev          notifications.NotificationMinimumSeverityLevel
			DisCats         notifications.NotificationDisabledCategories
			ExpectedMinSev  *string
			ExpectDisCats   bool
			ExpectedDisCats []any
			Method          string
		}
		var testCases []testCase
		for _, s := range []string{"run", "txRun"} {
			testCases = append(testCases,
				testCase{
					description: "default",
					Method:      s,
				},
				testCase{
					description:    "warning minimum severity",
					MinSev:         notifications.WarningLevel,
					ExpectedMinSev: &warningSev,
					Method:         s,
				},
				testCase{
					description:     "disabled categories",
					DisCats:         notifications.DisableCategories(notifications.Unsupported, notifications.Generic),
					ExpectDisCats:   true,
					ExpectedDisCats: []any{"UNSUPPORTED", "GENERIC"},
					Method:          s,
				},
				testCase{
					description:     "warning minimum severity and disabled categories",
					MinSev:          notifications.WarningLevel,
					DisCats:         notifications.DisableCategories(notifications.Unsupported, notifications.Generic),
					ExpectDisCats:   true,
					ExpectedDisCats: []any{"UNSUPPORTED", "GENERIC"},
					ExpectedMinSev:  &warningSev,
					Method:          s,
				},
				testCase{
					description:     "disable no categories",
					DisCats:         notifications.DisableNoCategories(),
					ExpectDisCats:   true,
					ExpectedDisCats: []any{},
					Method:          s,
				})
		}
		inner.Parallel()
		for _, test := range testCases {
			inner.Run(fmt.Sprintf("%s for %s", test.description, test.Method), func(t *testing.T) {
				bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
					srv.acceptWithMinor(5, 2)
					fieldAssertion := func(fieldNum int) func(fields []any) {
						return func(fields []any) {
							if test.ExpectedMinSev != nil {
								AssertStringEqual(t, fields[fieldNum].(map[string]any)["notifications_minimum_severity"].(string), *test.ExpectedMinSev)
							} else {
								AssertMapDoesNotHaveKey(t, fields[fieldNum].(map[string]any), "notifications_minimum_severity")
							}
							if test.ExpectDisCats {
								AssertDeepEquals(t, fields[fieldNum].(map[string]any)["notifications_disabled_categories"], test.ExpectedDisCats)
							} else {
								AssertMapDoesNotHaveKey(t, fields[fieldNum].(map[string]any), "notifications_disabled_categories")
							}
						}
					}
					if test.Method == "run" {
						srv.waitForRun(fieldAssertion(2))
					} else {
						srv.waitForTxBegin(fieldAssertion(0))
					}
					srv.sendFailureMsg("Neo.ClientError.Statement.SyntaxError", "Syntax error")
				})
				defer cleanup()
				defer bolt.Close(context.Background())

				if test.Method == "run" {
					_, _ = bolt.Run(
						context.Background(),
						idb.Command{Cypher: "cypher"},
						idb.TxConfig{
							NotificationConfig: idb.NotificationConfig{
								MinSev:  test.MinSev,
								DisCats: test.DisCats,
							},
						},
					)
				} else {
					_, _ = bolt.TxBegin(
						context.Background(),
						idb.TxConfig{
							NotificationConfig: idb.NotificationConfig{
								MinSev:  test.MinSev,
								DisCats: test.DisCats,
							},
						},
						true,
					)
				}
			})
		}
	})

	outer.Run("notifications unsupported", func(inner *testing.T) {
		type testCase struct {
			description string
			MinSev      notifications.NotificationMinimumSeverityLevel
			DisCats     notifications.NotificationDisabledCategories
			ExpectError bool
			Method      string
		}
		var testCases []testCase
		for _, s := range []string{"run", "txRun"} {
			testCases = append(testCases,
				testCase{
					description: "default",
					Method:      s,
				},
				testCase{
					description: "warning minimum severity",
					MinSev:      notifications.WarningLevel,
					ExpectError: true,
					Method:      s,
				},
				testCase{
					description: "disabled categories",
					DisCats:     notifications.DisableCategories(notifications.Unsupported, notifications.Generic),
					ExpectError: true,
					Method:      s,
				},
				testCase{
					description: "warning minimum severity and disabled categories",
					MinSev:      notifications.WarningLevel,
					DisCats:     notifications.DisableCategories(notifications.Unsupported, notifications.Generic),
					ExpectError: true,
					Method:      s,
				},
				testCase{
					description: "disable no categories",
					DisCats:     notifications.DisableNoCategories(),
					ExpectError: true,
					Method:      s,
				})
		}
		inner.Parallel()
		for _, test := range testCases {
			testCopy := test
			inner.Run(fmt.Sprintf("%s for %s", test.description, test.Method), func(t *testing.T) {
				bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
					srv.acceptWithMinor(5, 1)
					if !testCopy.ExpectError {
						if testCopy.Method == "run" {
							srv.waitForRun(nil)
						} else {
							srv.waitForTxBegin(nil)
						}
						srv.sendFailureMsg("Neo.ClientError.Statement.SyntaxError", "Syntax error")
					}
				})
				defer cleanup()
				defer bolt.Close(context.Background())

				var err error
				if test.Method == "run" {
					_, err = bolt.Run(
						context.Background(),
						idb.Command{Cypher: "cypher"},
						idb.TxConfig{
							NotificationConfig: idb.NotificationConfig{
								MinSev:  test.MinSev,
								DisCats: test.DisCats,
							},
						},
					)
				} else {
					_, err = bolt.TxBegin(
						context.Background(),
						idb.TxConfig{
							NotificationConfig: idb.NotificationConfig{
								MinSev:  test.MinSev,
								DisCats: test.DisCats,
							},
						},
						true,
					)
				}
				if test.ExpectError {
					AssertErrorMessageContains(t, err, "does not support: notification filtering")
				} else {
					AssertErrorMessageContains(t, err, "SyntaxError")
				}
			})
		}
	})

	outer.Run("Run transactional commit", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode}, true)
		AssertNoError(t, err)
		// Lazy start of transaction when no bookmark
		assertBoltState(t, bolt5Tx, bolt)
		str, err := bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "MATCH (n) RETURN n"})
		assertBoltState(t, bolt5StreamingTx, bolt)
		AssertNoError(t, err)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt5Tx, bolt)

		_ = bolt.TxCommit(context.Background(), tx)
		assertBoltState(t, bolt5Ready, bolt)
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, committedBookmark, bookmark)
	})

	// Verifies that current stream is discarded correctly even if it is larger
	// than what is served by a single pull.
	outer.Run("Commit while streaming", func(t *testing.T) {
		qid := int64(2)
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.send(msgSuccess, map[string]any{})
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			// Send Pull response
			srv.send(msgSuccess, map[string]any{"fields": []any{"k"}, "t_first": int64(1), "qid": qid})
			// ... and the record
			srv.send(msgRecord, []any{"v1"})
			// ... and the batch summary
			srv.send(msgSuccess, map[string]any{"has_more": true})
			// Wait for the discard message (no need for qid since the last executed query is discarded)
			srv.waitForDiscardN(-1)
			// Respond to discard with has more to indicate that there are more records
			srv.send(msgSuccess, map[string]any{"has_more": true})
			// Wait for the commit
			srv.waitForTxCommit()
			srv.send(msgSuccess, map[string]any{"bookmark": "x"})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode}, true)
		AssertNoError(t, err)
		_, err = bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "Whatever", FetchSize: 1})
		AssertNoError(t, err)

		err = bolt.TxCommit(context.Background(), tx)
		AssertNoError(t, err)
		assertBoltState(t, bolt5Ready, bolt)
	})

	// Verifies that current stream is discarded correctly even if it is larger
	// than what is served by a single pull.
	outer.Run("Commit while streams, explicit consume", func(t *testing.T) {
		qid := int64(2)
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.send(msgSuccess, map[string]any{})
			// First RunTx
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			// Send Pull response
			srv.send(msgSuccess, map[string]any{"fields": []any{"k"}, "t_first": int64(1), "qid": qid})
			// Driver should discard this stream which is small
			srv.send(msgRecord, []any{"v1"})
			srv.send(msgSuccess, map[string]any{"has_more": false})
			// Second RunTx
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			srv.send(msgSuccess, map[string]any{"fields": []any{"k"}, "t_first": int64(1), "qid": qid})
			// Driver should discard this stream, which is small
			srv.send(msgRecord, []any{"v1"})
			srv.send(msgSuccess, map[string]any{"has_more": false})
			// Wait for the commit
			srv.waitForTxCommit()
			srv.send(msgSuccess, map[string]any{"bookmark": "x"})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode}, true)
		AssertNoError(t, err)
		s, err := bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "Whatever", FetchSize: 1})
		AssertNoError(t, err)
		_, err = bolt.Consume(context.Background(), s)
		AssertNoError(t, err)
		s, err = bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "Whatever", FetchSize: 1})
		AssertNoError(t, err)
		_, err = bolt.Consume(context.Background(), s)
		AssertNoError(t, err)

		err = bolt.TxCommit(context.Background(), tx)
		AssertNoError(t, err)
		assertBoltState(t, bolt5Ready, bolt)
	})

	outer.Run("Begin transaction with bookmark success", func(t *testing.T) {
		committedBookmark := "cbm"
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRunTx(runResponse, true, committedBookmark)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}}, true)
		AssertNoError(t, err)
		assertBoltState(t, bolt5Tx, bolt)
		_, _ = bolt.RunTx(context.Background(), tx, idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"})
		assertBoltState(t, bolt5StreamingTx, bolt)
		_ = bolt.TxCommit(context.Background(), tx)
		assertBoltState(t, bolt5Ready, bolt)
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, committedBookmark, bookmark)
	})

	outer.Run("Begin transaction with bookmark failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.sendFailureMsg("code", "not synced")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		_, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}}, true)
		assertBoltState(t, bolt5Failed, bolt)
		AssertError(t, err)
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, "", bookmark)
	})

	outer.Run("Begin transaction lazy with syncMessages false", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.sendFailureMsg("code", "Driver didn't lazily send begin")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		_, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}}, false)

		assertBoltState(t, bolt5Tx, bolt)
		AssertNoError(t, err)
	})

	outer.Run("Begin Pipelined handles unserializable tx metadata", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		unserializable := func() {}

		_, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}, Meta: map[string]any{"foo": unserializable}},
			false)
		AssertErrorMessageContains(t, err, "Usage of type '%s' is not supported", reflect.TypeOf(unserializable))
	})

	outer.Run("Begin Pipelined", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.waitForRun(nil)
			srv.waitForPullN(1000)
			srv.sendSuccess(nil)
			srv.sendSuccess(nil)
			srv.sendSuccess(nil)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, _ := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode, Bookmarks: []string{"bm1"}}, false)
		_, err := bolt.RunTx(context.Background(), tx, idb.Command{})
		AssertNoError(t, err)
	})

	outer.Run("Run transactional rollback", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRunTx(runResponse, false, "")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode}, true)
		AssertNoError(t, err)
		assertBoltState(t, bolt5Tx, bolt)
		str, err := bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNoError(t, err)
		assertBoltState(t, bolt5StreamingTx, bolt)
		skeys, _ := bolt.Keys(str)
		assertKeys(t, runKeys, skeys)

		// Retrieve the records
		assertRunResponseOk(t, bolt, str)
		assertBoltState(t, bolt5Tx, bolt)

		_ = bolt.TxRollback(context.Background(), tx)
		assertBoltState(t, bolt5Ready, bolt)
	})

	outer.Run("Server close while streaming", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt5FetchSize)
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
		assertBoltState(t, bolt5Streaming, bolt)

		// Retrieve the first record
		rec, sum, err := bolt.Next(context.Background(), str)
		AssertNextOnlyRecord(t, rec, sum, err)

		// Next one should fail due to connection closed
		rec, sum, err = bolt.Next(context.Background(), str)
		AssertNextOnlyError(t, rec, sum, err)
		assertBoltDead(t, bolt)
	})

	outer.Run("Server fail on run with reset", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt5FetchSize)
			srv.sendFailureMsg("code", "msg") // RUN failed
			srv.sendIgnoredMsg()              // PULL Ignored
			srv.waitForReset()
			srv.sendSuccess(map[string]any{})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		// Fake syntax error that doesn't really matter...
		_, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		AssertNeo4jError(t, err)
		assertBoltState(t, bolt5Failed, bolt)

		bolt.Reset(context.Background())
		assertBoltState(t, bolt5Ready, bolt)
	})

	outer.Run("Server fail on run continue to commit", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.sendSuccess(nil)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt5FetchSize)
			srv.sendFailureMsg("code", "msg")
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		tx, err := bolt.TxBegin(context.Background(),
			idb.TxConfig{Mode: idb.ReadMode}, true)
		AssertNoError(t, err)
		_, err = bolt.RunTx(context.Background(), tx,
			idb.Command{Cypher: "MATCH (n) RETURN n"})
		AssertNeo4jError(t, err)
		err = bolt.TxCommit(context.Background(), tx) // This will fail due to above failed
		AssertNeo4jError(t, err)                      // Should have same error as from run since that is original cause
	})

	outer.Run("Reset in ready state", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRun(runResponse, nil)
		})
		defer cleanup()
		defer bolt.Close(context.Background())
		s, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		AssertNoError(t, err)
		_, err = bolt.Consume(context.Background(), s)
		AssertNoError(t, err)
		// Should be no-op since state already is ready
		bolt.Reset(context.Background())
	})

	outer.Run("Buffer stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRun(runResponse, nil)
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
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, bookmark, runBookmark)

		// Server closed connection and bolt will go into failed state
		_, err = bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher"}, idb.TxConfig{Mode: idb.ReadMode})
		AssertError(t, err)
		assertBoltState(t, bolt5Dead, bolt)

		// Should still be able to read from the stream even though bolt is dead
		assertRunResponseOk(t, bolt, stream)

		// Buffering again should not affect anything
		err = bolt.Buffer(context.Background(), stream)
		AssertNoError(t, err)
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)
	})

	outer.Run("Buffer stream with fetch size", func(t *testing.T) {
		keys := []any{"k1"}
		bookmark := "x"
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(3)
			srv.send(msgSuccess, map[string]any{"fields": keys})
			srv.send(msgRecord, []any{"1"})
			srv.send(msgRecord, []any{"2"})
			srv.send(msgRecord, []any{"3"})
			srv.send(msgSuccess, map[string]any{"has_more": true})
			srv.waitForPullN(-1)
			srv.send(msgRecord, []any{"4"})
			srv.send(msgRecord, []any{"5"})
			srv.send(msgSuccess, map[string]any{"bookmark": bookmark, "type": "r"})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		stream, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher", FetchSize: 3},
			idb.TxConfig{Mode: idb.ReadMode})
		// Read one to put it in less comfortable state
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Buffer the rest
		err = bolt.Buffer(context.Background(), stream)
		AssertNoError(t, err)
		// The bookmark should be set
		bookmark = bolt.Bookmark()
		AssertStringEqual(t, bookmark, bookmark)

		for i := 0; i < 4; i++ {
			rec, sum, err = bolt.Next(context.Background(), stream)
			AssertNextOnlyRecord(t, rec, sum, err)
		}
		rec, sum, err = bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)
		// Buffering again should not affect anything
		err = bolt.Buffer(context.Background(), stream)
		AssertNoError(t, err)
		rec, sum, err = bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)
	})

	outer.Run("Buffer stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt5FetchSize)
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
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, bookmark, "")
	})

	outer.Run("Buffer stream with invalid handle", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		err := bolt.Buffer(context.Background(), idb.StreamHandle(1))
		AssertError(t, err)
	})

	outer.Run("Consume stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.serveRun(runResponse, nil)
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
		assertBoltState(t, bolt5Ready, bolt)
		// The bookmark should be set
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, bookmark, runBookmark)
		AssertStringEqual(t, sum.Bookmark, runBookmark)

		// Should only get the summary from the stream since we consumed everything
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)

		// Consuming again should just return the summary again
		sum, err = bolt.Consume(context.Background(), stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
	})

	outer.Run("Consume stream with fetch size", func(t *testing.T) {
		qid := 3
		keys := []any{"k1"}
		bookmark := "x"
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(3)
			srv.send(msgSuccess, map[string]any{"fields": keys, "qid": int64(qid)})
			srv.send(msgRecord, []any{"1"})
			srv.send(msgRecord, []any{"2"})
			srv.send(msgRecord, []any{"3"})
			srv.send(msgSuccess, map[string]any{"has_more": true, "qid": int64(qid)})
			srv.waitForDiscardN(-1)
			srv.send(msgSuccess, map[string]any{"bookmark": bookmark, "type": "r"})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		stream, _ := bolt.Run(context.Background(),
			idb.Command{Cypher: "cypher", FetchSize: 3},
			idb.TxConfig{Mode: idb.ReadMode})
		// Read one to put it in less comfortable state
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Consume the rest
		sum, err = bolt.Consume(context.Background(), stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
		assertBoltState(t, bolt5Ready, bolt)

		// The bookmark should be set
		bookmark = bolt.Bookmark()
		AssertStringEqual(t, bookmark, bookmark)
		AssertStringEqual(t, sum.Bookmark, bookmark)

		// Should only get the summary from the stream since we consumed everything
		rec, sum, err = bolt.Next(context.Background(), stream)
		AssertNextOnlySummary(t, rec, sum, err)

		// Consuming again should just return the summary again
		sum, err = bolt.Consume(context.Background(), stream)
		AssertNoError(t, err)
		AssertNotNil(t, sum)
	})

	outer.Run("Consume stream with error", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.waitForPullN(bolt5FetchSize)
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
		bookmark := bolt.Bookmark()
		AssertStringEqual(t, bookmark, "")

		// Should not get the summary since there was an error
		rec, sum, err := bolt.Next(context.Background(), stream)
		AssertNeo4jError(t, err)
		AssertNextOnlyError(t, rec, sum, err)
	})

	outer.Run("Consume with invalid stream", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		sum, err := bolt.Consume(context.Background(), idb.StreamHandle(1))
		AssertNil(t, sum)
		AssertError(t, err)
	})

	outer.Run("GetRoutingTable using ROUTE message", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.acceptWithMinor(5, 0)
			srv.waitForRoute(func(fields []any) {
				// Fields contains context(map), bookmarks([]string), extras(map)
			})
			srv.sendSuccess(map[string]any{
				"rt": map[string]any{
					"ttl": 1000,
					"db":  "thedb",
					"servers": []any{
						map[string]any{
							"role":      "ROUTE",
							"addresses": []any{"router1"},
						},
					},
				},
			})
		})
		defer cleanup()
		defer bolt.Close(context.Background())

		rt, err := bolt.GetRoutingTable(context.Background(), map[string]string{"region": "space"}, nil, "thedb", "")
		AssertNoError(t, err)
		ert := &idb.RoutingTable{Routers: []string{"router1"},
			TimeToLive: 1000, DatabaseName: "thedb"}
		if !reflect.DeepEqual(rt, ert) {
			t.Fatalf("Expected:\n%+v\n != Actual: \n%+v\n", rt, ert)
		}
	})

	outer.Run("Expired authentication error should close connection", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.sendFailureMsg("Status.Security.AuthorizationExpired", "auth token is... expired")
		})
		defer cleanup()

		_, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		assertBoltState(t, bolt5Dead, bolt)
		AssertError(t, err)
	})

	outer.Run("Immediately expired authentication token error triggers a connection failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.sendFailureMsg("Neo.ClientError.Security.TokenExpired", "SSO token is... expired")
		})
		defer cleanup()

		_, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		assertBoltState(t, bolt5Failed, bolt)
		AssertError(t, err)
	})

	outer.Run("Expired authentication token error after run triggers a connection failure", func(t *testing.T) {
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForRun(nil)
			srv.sendFailureMsg("Neo.ClientError.Security.TokenExpired", "SSO token is... expired")
		})
		defer cleanup()

		_, err := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (" +
			"n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
		assertBoltState(t, bolt5Failed, bolt)
		AssertError(t, err)
	})

	outer.Run("Updates connection idle date on every response", func(inner *testing.T) {
		ctx := context.Background()
		testStart := time.Now()

		callbacks := []struct {
			scenario string
			server   func(*testing.T, *bolt5server)
			client   func(*testing.T, *bolt5)
		}{
			{
				scenario: "after HELLO",
				server:   func(t *testing.T, srv *bolt5server) {},
				client: func(t *testing.T, cli *bolt5) {
					AssertAfter(t, cli.IdleDate(), testStart)
				},
			},
			{
				scenario: "after successful RESET",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForReset()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					cli.ForceReset(ctx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed RESET",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForReset()
					srv.sendFailureMsg("o.o.p.s", "reset failed")
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					cli.ForceReset(ctx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after error on RESET",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForReset()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					cli.ForceReset(ctx)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful RUN/PULLALL",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForRun(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForPullN(1000)
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed RUN",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForRun(nil)
					srv.sendFailureMsg("o.o.p.s", "run failed")
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored RUN",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForRun(nil)
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed PULLALL",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForRun(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForPullN(1000)
					srv.sendFailureMsg("o.o.p.s", "pull all failed")
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.Run(ctx, idb.Command{Cypher: "RETURN 42"}, idb.TxConfig{})
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful BEGIN",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.TxBegin(ctx, idb.TxConfig{}, true)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed BEGIN",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendFailureMsg("o.o.p.s", "begin failed")
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.TxBegin(ctx, idb.TxConfig{}, true)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored BEGIN",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt5) {
					idleDate := cli.IdleDate()
					_, _ = cli.TxBegin(ctx, idb.TxConfig{}, true)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful COMMIT",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForTxCommit()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt5) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{}, true)
					idleDate := cli.IdleDate()
					_ = cli.TxCommit(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed COMMIT",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForTxCommit()
					srv.sendFailureMsg("o.o.p.s", "commit failed")
				},
				client: func(t *testing.T, cli *bolt5) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{}, true)
					idleDate := cli.IdleDate()
					_ = cli.TxCommit(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored COMMIT",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForTxCommit()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt5) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{}, true)
					idleDate := cli.IdleDate()
					_ = cli.TxCommit(ctx, tx)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after successful ROLLBACK",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForTxRollback()
					srv.sendSuccess(map[string]any{})
				},
				client: func(t *testing.T, cli *bolt5) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{}, true)
					idleDate := cli.IdleDate()
					_ = cli.TxRollback(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "after failed ROLLBACK",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForTxRollback()
					srv.sendFailureMsg("o.o.p.s", "rollback failed")
				},
				client: func(t *testing.T, cli *bolt5) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{}, true)
					idleDate := cli.IdleDate()
					_ = cli.TxRollback(ctx, tx)
					AssertAfter(t, cli.IdleDate(), idleDate)
				},
			},
			{
				scenario: "not after errored ROLLBACK",
				server: func(t *testing.T, srv *bolt5server) {
					srv.waitForTxBegin(nil)
					srv.sendSuccess(map[string]any{})
					srv.waitForTxRollback()
					srv.closeConnection()
				},
				client: func(t *testing.T, cli *bolt5) {
					tx, _ := cli.TxBegin(ctx, idb.TxConfig{}, true)
					idleDate := cli.IdleDate()
					_ = cli.TxRollback(ctx, tx)
					AssertDeepEquals(t, cli.IdleDate(), idleDate)
				},
			},
		}

		for _, callback := range callbacks {
			callbackCopy := callback
			inner.Run(callback.scenario, func(t *testing.T) {
				bolt, cleanup := connectToServer(inner, func(srv *bolt5server) {
					srv.accept(5)
					callbackCopy.server(t, srv)
				})
				defer cleanup()
				defer bolt.Close(ctx)

				callback.client(t, bolt)
			})
		}

	})

	outer.Run("closes underlying socket when context has terminated", func(inner *testing.T) {
		ctx := context.Background()
		pastDeadline := time.Now().Add(-6 * time.Hour)
		pastCtx, pastCtxCancel := context.WithDeadline(context.Background(), pastDeadline)
		defer pastCtxCancel()
		canceledCtx, cancelFunc := context.WithCancel(ctx)
		cancelFunc() // cancel it now
		type testCase struct {
			description string
			ctx         context.Context
			errorMatch  string
		}

		testCases := []testCase{
			{
				description: "due to a past deadline",
				ctx:         pastCtx,
				errorMatch:  "Timeout while writing to connection",
			},
			{
				description: "because of cancelation",
				ctx:         canceledCtx,
				errorMatch:  "Writing to connection has been canceled",
			},
		}
		for _, test := range testCases {
			inner.Run(test.description, func(t *testing.T) {
				var latch sync.WaitGroup
				latch.Add(1)
				bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
					srv.accept(5)
					defer func() {
						// test server reaches EOF since the client closes the socket
						// this happens before being able to dechunk the run message
						AssertDeepEquals(t, recover(), io.EOF)
						latch.Done()
					}()
					srv.waitForRun(nil)
				})
				defer cleanup()
				defer bolt.Close(ctx)

				_, err := bolt.Run(test.ctx, idb.Command{Cypher: "UNWIND [1,2] AS k RETURN k"}, idb.TxConfig{Mode: idb.ReadMode})

				latch.Wait()
				AssertErrorMessageContains(t, err, test.errorMatch)
			})
		}
	})

	outer.Run("tracks tfirst properly", func(t *testing.T) {
		ctx := context.Background()
		bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
			srv.accept(5)
			srv.waitForTxBegin(nil)
			srv.sendSuccess(nil)
			srv.waitForRun(nil)
			srv.waitForPullN(1)
			srv.send(msgSuccess, map[string]any{"fields": []any{"x"}, "t_first": int64(10)})
			srv.send(msgRecord, []any{"1"})
			srv.send(msgSuccess, map[string]any{"has_more": true})
			srv.waitForRun(nil)
			srv.waitForPullN(-1)
			srv.send(msgSuccess, map[string]any{"fields": []any{"x"}, "t_first": int64(20)})
			srv.send(msgRecord, []any{"3"})
			srv.send(msgRecord, []any{"4"})
			srv.send(msgSuccess, map[string]any{"bookmark": "b2", "type": "r"})
			srv.send(msgRecord, []any{"2"})
			srv.send(msgSuccess, map[string]any{"bookmark": "b1", "type": "r"})
		})
		defer cleanup()
		defer bolt.Close(ctx)

		tx1, _ := bolt.TxBegin(ctx, idb.TxConfig{Mode: idb.ReadMode}, true)
		results1, _ := bolt.RunTx(ctx, tx1, idb.Command{Cypher: "UNWIND [1,2] AS x RETURN x", FetchSize: 1})
		results2, _ := bolt.RunTx(ctx, tx1, idb.Command{Cypher: "UNWIND [3,4] AS x RETURN x", FetchSize: -1})
		summary2, _ := bolt.Consume(ctx, results2)
		summary1, _ := bolt.Consume(ctx, results1)

		AssertIntEqual(t, int(summary1.TFirst), 10)
		AssertIntEqual(t, int(summary2.TFirst), 20)
	})

	outer.Run("redacts credentials 5.0", func(t *testing.T) {
		runs := 100
		ctx := context.Background()
		authToken := auth.Manager.(iauth.Token)
		expectedPrincipal := authToken.Tokens["principal"].(string)
		expectedCredentials := authToken.Tokens["credentials"].(string)

		var wg sync.WaitGroup
		wg.Add(runs)
		for i := 0; i < runs; i++ {
			go func() {
				tcpConn, srv, cleanup := setupBolt5Pipe(t)
				defer cleanup()
				go func() {
					srv.waitForHandshake()
					srv.acceptVersion(5, 0)
					hello := srv.waitForHello()
					principal, exists := hello["principal"]
					if !exists {
						t.Error("Missing principal in hello")
					}
					if principal != expectedPrincipal {
						t.Errorf("Expected principal %s but got %s", expectedPrincipal, principal)
					}
					credentials, exists := hello["credentials"]
					if !exists {
						t.Error("Missing credentials in hello")
					}
					if credentials != expectedCredentials {
						t.Errorf("Expected credentials %s but got %s", expectedCredentials, credentials)
					}

					srv.acceptHello()
				}()

				boltLogger := recordingBoltLogger{}

				c, err := Connect(
					context.Background(),
					"serverName",
					tcpConn,
					auth,
					"007",
					nil,
					noopErrorListener{},
					logger,
					&boltLogger,
					idb.NotificationConfig{},
					DefaultReadBufferSize,
				)
				if err != nil {
					t.Error(err)
				}
				defer c.Close(ctx)

				bolt := c.(*bolt5)
				assertBoltState(t, bolt5Ready, bolt)

				AssertAny(t, boltLogger.clientMessages, func(logMsg string) bool {
					if strings.Contains(logMsg, "HELLO") {
						AssertStringContain(t, logMsg, "credentials")
						AssertStringNotContain(t, logMsg, expectedCredentials)
						return true
					}
					return false
				})

				wg.Done()
			}()
		}

		wg.Wait()
	})

	outer.Run("redacts credentials 5.1", func(t *testing.T) {
		runs := 100
		ctx := context.Background()
		authToken := auth.Manager.(iauth.Token)
		expectedPrincipal := authToken.Tokens["principal"].(string)
		expectedCredentials := authToken.Tokens["credentials"].(string)

		var wg sync.WaitGroup
		wg.Add(runs)
		for i := 0; i < runs; i++ {
			go func() {
				tcpConn, srv, cleanup := setupBolt5Pipe(t)
				defer cleanup()
				go func() {
					srv.waitForHandshake()
					srv.acceptVersion(5, 1)
					srv.waitForHelloWithoutAuthToken()
					srv.acceptHello()
					logon := srv.waitForLogon()
					srv.acceptLogon()
					principal, exists := logon["principal"]
					if !exists {
						t.Error("Missing principal in logon")
					}
					if principal != expectedPrincipal {
						t.Errorf("Expected principal %s but got %s", expectedPrincipal, principal)
					}
					credentials, exists := logon["credentials"]
					if !exists {
						t.Error("Missing credentials in logon")
					}
					if credentials != expectedCredentials {
						t.Errorf("Expected credentials %s but got %s", expectedCredentials, credentials)
					}

					srv.acceptLogon()
				}()

				boltLogger := recordingBoltLogger{}

				c, err := Connect(
					context.Background(),
					"serverName",
					tcpConn,
					auth,
					"007",
					nil,
					noopErrorListener{},
					logger,
					&boltLogger,
					idb.NotificationConfig{},
					DefaultReadBufferSize,
				)
				if err != nil {
					t.Error(err)
				}
				defer c.Close(ctx)

				bolt := c.(*bolt5)
				assertBoltState(t, bolt5Ready, bolt)

				AssertAny(t, boltLogger.clientMessages, func(logMsg string) bool {
					if strings.Contains(logMsg, "LOGON") {
						AssertStringContain(t, logMsg, "credentials")
						AssertStringNotContain(t, logMsg, expectedCredentials)
						return true
					}
					return false
				})

				wg.Done()
			}()
		}

		wg.Wait()
	})

	type txTimeoutTestCase struct {
		description string
		input       time.Duration
		output      int
		omitted     bool
	}

	txTimeoutTestCases := []txTimeoutTestCase{
		{
			description: "tx_timeout should not be present",
			input:       0,
			omitted:     true,
		},
		{
			description: "tx_timeout should round 1ns to 1ms",
			input:       time.Nanosecond,
			output:      1,
		},
		{
			description: "tx_timeout should round 999µs to 1ms",
			input:       time.Millisecond - time.Microsecond,
			output:      1,
		},
		{
			description: "tx_timeout should round 1ms to 1ms",
			input:       time.Millisecond,
			output:      1,
		},
		{
			description: "tx_timeout should round 1.001ms to 2ms",
			input:       time.Millisecond + time.Microsecond,
			output:      2,
		},
		{
			description: "tx_timeout should round 1.999ms to 2ms",
			input:       time.Millisecond*2 - time.Microsecond,
			output:      2,
		},
		{
			description: "tx_timeout should round 2ms to 2ms",
			input:       time.Millisecond * 2,
			output:      2,
		},
	}

	for _, test := range txTimeoutTestCases {
		outer.Run(test.description, func(t *testing.T) {
			tx := internalTx5{timeout: test.input}
			version := db.ProtocolVersion{Major: 5, Minor: 0}
			actual, ok := tx.toMeta(logger, "", version)["tx_timeout"]
			if test.omitted {
				if ok {
					t.Errorf("tx_timeout was present but should be omitted")
				}
				return
			}

			if ok {
				AssertIntEqual(t, int(actual.(int64)), test.output)
			} else {
				t.Errorf("missing tx_timout: expected %vms for value %v", test.input, test.output)
			}
		})
	}

	outer.Run("StreamSummary tests", func(t *testing.T) {
		// Test where both HadRecord and HadKey are false - omitted result
		t.Run("StreamSummary omitted result", func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
				srv.accept(5)
				// Simulate a run that returns no keys and no records
				srv.waitForRun(nil)
				srv.send(msgSuccess, map[string]any{"fields": []any{}})
				srv.send(msgSuccess, map[string]any{})
			})
			defer cleanup()
			defer bolt.Close(context.Background())

			stream, _ := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
			summary, err := bolt.Consume(context.Background(), stream)
			AssertNoError(t, err)

			// Validate StreamSummary fields
			AssertFalse(t, summary.StreamSummary.HadRecord)
			AssertFalse(t, summary.StreamSummary.HadKey)
		})

		// Test where both HadRecord and HadKey are true - success result
		t.Run("StreamSummary success result", func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
				srv.accept(5)
				// Simulate a run that returns keys and records
				srv.waitForRun(nil)
				srv.send(msgSuccess, map[string]any{"fields": []any{"name"}})
				srv.send(msgRecord, []any{"John Doe"})
				srv.send(msgSuccess, map[string]any{})
			})
			defer cleanup()
			defer bolt.Close(context.Background())

			stream, _ := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
			summary, err := bolt.Consume(context.Background(), stream)
			AssertNoError(t, err)

			// Validate StreamSummary fields
			AssertTrue(t, summary.StreamSummary.HadRecord)
			AssertTrue(t, summary.StreamSummary.HadKey)
		})

		// Test where HadRecord is false but HadKey is true - no data result
		t.Run("StreamSummary no data result", func(t *testing.T) {
			bolt, cleanup := connectToServer(t, func(srv *bolt5server) {
				srv.accept(5)
				// Simulate a run that returns keys but no records
				srv.waitForRun(nil)
				srv.send(msgSuccess, map[string]any{"fields": []any{"name"}})
				srv.send(msgSuccess, map[string]any{})
			})
			defer cleanup()
			defer bolt.Close(context.Background())

			stream, _ := bolt.Run(context.Background(), idb.Command{Cypher: "MATCH (n) RETURN n"}, idb.TxConfig{Mode: idb.ReadMode})
			summary, err := bolt.Consume(context.Background(), stream)
			AssertNoError(t, err)

			// Validate StreamSummary fields
			AssertFalse(t, summary.StreamSummary.HadRecord)
			AssertTrue(t, summary.StreamSummary.HadKey)
		})
	})
}
