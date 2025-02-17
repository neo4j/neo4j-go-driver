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

package neo4j

import (
	"context"
	"errors"
	"fmt"
	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/homedb"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type transactionFunc func(context.Context, ManagedTransactionWork, ...func(*TransactionConfig)) (any, error)
type transactionFuncApi func(session SessionWithContext) transactionFunc

var reAuthToken = &idb.ReAuthToken{FromSession: false, Manager: iauth.Token{Tokens: map[string]any{"scheme": "none"}}}

func TestSession(outer *testing.T) {
	var logger = log.ToVoid()
	var boltLogger log.BoltLogger = nil

	assertCleanSessionState := func(t *testing.T, sess *sessionWithContext) {
		if sess.explicitTx != nil {
			t.Errorf("Session should not be in tx mode")
		}
	}

	createSession := func() (*RouterFake, *PoolFake, *sessionWithContext) {
		ctx := context.Background()
		conf := Config{MaxTransactionRetryTime: 3 * time.Millisecond, MaxConnectionPoolSize: 100}
		router := RouterFake{}
		pool := PoolFake{}
		cache, _ := homedb.NewCache(100)
		sessConfig := SessionConfig{AccessMode: AccessModeRead, BoltLogger: boltLogger}
		sess := newSessionWithContext(ctx, &conf, sessConfig, &router, &pool, cache, logger, reAuthToken)
		sess.throttleTime = time.Millisecond * 1
		return &router, &pool, sess
	}

	createSessionFromConfig := func(sessConfig SessionConfig) (*RouterFake, *PoolFake, *sessionWithContext) {
		ctx := context.Background()
		conf := Config{MaxTransactionRetryTime: 3 * time.Millisecond}
		router := RouterFake{}
		pool := PoolFake{}
		cache, _ := homedb.NewCache(100)
		sess := newSessionWithContext(ctx, &conf, sessConfig, &router, &pool, cache, logger, reAuthToken)
		sess.throttleTime = time.Millisecond * 1
		return &router, &pool, sess
	}

	createSessionWithBookmarks := func(bookmarks Bookmarks) (*RouterFake, *PoolFake, *sessionWithContext) {
		sessConfig := SessionConfig{AccessMode: AccessModeRead, Bookmarks: bookmarks, BoltLogger: boltLogger}
		return createSessionFromConfig(sessConfig)
	}

	createSessionWithHomeDbGuess := func(ssrEnabled bool) (*RouterFake, *PoolFake, *sessionWithContext, *int, *int) {
		router, pool, sess := createSession()
		cacheKey := "DEFAULT"
		databaseName := "db1"

		sess.cache.SetEnabled(true)
		sess.cache.Set(cacheKey, databaseName)
		sess.homeDbGuess = cacheKey

		// Fake a routing table lookup for the guessed home database
		router.GetTableHook = func(guess string) *idb.RoutingTable {
			if guess == cacheKey {
				return &idb.RoutingTable{}
			}
			return nil
		}

		// Track number of borrow and return calls
		borrowCount := 0
		returnCount := 0

		pool.BorrowHook = func() (idb.Connection, error) {
			borrowCount++
			return &ConnFake{Alive: true, SsrEnabled: ssrEnabled}, nil
		}

		pool.ReturnHook = func() {
			returnCount++
		}
		return router, pool, sess, &borrowCount, &returnCount
	}

	tokenExpiredErr := &db.Neo4jError{Code: "Neo.ClientError.Security.TokenExpired", Msg: "oopsie whoopsie"}

	outer.Run("Transaction Functions", func(inner *testing.T) {
		// Checks that retries occur on database error and that it stops retrying after a certain
		// amount of time and that connections are returned to pool upon failure.
		inner.Run("Consistent transient error", func(t *testing.T) {
			_, pool, sess := createSession()
			numReturns := 0
			pool.ReturnHook = func() {
				numReturns++
			}
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			transientErr := &db.Neo4jError{Code: "Neo.TransientError.General.MemoryPoolOutOfMemoryError"}
			numRetries := 0
			_, err := sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				// Previous connection should be returned to pool since it failed
				if numRetries > 0 && numReturns != numRetries {
					t.Errorf("Should have returned previous connection to pool")
				}
				numRetries++
				return nil, transientErr
			})

			if numRetries < 2 {
				t.Errorf("Should have retried at least once but executed %d", numRetries)
			}
			AssertTrue(t, IsTransactionExecutionLimit(err))
			errL := err.(*TransactionExecutionLimit)
			assertErrorEq(t, transientErr, errL.Errors[len(errL.Errors)-1])
			assertCleanSessionState(t, sess)
		})

		// Checks that session is in clean state after connection fails to rollback.
		// "User" initiates rollback by letting the transaction function return a custom error.
		inner.Run("Failed rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			rollbackErr := errors.New("RollbackErrorFake")
			causeOfRollbackErr := errors.New("UserErrorFake")
			pool.BorrowConn = &ConnFake{Alive: true, TxRollbackErr: rollbackErr}
			numRetries := 0
			_, err := sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				numRetries++
				return nil, causeOfRollbackErr
			})
			if numRetries != 1 {
				t.Error("Should not retry on user error")
			}
			assertErrorEq(t, causeOfRollbackErr, err)
			assertCleanSessionState(t, sess)
		})

		// Check that session is in clean state after connection fails to commit.
		inner.Run("Failed commit", func(t *testing.T) {
			_, pool, sess := createSession()
			pool.BorrowConn = &ConnFake{Alive: false, TxCommitErr: io.EOF}
			numRetries := 0
			_, err := sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				numRetries++
				return nil, nil
			})
			if numRetries != 1 {
				t.Error("Should not retry on commit error")
			}
			// Should not be a TransactionExecutionLimitError here
			AssertTrue(t, IsConnectivityError(err))
			AssertSameType(t, err.(*ConnectivityError).Inner, &errorutil.CommitFailedDeadError{})
			assertCleanSessionState(t, sess)
		})

		inner.Run("Retrieves default database name for impersonated user", func(t *testing.T) {
			sessConfig := SessionConfig{ImpersonatedUser: "me"}
			router, pool, sess := createSessionFromConfig(sessConfig)
			conn := &ConnFake{}
			pool.BorrowConn = conn
			numDefaultDbLookups := 0
			const mydb = "mydb"
			router.GetNameOfDefaultDbHook = func(user string) (string, error) {
				numDefaultDbLookups++
				return mydb, nil
			}
			router.GetOrUpdateWritersHook = func(_ func(context.Context) ([]string, error), database string) ([]string, error) {
				AssertStringEqual(t, mydb, database)
				return []string{"aserver"}, nil
			}

			sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, nil
			})
			_, err := sess.BeginTransaction(context.Background())
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
		})

		transactionFunctions := map[string]transactionFuncApi{
			"read tx func":  func(s SessionWithContext) transactionFunc { return s.ExecuteRead },
			"write tx func": func(s SessionWithContext) transactionFunc { return s.ExecuteWrite },
		}

		for name, txFuncApi := range transactionFunctions {
			inner.Run(fmt.Sprintf("Implicitly rolls back when a %s panics without retry", name), func(t *testing.T) {
				_, pool, sess := createSessionFromConfig(SessionConfig{})
				pool.BorrowConn = &ConnFake{Alive: true}
				poolReturnCalled := 0
				pool.ReturnHook = func() {
					poolReturnCalled++
				}
				panicBubblesUp := false
				func() {
					defer func() {
						panicBubblesUp = recover() != nil
					}()
					_, _ = txFuncApi(sess)(context.Background(), func(tx ManagedTransaction) (any, error) {
						panic("oopsie")
					})
				}()
				AssertIntEqual(t, poolReturnCalled, 1)
				AssertTrue(t, panicBubblesUp)
			})
		}
	})

	outer.Run("Bookmarking", func(inner *testing.T) {
		inner.Run("Initial bookmarks are returned from LastBookmarks", func(t *testing.T) {
			_, _, sess := createSessionWithBookmarks(BookmarksFromRawValues("b1", "b2"))
			AssertDeepEquals(t, sess.LastBookmarks(), BookmarksFromRawValues("b1", "b2"))
		})

		inner.Run("Initial bookmarks are used and cleaned up before usage", func(t *testing.T) {
			dirtyBookmarks := BookmarksFromRawValues("", "b1", "", "b2", "")
			cleanBookmarks := BookmarksFromRawValues("b1", "b2")
			_, pool, sess := createSessionWithBookmarks(dirtyBookmarks)
			err := errors.New("make all fail")
			conn := &ConnFake{Alive: true, RunErr: err, TxBeginErr: err}
			pool.BorrowConn = conn

			sess.Run(context.Background(), "cypher", nil)
			sess.BeginTransaction(context.Background())
			sess.ExecuteRead(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, errors.New("something")
			})
			sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, errors.New("something")
			})
			AssertLen(t, conn.RecordedTxs, 4)
			for _, rtx := range conn.RecordedTxs {
				AssertEqualsInAnyOrder(t, rtx.Bookmarks, cleanBookmarks)
			}
		})

		inner.Run("LastBookmarks is empty when no initial bookmark", func(t *testing.T) {
			_, _, sess := createSession()
			AssertLen(t, sess.LastBookmarks(), 0)
		})
	})

	outer.Run("Run", func(inner *testing.T) {
		// Checks that chained Run results are buffered and that bookmarks are retrieved for
		// those and that a Consume on the last result also gives the appropriate bookmark.
		inner.Run("Chained and consume", func(t *testing.T) {
			_, pool, sess := createSession()
			bufferCalls := 0
			consumeCalls := 0
			conn := &ConnFake{Alive: true}
			conn.BufferHook = func() {
				bufferCalls++
				conn.Bookm = fmt.Sprintf("buffer-%d", bufferCalls)
			}
			conn.ConsumeHook = func() {
				consumeCalls++
				conn.Bookm = fmt.Sprintf("consume-%d", consumeCalls)
				conn.ConsumeSum = &db.Summary{}
			}
			pool.BorrowConn = conn

			sess.Run(context.Background(), "cypher", nil)
			AssertIntEqual(t, bufferCalls, 0)
			AssertLen(t, sess.LastBookmarks(), 0)
			// Should call Buffer on connection to ensure that first Run is buffered and
			// it's bookmark retrieved
			sess.Run(context.Background(), "cypher", nil)
			AssertDeepEquals(t, BookmarksToRawValues(sess.LastBookmarks()), []string{"buffer-1"})
			result, _ := sess.Run(context.Background(), "cypher", nil)
			AssertDeepEquals(t, BookmarksToRawValues(sess.LastBookmarks()), []string{"buffer-2"})
			// And finally consuming the last result should give a new bookmark
			AssertIntEqual(t, consumeCalls, 0)
			result.Consume(context.Background())
			AssertDeepEquals(t, BookmarksToRawValues(sess.LastBookmarks()), []string{"consume-1"})
		})

		inner.Run("Pending and invoke tx function", func(t *testing.T) {
			// Checks that a pending Run (not consumed or iterated) gets buffered and it's
			// bookmark is used when starting a transaction.
			_, pool, sess := createSession()
			bufferCalls := 0
			conn := &ConnFake{Alive: true}
			conn.BufferHook = func() {
				bufferCalls++
				conn.Bookm = fmt.Sprintf("%d", bufferCalls)
			}
			pool.BorrowConn = conn
			sess.Run(context.Background(), "cypher", nil)
			AssertIntEqual(t, bufferCalls, 0)
			// Run transaction function. assumes code is shared between ExecuteRead/ExecuteWrite
			sess.ExecuteRead(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, errors.New("somehting")
			})
			AssertLen(t, conn.RecordedTxs, 2)
			rtx := conn.RecordedTxs[1]
			if !reflect.DeepEqual([]string{"1"}, rtx.Bookmarks) {
				t.Errorf("Using unclean or no bookmarks: %+v", rtx)
			}
			AssertDeepEquals(t, BookmarksToRawValues(sess.LastBookmarks()), []string{"1"})
			AssertIntEqual(t, bufferCalls, 1)
		})

		inner.Run("Pending and start tx", func(t *testing.T) {
			// Checks that a pending Run (not consumed or iterated) gets buffered and it's
			// bookmark is used when starting a transaction.
			_, pool, sess := createSession()
			bufferCalls := 0
			conn := &ConnFake{Alive: true}
			conn.BufferHook = func() {
				bufferCalls++
				conn.Bookm = fmt.Sprintf("%d", bufferCalls)
			}
			pool.BorrowConn = conn
			sess.Run(context.Background(), "cypher", nil)
			AssertIntEqual(t, bufferCalls, 0)
			// Begin a transaction
			sess.BeginTransaction(context.Background())
			AssertLen(t, conn.RecordedTxs, 2)
			rtx := conn.RecordedTxs[1]
			if !reflect.DeepEqual([]string{"1"}, rtx.Bookmarks) {
				t.Errorf("Using unclean or no bookmarks: %+v", rtx)
			}
			AssertDeepEquals(t, BookmarksToRawValues(sess.LastBookmarks()), []string{"1"})
			AssertIntEqual(t, bufferCalls, 1)
		})

		inner.Run("While in tx", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			// Begin a transaction on the session
			_, err := sess.BeginTransaction(context.Background())
			AssertNoError(t, err)
			// Trying to use Run should cause a usage error
			_, err = sess.Run(context.Background(), "cypher", nil)
			assertUsageError(t, err)
		})

		inner.Run("Retrieves default database name for impersonated user", func(t *testing.T) {
			sessConfig := SessionConfig{ImpersonatedUser: "me"}
			router, pool, sess := createSessionFromConfig(sessConfig)
			conn := &ConnFake{}
			pool.BorrowConn = conn
			numDefaultDbLookups := 0
			const mydb = "mydb"
			router.GetNameOfDefaultDbHook = func(user string) (string, error) {
				numDefaultDbLookups++
				return mydb, nil
			}
			router.GetOrUpdateReadersHook = func(_ func(context.Context) ([]string, error), database string) ([]string, error) {
				AssertStringEqual(t, mydb, database)
				return []string{"aserver"}, nil
			}

			res, err := sess.Run(context.Background(), "cypher", nil)
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
			res.Consume(context.Background())

			// Triggering another operation on the same session should NOT look up again
			conn = &ConnFake{}
			pool.BorrowConn = conn
			_, err = sess.Run(context.Background(), "cypher", nil)
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
		})

		inner.Run("Token expiration in session run after errored connection acquisition", func(t *testing.T) {
			_, pool, sess := createSession()
			pool.BorrowErr = tokenExpiredErr

			_, err := sess.Run(context.Background(), "cypher", map[string]any{})

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after run", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, RunErr: tokenExpiredErr}
			pool.BorrowConn = conn

			_, err := sess.Run(context.Background(), "cypher", map[string]any{})

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after result collect call", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, Nexts: []Next{{Err: tokenExpiredErr}}}
			pool.BorrowConn = conn

			result, err := sess.Run(context.Background(), "cypher", map[string]any{})
			AssertNil(t, err)
			_, err = result.Collect(context.Background())

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after result consume call", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, ConsumeErr: tokenExpiredErr}
			pool.BorrowConn = conn

			result, err := sess.Run(context.Background(), "cypher", map[string]any{})
			AssertNil(t, err)
			_, err = result.Consume(context.Background())

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after result consume next and err call", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, Nexts: []Next{{Err: tokenExpiredErr}}}
			pool.BorrowConn = conn

			result, err := sess.Run(context.Background(), "cypher", map[string]any{})
			AssertNil(t, err)
			_ = result.Next(context.Background())
			err = result.Err()

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after result single record extraction", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, Nexts: []Next{{Err: tokenExpiredErr}}}
			pool.BorrowConn = conn

			result, err := sess.Run(context.Background(), "cypher", map[string]any{})
			AssertNil(t, err)
			_, err = result.Single(context.Background())

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after write transaction function", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn

			_, err := sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, tokenExpiredErr
			})

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after read transaction function", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn

			_, err := sess.ExecuteRead(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, tokenExpiredErr
			})

			assertTokenExpiredError(t, err)
		})
	})

	outer.Run("Explicit transaction", func(inner *testing.T) {
		inner.Run("While already in tx", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			// Begin a transaction on the session
			_, err := sess.BeginTransaction(context.Background())
			AssertNoError(t, err)
			// Trying to begin a new transaction should cause a usage error
			_, err = sess.BeginTransaction(context.Background())
			assertUsageError(t, err)
		})

		inner.Run("Commit propagates bookmark", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			bookmark := "magic"
			conn.TxCommitHook = func() { conn.Bookm = bookmark }
			pool.BorrowConn = conn
			// Begin and commit a transaction on the session
			tx, _ := sess.BeginTransaction(context.Background())
			tx.Commit(context.Background())
			AssertDeepEquals(t, BookmarksToRawValues(sess.LastBookmarks()), []string{bookmark})
			// The bookmark should be used in next transaction
			sess.BeginTransaction(context.Background())
			AssertLen(t, conn.RecordedTxs, 2)
			rtx := conn.RecordedTxs[1]
			if !reflect.DeepEqual([]string{bookmark}, rtx.Bookmarks) {
				t.Errorf("Not using the correct bookmark")
			}
		})

		inner.Run("Rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			// Begin a transaction on the session
			tx, _ := sess.BeginTransaction(context.Background())
			tx.Rollback(context.Background())
			// Trying begin a new transaction should succeed after rollback
			_, err := sess.BeginTransaction(context.Background())
			AssertNoError(t, err)
		})

		inner.Run("Retrieves default database name for impersonated user", func(t *testing.T) {
			sessConfig := SessionConfig{ImpersonatedUser: "me"}
			router, pool, sess := createSessionFromConfig(sessConfig)
			conn := &ConnFake{}
			pool.BorrowConn = conn
			numDefaultDbLookups := 0
			const mydb = "mydb"
			router.GetNameOfDefaultDbHook = func(user string) (string, error) {
				numDefaultDbLookups++
				return mydb, nil
			}
			router.GetOrUpdateReadersHook = func(_ func(context.Context) ([]string, error), database string) ([]string, error) {
				AssertStringEqual(t, mydb, database)
				return []string{"aserver"}, nil
			}

			_, err := sess.BeginTransaction(context.Background())
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
		})

		inner.Run("Token expiration after transaction begin", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, TxBeginErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.Background())

			AssertNil(t, tx)
			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after transaction run", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, RunTxErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.Background())
			AssertNil(t, err)
			_, err = tx.Run(context.Background(), "cypher", map[string]any{})

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after transaction commit", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, TxCommitErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.Background())
			AssertNil(t, err)
			_, err = tx.Run(context.Background(), "cypher", map[string]any{})
			AssertNil(t, err)
			err = tx.Commit(context.Background())

			assertTokenExpiredError(t, err)
		})

		inner.Run("Token expiration after transaction rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, TxRollbackErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.Background())
			AssertNil(t, err)
			_, err = tx.Run(context.Background(), "cypher", map[string]any{})
			AssertNil(t, err)
			err = tx.Rollback(context.Background())

			assertTokenExpiredError(t, err)
		})
	})

	outer.Run("GetServerInfo", func(inner *testing.T) {

		inner.Run("Retrieves info from first borrowed connection", func(t *testing.T) {
			borrowCalled := false
			ctx := context.Background()
			_, pool, session := createSession()
			defer session.Close(ctx)
			pool.BorrowHook = func() (idb.Connection, error) {
				if borrowCalled {
					inner.Errorf("expected only 1 call to borrow, got more")
				}
				result := &ConnFake{
					Name: "home",
					ConnectionVersion: db.ProtocolVersion{
						Major: 5,
						Minor: 0,
					},
					Alive:              true,
					DatabaseName:       "neo4j",
					ServerVersionValue: "smith",
				}
				borrowCalled = true
				return result, nil
			}

			info, err := session.getServerInfo(ctx)

			AssertNoError(t, err)
			AssertDeepEquals(t, info.ProtocolVersion().Major, 5)
			AssertDeepEquals(t, info.ProtocolVersion().Minor, 0)
			AssertDeepEquals(t, info.Agent(), "smith")
			AssertDeepEquals(t, info.Address(), "home")
		})

		inner.Run("Fails if home DB resolution fails", func(t *testing.T) {
			ctx := context.Background()
			router, _, session := createSession()
			defer session.Close(ctx)
			expectedErr := fmt.Errorf("home db err")
			router.GetNameOfDefaultDbHook = func(string) (string, error) {
				return "", expectedErr
			}

			_, err := session.getServerInfo(ctx)

			assertErrorEq(t, err, expectedErr)
		})

		inner.Run("Fails if servers retrieval fails", func(t *testing.T) {
			ctx := context.Background()
			router, _, session := createSession()
			defer session.Close(ctx)
			expectedErr := fmt.Errorf("server retrieval err")
			router.GetOrUpdateReadersHook = func(func(context.Context) ([]string, error), string) ([]string, error) {
				return nil, expectedErr
			}

			_, err := session.getServerInfo(ctx)

			assertErrorEq(t, err, expectedErr)
		})

		inner.Run("Fails if connection borrow fails", func(t *testing.T) {
			ctx := context.Background()
			_, pool, session := createSession()
			defer session.Close(ctx)
			expectedErr := fmt.Errorf("connection borrow err")
			pool.BorrowErr = expectedErr

			_, err := session.getServerInfo(ctx)

			assertErrorEq(t, err, expectedErr)
		})
	})

	outer.Run("Close", func(ct *testing.T) {
		ct.Run("Cleans up connection pool async", func(t *testing.T) {
			_, pool, sess := createSession()
			wg := sync.WaitGroup{}
			wg.Add(1)
			pool.CleanUpHook = func() {
				wg.Done()
			}
			sess.Close(context.Background())
			wg.Wait()
		})

		ct.Run("Cleans up router async", func(t *testing.T) {
			router, _, sess := createSession()
			wg := sync.WaitGroup{}
			wg.Add(1)
			router.CleanUpHook = func() {
				wg.Done()
			}
			sess.Close(context.Background())
			wg.Wait()
		})

		ct.Run("Does not put back connection twice to the pool", func(inner *testing.T) {
			type testCase struct {
				name       string
				completeTx func(context.Context, SessionWithContext, ExplicitTransaction) error
			}
			cases := []testCase{
				{
					name: "session close",
					completeTx: func(ctx context.Context, session SessionWithContext, _ ExplicitTransaction) error {
						return session.Close(ctx)
					},
				},
				{
					name: "tx commit",
					completeTx: func(ctx context.Context, _ SessionWithContext, transaction ExplicitTransaction) error {
						return transaction.Commit(ctx)
					},
				},
				{
					name: "tx rollback",
					completeTx: func(ctx context.Context, _ SessionWithContext, transaction ExplicitTransaction) error {
						return transaction.Rollback(ctx)
					},
				},
				{
					name: "tx close",
					completeTx: func(ctx context.Context, _ SessionWithContext, transaction ExplicitTransaction) error {
						return transaction.Close(ctx)
					},
				},
			}

			for _, test := range cases {
				inner.Run(fmt.Sprintf("after %s", test.name), func(t *testing.T) {
					_, pool, session := createSession()
					conn := &ConnFake{Alive: true, RunTxErr: errors.New("invalid transaction handle")}
					poolReturnsCalls := 0
					pool.BorrowConn = conn
					pool.ReturnHook = func() {
						poolReturnsCalls++
					}
					tx, err := session.BeginTransaction(ctx)

					AssertNoError(t, err)
					AssertNoError(t, test.completeTx(ctx, session, tx))
					AssertIntEqual(t, poolReturnsCalls, 1)
					_, err = tx.Run(ctx, "RETURN 42", nil)
					AssertErrorMessageContains(t, err, "cannot use this transaction")
					AssertIntEqual(t, poolReturnsCalls, 1) // pool.Return must not be called again
				})
			}
		})

		ct.Run("Does not put back connection twice to the pool after second failed run", func(t *testing.T) {
			_, pool, session := createSession()
			runTxErr := errors.New("oopsie")
			conn := &ConnFake{Alive: true, RunTxErr: runTxErr}
			poolReturnsCalls := 0
			pool.BorrowConn = conn
			pool.ReturnHook = func() {
				poolReturnsCalls++
			}
			tx, err := session.BeginTransaction(context.Background())

			AssertNoError(t, err)
			_, err = tx.Run(ctx, "RETURN 42", nil)
			AssertDeepEquals(t, err, runTxErr)
			AssertIntEqual(t, poolReturnsCalls, 1)
			_, err = tx.Run(ctx, "RETURN 42", nil)
			AssertErrorMessageContains(t, err, "cannot use this transaction")
			AssertIntEqual(t, poolReturnsCalls, 1) // pool.Return must not be called again
		})

		ct.Run("Run returns error if session is closed", func(t *testing.T) {
			_, _, sess := createSession()
			sess.Close(context.Background())
			_, err := sess.Run(context.Background(), "cypher", nil)
			AssertErrorMessageContains(t, err, "Operation attempted on a closed session")
		})

		ct.Run("BeginTransaction returns error if session is closed", func(t *testing.T) {
			_, _, sess := createSession()
			sess.Close(context.Background())
			_, err := sess.BeginTransaction(context.Background())
			AssertErrorMessageContains(t, err, "Operation attempted on a closed session")
		})

		ct.Run("ExecuteRead returns error if session is closed", func(t *testing.T) {
			_, _, sess := createSession()
			sess.Close(context.Background())
			_, err := sess.ExecuteRead(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, nil
			})
			AssertErrorMessageContains(t, err, "Operation attempted on a closed session")
		})

		ct.Run("ExecuteWrite returns error if session is closed", func(t *testing.T) {
			_, _, sess := createSession()
			sess.Close(context.Background())
			_, err := sess.ExecuteWrite(context.Background(), func(tx ManagedTransaction) (any, error) {
				return nil, nil
			})
			AssertErrorMessageContains(t, err, "Operation attempted on a closed session")
		})

		ct.Run("Session returns nil if closed multiple times", func(t *testing.T) {
			_, _, sess := createSession()
			err := sess.Close(context.Background())
			AssertNoError(t, err)
			err = sess.Close(context.Background())
			AssertNoError(t, err)
		})
	})

	outer.Run("TestGetConnection_HomeDatabaseGuess", func(inner *testing.T) {

		inner.Run("Returns error on borrow failure", func(t *testing.T) {
			_, pool, sess := createSession()
			pool.BorrowErr = errors.New("connection borrow failure")
			_, err := sess.getConnection(context.Background(), idb.ReadMode, 1*time.Minute)
			AssertErrorMessageContains(t, err, "connection borrow failure")
		})

		inner.Run("Borrows once, does not return connection if SSR enabled & home DB guess used", func(t *testing.T) {
			_, _, sess, borrowCount, returnCount := createSessionWithHomeDbGuess(true)

			_, err := sess.getConnection(context.Background(), idb.ReadMode, 1*time.Minute)

			AssertNoError(t, err)
			AssertIntEqual(t, *borrowCount, 1) // Should borrow only once
			AssertIntEqual(t, *returnCount, 0) // Should not return connection
		})

		inner.Run("Borrows twice, returns one connection if SSR disabled & home DB guess used", func(t *testing.T) {
			_, _, sess, borrowCount, returnCount := createSessionWithHomeDbGuess(false)

			_, err := sess.getConnection(context.Background(), idb.ReadMode, 1*time.Minute)

			AssertNoError(t, err)
			AssertIntEqual(t, *borrowCount, 2) // Should borrow twice due to SSR being disabled on connection
			AssertIntEqual(t, *returnCount, 1) // Should return the first connection
		})
	})

	outer.Run("TestGetServerList_UsedHomeDatabaseGuess", func(inner *testing.T) {

		inner.Run("Returns false when DatabaseName is set", func(t *testing.T) {
			_, _, sess := createSession()
			sess.config.DatabaseName = "my_database"

			_, usedHomeDbGuess, err := sess.getServerList(context.Background(), idb.ReadMode)

			AssertNoError(t, err)
			AssertFalse(t, usedHomeDbGuess)
		})

		inner.Run("Returns true when homeDbGuess is used", func(t *testing.T) {
			_, _, sess, _, _ := createSessionWithHomeDbGuess(true)

			_, usedHomeDbGuess, err := sess.getServerList(context.Background(), idb.ReadMode)

			AssertNoError(t, err)
			AssertTrue(t, usedHomeDbGuess)
		})

		inner.Run("Returns false when homeDbGuess is not set", func(t *testing.T) {
			_, _, sess, _, _ := createSessionWithHomeDbGuess(true)
			sess.homeDbGuess = ""

			_, usedHomeDbGuess, err := sess.getServerList(context.Background(), idb.ReadMode)

			AssertNoError(t, err)
			AssertFalse(t, usedHomeDbGuess)
		})

		inner.Run("Returns false when homeDbGuess is set but cache is disabled", func(t *testing.T) {
			_, _, sess, _, _ := createSessionWithHomeDbGuess(true)
			sess.cache.SetEnabled(false)

			_, usedHomeDbGuess, err := sess.getServerList(context.Background(), idb.ReadMode)

			AssertNoError(t, err)
			AssertFalse(t, usedHomeDbGuess)
		})
	})
}

func assertTokenExpiredError(t *testing.T, err error) {
	t.Helper()
	AssertSameType(t, err, &TokenExpiredError{})
	AssertErrorMessageContains(t, err, "Neo.ClientError.Security.TokenExpired")
	AssertErrorMessageContains(t, err, "oopsie whoopsie")
}
