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

package neo4j

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/retry"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

func TestSession(st *testing.T) {
	logger := log.Console{Errors: true, Infos: true, Warns: true, Debugs: true}
	boltLogger := log.ConsoleBoltLogger{}

	assertCleanSessionState := func(t *testing.T, sess *session) {
		if sess.txExplicit != nil {
			t.Errorf("Session should not be in tx mode")
		}
	}

	createSession := func() (*RouterFake, *PoolFake, *session) {
		conf := Config{MaxTransactionRetryTime: 3 * time.Millisecond}
		router := RouterFake{}
		pool := PoolFake{}
		sessConfig := SessionConfig{AccessMode: AccessModeRead, BoltLogger: &boltLogger}
		sess := newSession(&conf, sessConfig, &router, &pool, &logger)
		sess.throttleTime = time.Millisecond * 1
		return &router, &pool, sess
	}

	createSessionFromConfig := func(sessConfig SessionConfig) (*RouterFake, *PoolFake, *session) {
		conf := Config{MaxTransactionRetryTime: 3 * time.Millisecond}
		router := RouterFake{}
		pool := PoolFake{}
		sess := newSession(&conf, sessConfig, &router, &pool, &logger)
		sess.throttleTime = time.Millisecond * 1
		return &router, &pool, sess
	}

	createSessionWithBookmarks := func(bookmarks []string) (*RouterFake, *PoolFake, *session) {
		sessConfig := SessionConfig{AccessMode: AccessModeRead, Bookmarks: bookmarks, BoltLogger: &boltLogger}
		return createSessionFromConfig(sessConfig)
	}

	tokenExpiredErr := &db.Neo4jError{Code: "Neo.ClientError.Security.TokenExpired", Msg: "oopsie whoopsie"}

	st.Run("Retry mechanism", func(rt *testing.T) {
		// Checks that retries occur on database error and that it stops retrying after a certain
		// amount of time and that connections are returned to pool upon failure.
		rt.Run("Consistent transient error", func(t *testing.T) {
			_, pool, sess := createSession()
			numReturns := 0
			pool.ReturnHook = func() {
				numReturns++
			}
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			transientErr := &db.Neo4jError{Code: "Neo.TransientError.General.MemoryPoolOutOfMemoryError"}
			numRetries := 0
			_, err := sess.WriteTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
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
		rt.Run("Failed rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			rollbackErr := errors.New("RollbackErrorFake")
			causeOfRollbackErr := errors.New("UserErrorFake")
			pool.BorrowConn = &ConnFake{Alive: true, TxRollbackErr: rollbackErr}
			numRetries := 0
			_, err := sess.WriteTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				numRetries++
				return nil, causeOfRollbackErr
			})
			if numRetries != 1 {
				t.Error("Should not retry on user error")
			}
			assertErrorEq(t, causeOfRollbackErr, err)
			assertCleanSessionState(t, sess)
		})

		// Check that sesssion is in clean state after connection fails to commit.
		rt.Run("Failed commit", func(t *testing.T) {
			_, pool, sess := createSession()
			pool.BorrowConn = &ConnFake{Alive: false, TxCommitErr: io.EOF}
			numRetries := 0
			_, err := sess.WriteTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				numRetries++
				return nil, nil
			})
			if numRetries != 1 {
				t.Error("Should not retry on commit error")
			}
			// Should not be a TransactionExecutionLimitError here
			AssertTrue(t, IsConnectivityError(err))
			AssertSameType(t, err.(*ConnectivityError).inner, &retry.CommitFailedDeadError{})
			assertCleanSessionState(t, sess)
		})

		rt.Run("Retrieves default database name for impersonated user", func(t *testing.T) {
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
			router.WritersHook = func(bookmarks []string, database string) ([]string, error) {
				AssertStringEqual(t, mydb, database)
				return []string{"aserver"}, nil
			}

			sess.WriteTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				return nil, nil
			})
			_, err := sess.BeginTransaction(context.TODO())
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
		})
	})

	st.Run("Bookmarking", func(bt *testing.T) {
		bt.Run("Initial bookmark is used for LastBookmark", func(t *testing.T) {
			_, _, sess := createSessionWithBookmarks([]string{"b1", "b2"})
			AssertStringEqual(t, sess.LastBookmark(), "b2")
		})

		bt.Run("Initial bookmarks are used and cleaned up before usage", func(t *testing.T) {
			dirtyBookmarks := []string{"", "b1", "", "b2", ""}
			cleanBookmarks := []string{"b1", "b2"}
			_, pool, sess := createSessionWithBookmarks(dirtyBookmarks)
			err := errors.New("make all fail")
			conn := &ConnFake{Alive: true, RunErr: err, TxBeginErr: err}
			pool.BorrowConn = conn

			sess.Run(context.TODO(), "cypher", nil)
			sess.BeginTransaction(context.TODO())
			sess.ReadTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				return nil, errors.New("something")
			})
			sess.WriteTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				return nil, errors.New("something")
			})
			AssertLen(t, conn.RecordedTxs, 4)
			for _, rtx := range conn.RecordedTxs {
				if !reflect.DeepEqual(cleanBookmarks, rtx.Bookmarks) {
					t.Errorf("Using unclean or no bookmarks: %+v", rtx)
				}
			}
		})

		bt.Run("LastBookmark is empty when no initial bookmark", func(t *testing.T) {
			_, _, sess := createSession()
			AssertStringEqual(t, sess.LastBookmark(), "")
		})
	})

	st.Run("Run", func(bt *testing.T) {
		bt.Run("Forces reset on acquired connection", func(t *testing.T) {
			_, pool, sess := createSession()
			forceResetCalls := 0
			connection := &ConnFake{
				ForceResetHook: func() error {
					forceResetCalls++
					return nil
				},
			}
			pool.BorrowConn = connection

			_, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})
			AssertNoError(t, err)
			AssertIntEqual(t, forceResetCalls, 1)
		})

		bt.Run("Picks connection from the pool until force-reset succeeds", func(t *testing.T) {
			_, pool, sess := createSession()
			forceResetCalls := 0
			connection := &ConnFake{
				ForceResetHook: func() error {
					forceResetCalls++
					if forceResetCalls == 1 {
						return errors.New("force-reset failure")
					}
					return nil
				},
			}
			pool.BorrowConn = connection

			_, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})
			AssertNoError(t, err)
			AssertIntEqual(t, forceResetCalls, 2)
		})

		// Checks that chained Run results are buffered and that bookmarks are retrieved for
		// those and that a Consume on the last result also gives the appropriate bookmark.
		bt.Run("Chained and consume", func(t *testing.T) {
			_, pool, sess := createSession()
			bufferCalls := 0
			consumeCalls := 0
			conn := &ConnFake{Alive: true}
			conn.BufferHook = func() {
				bufferCalls++
				conn.Bookm = fmt.Sprintf("%d", bufferCalls)
			}
			conn.ConsumeHook = func() {
				consumeCalls++
				conn.Bookm = fmt.Sprintf("%d", consumeCalls)
				conn.ConsumeSum = &db.Summary{}
			}
			pool.BorrowConn = conn

			sess.Run(context.TODO(), "cypher", nil)
			AssertIntEqual(t, bufferCalls, 0)
			AssertStringEqual(t, sess.LastBookmark(), "")
			// Should call Buffer on connection to ensure that first Run is buffered and
			// it's bookmark retrieved
			sess.Run(context.TODO(), "cypher", nil)
			AssertStringEqual(t, sess.LastBookmark(), "1")
			result, _ := sess.Run(context.TODO(), "cypher", nil)
			AssertStringEqual(t, sess.LastBookmark(), "2")
			// And finally consuming the last result should give a new bookmark
			AssertIntEqual(t, consumeCalls, 0)
			result.Consume()
			AssertStringEqual(t, sess.LastBookmark(), "1")
		})

		bt.Run("Pending and invoke tx function", func(t *testing.T) {
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
			sess.Run(context.TODO(), "cypher", nil)
			AssertIntEqual(t, bufferCalls, 0)
			// Run transaction function. assumes code is shared between ReadTransaction/WriteTransaction
			sess.ReadTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				return nil, errors.New("somehting")
			})
			AssertLen(t, conn.RecordedTxs, 2)
			rtx := conn.RecordedTxs[1]
			if !reflect.DeepEqual([]string{"1"}, rtx.Bookmarks) {
				t.Errorf("Using unclean or no bookmarks: %+v", rtx)
			}
			AssertStringEqual(t, sess.LastBookmark(), "1")
			AssertIntEqual(t, bufferCalls, 1)
		})

		bt.Run("Pending and start tx", func(t *testing.T) {
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
			sess.Run(context.TODO(), "cypher", nil)
			AssertIntEqual(t, bufferCalls, 0)
			// Begin a transaction
			sess.BeginTransaction(context.TODO())
			AssertLen(t, conn.RecordedTxs, 2)
			rtx := conn.RecordedTxs[1]
			if !reflect.DeepEqual([]string{"1"}, rtx.Bookmarks) {
				t.Errorf("Using unclean or no bookmarks: %+v", rtx)
			}
			AssertStringEqual(t, sess.LastBookmark(), "1")
			AssertIntEqual(t, bufferCalls, 1)
		})

		bt.Run("While in tx", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			// Begin a transaction on the session
			_, err := sess.BeginTransaction(context.TODO())
			AssertNoError(t, err)
			// Trying to use Run should cause a usage error
			_, err = sess.Run(context.TODO(), "cypher", nil)
			assertUsageError(t, err)
		})

		bt.Run("Retrieves default database name for impersonated user", func(t *testing.T) {
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
			router.ReadersHook = func(bookmarks []string, database string) ([]string, error) {
				AssertStringEqual(t, mydb, database)
				return []string{"aserver"}, nil
			}

			res, err := sess.Run(context.TODO(), "cypher", nil)
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
			res.Consume()

			// Triggering another operation on the same session should NOT look up again
			conn = &ConnFake{}
			pool.BorrowConn = conn
			_, err = sess.Run(context.TODO(), "cypher", nil)
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
		})

		bt.Run("Token expiration in session run after errored connection acquisition", func(t *testing.T) {
			_, pool, sess := createSession()
			pool.BorrowErr = tokenExpiredErr

			_, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after run", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, RunErr: tokenExpiredErr}
			pool.BorrowConn = conn

			_, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after result collect call", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, Nexts: []Next{{Err: tokenExpiredErr}}}
			pool.BorrowConn = conn

			result, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})
			AssertNil(t, err)
			_, err = result.Collect()

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after result consume call", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, ConsumeErr: tokenExpiredErr}
			pool.BorrowConn = conn

			result, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})
			AssertNil(t, err)
			_, err = result.Consume()

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after result consume next and err call", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, Nexts: []Next{{Err: tokenExpiredErr}}}
			pool.BorrowConn = conn

			result, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})
			AssertNil(t, err)
			_ = result.Next()
			err = result.Err()

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after result single record extraction", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, Nexts: []Next{{Err: tokenExpiredErr}}}
			pool.BorrowConn = conn

			result, err := sess.Run(context.TODO(), "cypher", map[string]interface{}{})
			AssertNil(t, err)
			_, err = result.Single()

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after write transaction function", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn

			_, err := sess.WriteTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				return nil, tokenExpiredErr
			})

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after read transaction function", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn

			_, err := sess.ReadTransaction(context.TODO(), func(tx Transaction) (interface{}, error) {
				return nil, tokenExpiredErr
			})

			assertTokenExpiredError(t, err)
		})
	})

	st.Run("Explicit transaction", func(bt *testing.T) {
		bt.Run("While already in tx", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			// Begin a transaction on the session
			_, err := sess.BeginTransaction(context.TODO())
			AssertNoError(t, err)
			// Trying to begin a new transaction should cause a usage error
			_, err = sess.BeginTransaction(context.TODO())
			assertUsageError(t, err)
		})

		bt.Run("Commit propagates bookmark", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			bookmark := "magic"
			conn.TxCommitHook = func() { conn.Bookm = bookmark }
			pool.BorrowConn = conn
			// Begin and commit a transaction on the session
			tx, _ := sess.BeginTransaction(context.TODO())
			tx.Commit()
			AssertStringEqual(t, sess.LastBookmark(), bookmark)
			// The bookmark should be used in next transaction
			tx, _ = sess.BeginTransaction(context.TODO())
			AssertLen(t, conn.RecordedTxs, 2)
			rtx := conn.RecordedTxs[1]
			if !reflect.DeepEqual([]string{bookmark}, rtx.Bookmarks) {
				t.Errorf("Not using the correct bookmark")
			}
		})

		bt.Run("Rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true}
			pool.BorrowConn = conn
			// Begin a transaction on the session
			tx, _ := sess.BeginTransaction(context.TODO())
			tx.Rollback()
			// Trying begin a new transaction should succeed after rollback
			_, err := sess.BeginTransaction(context.TODO())
			AssertNoError(t, err)
		})

		bt.Run("Retrieves default database name for impersonated user", func(t *testing.T) {
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
			router.ReadersHook = func(bookmarks []string, database string) ([]string, error) {
				AssertStringEqual(t, mydb, database)
				return []string{"aserver"}, nil
			}

			_, err := sess.BeginTransaction(context.TODO())
			AssertNoError(t, err)
			AssertStringEqual(t, mydb, conn.DatabaseName)
			AssertIntEqual(t, numDefaultDbLookups, 1)
		})

		bt.Run("Token expiration after transaction begin", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, TxBeginErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.TODO())

			AssertNil(t, tx)
			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after transaction run", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, RunTxErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.TODO())
			AssertNil(t, err)
			_, err = tx.Run("cypher", map[string]interface{}{})

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after transaction commit", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, TxCommitErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.TODO())
			AssertNil(t, err)
			_, err = tx.Run("cypher", map[string]interface{}{})
			AssertNil(t, err)
			err = tx.Commit()

			assertTokenExpiredError(t, err)
		})

		bt.Run("Token expiration after transaction rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			conn := &ConnFake{Alive: true, TxRollbackErr: tokenExpiredErr}
			pool.BorrowConn = conn

			tx, err := sess.BeginTransaction(context.TODO())
			AssertNil(t, err)
			_, err = tx.Run("cypher", map[string]interface{}{})
			AssertNil(t, err)
			err = tx.Rollback()

			assertTokenExpiredError(t, err)
		})
	})

	st.Run("Close", func(ct *testing.T) {
		ct.Run("Cleans up connection pool async", func(t *testing.T) {
			_, pool, sess := createSession()
			wg := sync.WaitGroup{}
			wg.Add(1)
			pool.CleanUpHook = func() {
				wg.Done()
			}
			sess.Close()
			wg.Wait()
		})
		ct.Run("Cleans up router async", func(t *testing.T) {
			router, _, sess := createSession()
			wg := sync.WaitGroup{}
			wg.Add(1)
			router.CleanUpHook = func() {
				wg.Done()
			}
			sess.Close()
			wg.Wait()
		})
	})
}

func assertTokenExpiredError(t *testing.T, err error) {
	AssertSameType(t, err, &TokenExpiredError{})
	AssertErrorMessageContains(t, err, "Neo.ClientError.Security.TokenExpired")
	AssertErrorMessageContains(t, err, "oopsie whoopsie")
}
