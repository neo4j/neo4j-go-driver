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

package neo4j

import (
	"errors"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/log"
)

func TestSession(st *testing.T) {
	logger := log.ConsoleLogger{Errors: true, Infos: true, Warns: false}

	assertCleanSessionState := func(t *testing.T, sess *session) {
		if sess.inTx {
			t.Errorf("Session should not be in tx mode")
		}
	}

	createSession := func() (*testRouter, *testPool, *session) {
		conf := Config{MaxTransactionRetryTime: 3 * time.Millisecond}
		router := testRouter{}
		pool := testPool{}
		sess := newSession(&conf, &router, &pool, db.ReadMode, []string{}, "", &logger)
		sess.throttleTime = time.Millisecond * 1
		return &router, &pool, sess
	}

	st.Run("Retry mechanism", func(rt *testing.T) {
		// Checks that retries occur on database error and that it stops retrying after a certain
		// amount of time and that connections are returned to pool upon failure.
		rt.Run("Consistent transient error", func(t *testing.T) {
			_, pool, sess := createSession()
			numReturns := 0
			pool.returnHook = func() {
				numReturns++
			}
			conn := &testConn{isAlive: true}
			pool.borrowConn = conn
			transientErr := &db.DatabaseError{Code: "Neo.TransientError.General.MemoryPoolOutOfMemoryError"}
			numRetries := 0

			_, err := sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
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
			assertErrorEq(t, transientErr, err)
			assertCleanSessionState(t, sess)
		})

		// Checks that session is in clean state after connection fails to rollback.
		// "User" initiates rollback by letting the transaction function return a custom error.
		rt.Run("Failed rollback", func(t *testing.T) {
			_, pool, sess := createSession()
			rollbackErr := errors.New("Rollback error")
			causeOfRollbackErr := errors.New("Cause of rollback")
			pool.borrowConn = &testConn{isAlive: true, txRollbackErr: rollbackErr}
			_, err := sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
				return nil, causeOfRollbackErr
			})
			assertErrorEq(t, causeOfRollbackErr, err)
			assertCleanSessionState(t, sess)
		})

		// Check that sesssion is in clean state after connection fails to commit.
		rt.Run("Failed commit", func(t *testing.T) {
			_, pool, sess := createSession()
			commitErr := errors.New("Commit error")
			pool.borrowConn = &testConn{isAlive: true, txCommitErr: commitErr}
			_, err := sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
				return nil, nil
			})
			assertErrorEq(t, commitErr, err)
			assertCleanSessionState(t, sess)
		})
	})
}
