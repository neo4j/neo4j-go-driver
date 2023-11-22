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

package test_integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestTransaction(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)
	var err error
	var driver neo4j.DriverWithContext
	var session neo4j.SessionWithContext
	var tx neo4j.ExplicitTransaction
	var result neo4j.ResultWithContext

	driver = server.Driver()
	session = driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	defer func() {
		if session != nil {
			session.Close(ctx)
		}
		driver.Close(ctx)
	}()

	outer.Run("Retry Mechanism", func(inner *testing.T) {
		transientError := &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.Outdated"}

		inner.Run("should work on ExecuteWrite", func(t *testing.T) {
			times := 0
			_, err = session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
				times++
				return nil, transientError
			})

			assertNotNil(t, err)
			assertTrue(t, times > 1)
		})

		inner.Run("should work on ExecuteRead", func(t *testing.T) {
			times := 0
			_, err = session.ExecuteRead(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
				times++
				return nil, transientError
			})

			assertNotNil(t, err)
			assertTrue(t, times > 1)
		})
	})

	outer.Run("should commit if work function doesn't return error", func(t *testing.T) {
		initialCount := readTransactionWithIntWork(ctx, t, session, intReturningWork(ctx, t, "MATCH (n:Person1) RETURN count(n)", nil))
		createResult := writeTransactionWithIntWork(ctx, t, session, intReturningWork(ctx, t, "CREATE (n:Person1) RETURN count(n)", nil))
		assertEquals(t, createResult, 1)

		matchResult := readTransactionWithIntWork(ctx, t, session, intReturningWork(ctx, t, "MATCH (n:Person1) RETURN count(n)", nil))
		assertEquals(t, matchResult, initialCount+createResult)
	})

	outer.Run("should rollback if work function returns error", func(t *testing.T) {
		createWork := intReturningWork(ctx, t, "CREATE (n:Person2) RETURN count(n)", nil)
		createResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			innerResult, err := createWork(tx)
			assertNil(t, err)
			assertEquals(t, innerResult, 1)

			return nil, errors.New("some error")
		})
		assertNotNil(t, err)
		assertNil(t, createResult)

		matchResult := readTransactionWithIntWork(ctx, t, session, intReturningWork(ctx, t, "MATCH (n:Person2) RETURN count(n)", nil))
		assertEquals(t, matchResult, 0)
	})

	outer.Run("should have keys available after run", func(t *testing.T) {
		tx, err = session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)

		result, err = tx.Run(ctx, "RETURN 1 AS N, 2 AS M", nil)
		assertNil(t, err)

		keys, err := result.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})
	})

	outer.Run("should have keys available after run and consume", func(t *testing.T) {
		tx, err = session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)

		result, err = tx.Run(ctx, "RETURN 1 AS N, 2 AS M", nil)
		assertNil(t, err)

		keys, err := result.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})

		_, err = result.Consume(ctx)
		assertNil(t, err)

		keys, err = result.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})
	})

	outer.Run("should have keys available for consecutive runs", func(t *testing.T) {
		tx, err = session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)

		result1, err := tx.Run(ctx, "RETURN 1 AS N, 2 AS M", nil)
		assertNil(t, err)

		result2, err := tx.Run(ctx, "RETURN 1 AS X, 2 AS Y", nil)
		assertNil(t, err)

		keys, err := result1.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})

		keys, err = result2.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"X", "Y"})
	})

	outer.Run("should have keys available for consecutive runs and consumes", func(t *testing.T) {
		tx, err = session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)

		result1, err := tx.Run(ctx, "RETURN 1 AS N, 2 AS M", nil)
		assertNil(t, err)

		result2, err := tx.Run(ctx, "RETURN 1 AS X, 2 AS Y", nil)
		assertNil(t, err)

		_, err = result1.Consume(ctx)
		assertNil(t, err)
		_, err = result2.Consume(ctx)
		assertNil(t, err)

		keys, err := result1.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})

		keys, err = result2.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"X", "Y"})
	})

	outer.Run("should have keys available for consecutive runs independent of order", func(t *testing.T) {
		tx, err = session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)

		result1, err := tx.Run(ctx, "RETURN 1 AS N, 2 AS M", nil)
		assertNil(t, err)

		result2, err := tx.Run(ctx, "RETURN 1 AS X, 2 AS Y", nil)
		assertNil(t, err)

		keys, err := result2.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"X", "Y"})

		keys, err = result1.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})

	})

	outer.Run("should have keys available for consecutive runs and consumes independent of order", func(t *testing.T) {
		tx, err = session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)

		result1, err := tx.Run(ctx, "RETURN 1 AS N, 2 AS M", nil)
		assertNil(t, err)

		result2, err := tx.Run(ctx, "RETURN 1 AS X, 2 AS Y", nil)
		assertNil(t, err)

		_, err = result1.Consume(ctx)
		assertNil(t, err)
		_, err = result2.Consume(ctx)
		assertNil(t, err)

		keys, err := result2.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"X", "Y"})

		keys, err = result1.Keys()
		assertNil(t, err)
		assertEquals(t, keys, []string{"N", "M"})
	})

	outer.Run("V3+", func(inner *testing.T) {

		if server.Version.LessThan(V350) {
			inner.Skip("this test is targeted for server version after neo4j 3.5.0")
		}

		inner.Run("should set transaction metadata", func(t *testing.T) {
			metadata := map[string]any{
				"m1": int64(1),
				"m2": "some string",
				"m3": 4.0,
				"m4": neo4j.LocalDateTimeOf(time.Now()),
			}

			tx, err = session.BeginTransaction(ctx, neo4j.WithTxMetadata(metadata))
			assertNil(t, err)
			defer tx.Close(ctx)

			number := transactionWithIntWork(t, tx, intReturningWork(ctx, t, "RETURN $x", map[string]any{"x": 1}))
			assertEquals(t, number, 1)

			if !server.IsEnterprise {
				t.Skip("Can not list transactions on non-enterprise version")
			}

			session2 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
			defer session2.Close(ctx)
			matched, err := session2.ExecuteRead(ctx, listTransactionsAndMatchMetadataWork(ctx, server.Version, metadata))
			assertNil(t, err)
			assertTrue(t, matched.(bool))
		})

		inner.Run("should set transaction timeout", func(t *testing.T) {
			createNode(ctx, t, session, "TxTimeOut", nil)

			session2, tx2 := newSessionAndTx(ctx, t, driver, neo4j.AccessModeWrite)
			defer session2.Close(ctx)
			defer tx2.Close(ctx)

			updateNodeInTx(ctx, t, tx2, "TxTimeOut", map[string]any{"id": 1})

			session3, tx3 := newSessionAndTx(ctx, t, driver, neo4j.AccessModeWrite, neo4j.WithTxTimeout(1*time.Second))
			defer session3.Close(ctx)
			defer tx3.Close(ctx)

			_, err := updateNodeWork(ctx, t, "TxTimeOut", map[string]any{"id": 2})(tx3)
			assertNotNil(t, err)
		})

	})

	outer.Run("V3 API on V1 & V2", func(t *testing.T) {
		if server.Version.GreaterThanOrEqual(V350) {
			t.Skip("this test is targeted for server versions less than neo4j 3.5.0")
		}

		t.Run("should fail when transaction timeout is set for Session.BeginTransaction", func(t *testing.T) {
			_, err := session.BeginTransaction(ctx, neo4j.WithTxTimeout(1*time.Second))
			assertNotNil(t, err)
		})

		t.Run("should fail when transaction metadata is set for Session.BeginTransaction", func(t *testing.T) {
			_, err := session.BeginTransaction(ctx, neo4j.WithTxMetadata(map[string]any{"x": 1}))
			assertNotNil(t, err)
		})
	})
}
