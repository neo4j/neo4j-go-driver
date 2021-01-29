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

package test_stub

import (
	"path"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-stub/control"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Transaction(t *testing.T) {
	var verifyReturn1 = func(t *testing.T, script string, bookmarks []string, txConfig ...func(*neo4j.TransactionConfig)) {
		if bookmarks == nil {
			bookmarks = []string{}
		}

		stub := control.NewStubServer(t, 9001, script)
		defer stub.Finished(t)

		driver := newDriver(t, "bolt://localhost:9001")
		defer driver.Close()

		session := createWriteSession(t, driver, bookmarks...)
		defer session.Close()

		tx := createTx(t, session, txConfig...)
		defer tx.Close()

		result, err := tx.Run("RETURN $x", map[string]interface{}{"x": 1})
		require.NoError(t, err)
		require.NotNil(t, result)

		var count int64
		for result.Next() {
			if x, ok := result.Record().Get("x"); ok {
				count += x.(int64)
			}
		}

		require.NoError(t, result.Err())

		assert.Equal(t, count, int64(1))
		assert.Empty(t, session.LastBookmark())

		err = tx.Commit()
		require.NoError(t, err)

		assert.Equal(t, "bookmark:1", session.LastBookmark())
	}

	var verifyFailureOnExplicitCommit = func(t *testing.T, script string) {
		stub := control.NewStubServer(t, 9001, script)
		defer stub.Finished(t)

		driver := newDriver(t, "bolt://localhost:9001")
		defer driver.Close()

		session := createWriteSession(t, driver)
		defer session.Close()

		tx := createTx(t, session)
		defer tx.Close()

		result, err := tx.Run("CREATE (n {name: 'Bob'})", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		require.False(t, result.Next())
		require.NoError(t, result.Err())

		err = tx.Commit()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected connection state")
	}

	var verifyFailureOnTxFuncCommit = func(t *testing.T, script string) {
		stub := control.NewStubServer(t, 9001, script)
		defer stub.Finished(t)

		driver := newDriver(t, "bolt://localhost:9001")
		defer driver.Close()

		session := createWriteSession(t, driver)
		defer session.Close()

		result, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			innerResult, innerErr := tx.Run("CREATE (n {name: 'Bob'})", nil)
			require.NoError(t, innerErr)
			require.NotNil(t, innerResult)
			return innerResult, innerErr
		})

		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected connection state")
	}

	t.Run("V1", func(t *testing.T) {
		t.Run("shouldFailOnConnectionFailureOnExplicitCommit", func(t *testing.T) {
			verifyFailureOnExplicitCommit(t, path.Join("v1", "connection_error_on_commit.script"))
		})

		t.Run("shouldFailOnConnectionFailureOnTxFuncCommit", func(t *testing.T) {
			verifyFailureOnTxFuncCommit(t, path.Join("v1", "connection_error_on_commit.script"))
		})
	})

	t.Run("V3", func(t *testing.T) {
		t.Run("shouldExecuteSimpleQuery", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_1_in_tx.script"), nil)
		})

		t.Run("shouldBeginTransactionWithMetadata", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "begin_with_metadata.script"), nil, neo4j.WithTxMetadata(map[string]interface{}{"user": "some-user"}))
		})

		t.Run("shouldBeginTransactionWithTimeout", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "begin_with_timeout.script"), nil, neo4j.WithTxTimeout(12340*time.Millisecond))
		})

		t.Run("shouldFailOnConnectionFailureOnExplicitCommit", func(t *testing.T) {
			verifyFailureOnExplicitCommit(t, path.Join("v3", "connection_error_on_commit.script"))
		})

		t.Run("shouldFailOnConnectionFailureOnTxFuncCommit", func(t *testing.T) {
			verifyFailureOnTxFuncCommit(t, path.Join("v3", "connection_error_on_commit.script"))
		})
	})
}
