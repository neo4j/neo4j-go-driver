/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-stub/control"
)

func Test_Query(t *testing.T) {
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

		result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1}, txConfig...)
		assertNoError(t, err)
		assertNotNil(t, result)

		var count int64
		for result.Next() {
			if x, ok := result.Record().Get("x"); ok {
				count += x.(int64)
			}
		}

		assertNoError(t, result.Err())
		assertInt64Eq(t, count, int64(1))
	}

	t.Run("V1", func(t *testing.T) {
		t.Skip()
		t.Run("shouldExecuteSimpleQuery", func(t *testing.T) {
			verifyReturn1(t, path.Join("v1", "return_1.script"), nil)
		})
	})

	t.Run("V3", func(t *testing.T) {
		t.Run("shouldExecuteSimpleQuery", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_1.script"), nil)
		})

		t.Run("shouldRunWithBookmarks", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_with_bookmarks.script"), []string{"foo", "bar"})
		})

		t.Run("shouldRunWithMetadata", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_with_metadata.script"), nil, neo4j.WithTxMetadata(map[string]interface{}{"user": "some-user"}))
		})

		t.Run("shouldRunWithTimeout", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_with_timeout.script"), nil, neo4j.WithTxTimeout(12340*time.Millisecond))
		})

	})

}
