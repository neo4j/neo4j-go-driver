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

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-stub/control"
)

func Test_PassingAccessMode(t *testing.T) {
	t.Run("Direct", func(t *testing.T) {
		verifyReturn1 := func(t *testing.T, script string, mode neo4j.AccessMode) {
			stub := control.NewStubServer(t, 9001, script)
			defer stub.Finished(t)

			driver := newDriver(t, "bolt://localhost:9001")
			defer driver.Close()

			session, err := driver.Session(mode)
			assertNoError(t, err)
			assertNotNil(t, session)
			defer session.Close()

			result, err := neo4j.Single(session.Run("RETURN 1", nil))
			assertNoError(t, err)
			assertNotNil(t, result)

			assertInt64Eq(t, int64(1), result.GetByIndex(0).(int64))
		}

		verifyReturn1Tx := func(t *testing.T, script string, mode neo4j.AccessMode) {
			stub := control.NewStubServer(t, 9001, script)
			defer stub.Finished(t)

			driver := newDriver(t, "bolt://localhost:9001")
			defer driver.Close()

			session, err := driver.Session(mode)
			assertNoError(t, err)
			assertNotNil(t, session)
			defer session.Close()

			txFunc := session.WriteTransaction
			if mode == neo4j.AccessModeRead {
				txFunc = session.ReadTransaction
			}

			result, err := neo4j.Single(txFunc(func(tx neo4j.Transaction) (interface{}, error) {
				return tx.Run("RETURN 1", nil)
			}))
			assertNoError(t, err)
			assertNotNil(t, result)

			assertInt64Eq(t, int64(1), result.GetByIndex(0).(int64))
		}

		t.Run("shouldPassAccessModeForReadSessionRun", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_1_read.script"), neo4j.AccessModeRead)
		})

		t.Run("shouldPassAccessModeForReadSessionReadTransaction", func(t *testing.T) {
			verifyReturn1Tx(t, path.Join("v3", "return_1_read_tx.script"), neo4j.AccessModeRead)
		})

		t.Run("shouldNotPassAccessModeForWriteSessionRun", func(t *testing.T) {
			verifyReturn1(t, path.Join("v3", "return_1_write.script"), neo4j.AccessModeWrite)
		})

		t.Run("shouldNotPassAccessModeForWriteSessionWriteTransaction", func(t *testing.T) {
			verifyReturn1Tx(t, path.Join("v3", "return_1_write_tx.script"), neo4j.AccessModeWrite)
		})

	})

	t.Run("Routing", func(t *testing.T) {
		verifyReturn1 := func(t *testing.T, port int, script string, mode neo4j.AccessMode) {
			router := control.NewStubServer(t, 9001, path.Join("v3", "acquire_endpoints.script"))
			defer router.Finished(t)

			server := control.NewStubServer(t, port, script)
			defer server.Finished(t)

			driver := newDriver(t, "bolt+routing://localhost:9001")
			defer driver.Close()

			session, err := driver.Session(mode)
			assertNoError(t, err)
			assertNotNil(t, session)
			defer session.Close()

			result, err := neo4j.Single(session.Run("RETURN 1", nil))
			assertNoError(t, err)
			assertNotNil(t, result)

			assertInt64Eq(t, int64(1), result.GetByIndex(0).(int64))
		}

		verifyReturn1Tx := func(t *testing.T, port int, script string, mode neo4j.AccessMode) {
			router := control.NewStubServer(t, 9001, path.Join("v3", "acquire_endpoints.script"))
			defer router.Finished(t)

			server := control.NewStubServer(t, port, script)
			defer server.Finished(t)

			driver := newDriver(t, "bolt+routing://localhost:9001")
			defer driver.Close()

			session, err := driver.Session(mode)
			assertNoError(t, err)
			assertNotNil(t, session)
			defer session.Close()

			txFunc := session.WriteTransaction
			if mode == neo4j.AccessModeRead {
				txFunc = session.ReadTransaction
			}

			result, err := neo4j.Single(txFunc(func(tx neo4j.Transaction) (interface{}, error) {
				return tx.Run("RETURN 1", nil)
			}))
			assertNoError(t, err)
			assertNotNil(t, result)

			assertInt64Eq(t, int64(1), result.GetByIndex(0).(int64))
		}

		t.Run("shouldPassAccessModeForReadSessionRun", func(t *testing.T) {
			verifyReturn1(t, 9006, path.Join("v3", "read_server_read.script"), neo4j.AccessModeRead)
		})

		t.Run("shouldPassAccessModeForReadSessionReadTransaction", func(t *testing.T) {
			verifyReturn1Tx(t, 9005, path.Join("v3", "read_server_read_tx.script"), neo4j.AccessModeRead)
		})

		t.Run("shouldNotPassAccessModeForWriteSessionRun", func(t *testing.T) {
			verifyReturn1(t, 9007, path.Join("v3", "write_server_write.script"), neo4j.AccessModeWrite)
		})

		t.Run("shouldNotPassAccessModeForWriteSessionWriteTransaction", func(t *testing.T) {
			verifyReturn1Tx(t, 9007, path.Join("v3", "write_server_write_tx.script"), neo4j.AccessModeWrite)
		})

	})
}
