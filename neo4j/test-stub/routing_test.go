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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Routing(t *testing.T) {
	t.Run("shouldRevertToInitialRouterIfKnownRouterReturnsInvalidRoutingTable", func(t *testing.T) {
		router1 := control.NewStubServer(t, 9001, path.Join("v3", "acquire_endpoints_point_to_empty_router_and_exit.script"))
		router2 := control.NewStubServer(t, 9004, path.Join("v3", "acquire_endpoints_empty.script"))
		router3 := control.NewStubServer(t, 9003, path.Join("v3", "acquire_endpoints_three_servers_and_exit.script"))
		reader := control.NewStubServer(t, 9002, path.Join("v3", "read_server_read_tx.script"))
		defer router1.Finished(t)
		defer router2.Finished(t)
		defer router3.Finished(t)
		defer reader.Finished(t)

		driver := newDriver(t, "bolt+routing://my.virtual.host:8080", func(config *neo4j.Config) {
			config.AddressResolver = func(address neo4j.ServerAddress) []neo4j.ServerAddress {
				return []neo4j.ServerAddress{
					neo4j.NewServerAddress("127.0.0.1", "9001"),
					neo4j.NewServerAddress("127.0.0.1", "9003"),
				}
			}
		})
		defer driver.Close()

		session, err := driver.Session(neo4j.AccessModeRead)
		require.NoError(t, err)
		require.NotNil(t, session)
		defer session.Close()

		result, err := neo4j.Single(session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			return tx.Run("RETURN 1", nil)
		}))
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, int64(1), result.GetByIndex(0))
	})
}
