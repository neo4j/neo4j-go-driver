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

package test_integration

import (
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestTimeoutAndLifetime(outer *testing.T) {
	server := dbserver.GetDbServer()

	outer.Run("should error when ConnectionAcquisitionTimeout is hit", func(t *testing.T) {
		var err error
		var driver neo4j.Driver
		var session1, session2 neo4j.Session

		driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.ConfigFunc(), func(config *neo4j.Config) {
			config.ConnectionAcquisitionTimeout = 1 * time.Second
			config.MaxConnectionPoolSize = 1
		})
		assertNil(t, err)
		assertNotNil(t, driver)
		defer driver.Close()

		session1, _ = newSessionAndTx(t, driver, neo4j.AccessModeRead)
		defer session1.Close()

		session2 = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
		assertNotNil(t, session2)
		defer session2.Close()

		_, err = session2.Run("RETURN 1", nil)
		assertNotNil(t, err)
	})

	outer.Run("should timeout connection when SocketConnectTimeout is hit", func(t *testing.T) {
		var err error
		var driver neo4j.Driver
		var session neo4j.Session

		driver, err = neo4j.NewDriver("bolt://10.255.255.1:8080", server.AuthToken(), server.ConfigFunc(), func(config *neo4j.Config) {
			config.SocketConnectTimeout = 1 * time.Second
		})
		assertNil(t, err)
		assertNotNil(t, driver)
		defer driver.Close()

		session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
		defer session.Close()

		_, err = session.BeginTransaction()
		assertNotNil(t, err)
	})

}
