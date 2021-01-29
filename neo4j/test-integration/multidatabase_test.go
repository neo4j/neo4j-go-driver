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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test_integration

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/dbserver"
)

func TestMultidatabase(ot *testing.T) {
	server := dbserver.GetDbServer()

	driver := server.Driver()
	defer driver.Close()

	// Need > 4.0 for database support
	if server.Version.LessThan(V4) {
		ot.Skip("Versions prior to 4.0 does not support multidatabase")
	}

	if !server.IsEnterprise {
		ot.Skip("Need enterprise version to test multidatabase")
	}

	// Ensure that a test database exists using system database
	func() {
		sysSess := driver.NewSession(neo4j.SessionConfig{DatabaseName: "system"})
		defer sysSess.Close()
		_, err := sysSess.Run("DROP DATABASE testdb IF EXISTS", nil)
		assertNoError(ot, err)
		_, err = sysSess.Run("CREATE DATABASE testdb", nil)
		assertNoError(ot, err)
	}()

	ot.Run("Node created in test db should not be visible in default db", func(t *testing.T) {
		// Create node in testdb session
		testSess := driver.NewSession(neo4j.SessionConfig{DatabaseName: "testdb"})
		randId := createRandomNode(t, testSess)
		testSess.Close()

		// Look for above node in default database session, it shouldn't exist there
		defaultSess := driver.NewSession(neo4j.SessionConfig{})
		assertNoRandomNode(t, defaultSess, randId)
		defaultSess.Close()

		// Look again in testdb session, should of course exist here
		testSess = driver.NewSession(neo4j.SessionConfig{DatabaseName: "testdb"})
		assertRandomNode(t, testSess, randId)
		testSess.Close()
	})
}
