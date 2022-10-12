/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestMultidatabase(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	driver := server.Driver()
	defer driver.Close(ctx)

	// Need > 4.0 for database support
	if server.Version.LessThan(V4) {
		outer.Skip("Versions prior to 4.0 do not support multidatabase")
	}

	if !server.IsEnterprise {
		outer.Skip("Need enterprise version to test multidatabase")
	}

	// Ensure that a test database exists using system database
	func() {
		sysSess := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "system"})
		defer sysSess.Close(ctx)
		_, err := sysSess.Run(ctx, server.DropDatabaseQuery("testdb"), nil)
		assertNil(outer, err)
		_, err = sysSess.Run(ctx, server.CreateDatabaseQuery("testdb"), nil)
		assertNil(outer, err)
	}()

	outer.Run("Node created in test db should not be visible in default db", func(t *testing.T) {
		// Create node in testdb session
		testSess := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "testdb"})
		randId := createRandomNode(ctx, t, testSess)
		testSess.Close(ctx)

		// Look for above node in default database session, it shouldn't exist there
		defaultSess := driver.NewSession(ctx, neo4j.SessionConfig{})
		assertNoRandomNode(ctx, t, defaultSess, randId)
		defaultSess.Close(ctx)

		// Look again in testdb session, should of course exist here
		testSess = driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "testdb"})
		assertRandomNode(ctx, t, testSess, randId)
		testSess.Close(ctx)
	})

}
