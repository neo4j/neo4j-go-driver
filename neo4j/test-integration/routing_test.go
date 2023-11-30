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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestRouting(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}
	ctx := context.Background()

	server := dbserver.GetDbServer(ctx)

	var session neo4j.SessionWithContext
	var result neo4j.ResultWithContext
	var summary neo4j.ResultSummary
	var err error

	getDriver := func(address string) neo4j.DriverWithContext {
		driver, err := neo4j.NewDriverWithContext(address, server.AuthToken(), server.ConfigFunc())
		if err != nil {
			panic(err.Error())
		}
		return driver
	}

	outer.Run("should successfully execute read/write when initial address contains unusable items", func(t *testing.T) {
		if !server.IsCluster {
			t.Skip("Needs cluster")
		}
		// Rely on address resolving
		driver := getDriver("neo4j://localhost")
		assertNil(t, err)
		defer driver.Close(ctx)

		session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeRead})
		defer session.Close(ctx)

		result, err = session.Run(ctx, "RETURN 1", nil)
		assertNil(t, err)

		summary, err = result.Consume(ctx)
		assertNil(t, err)
		assertNotNil(t, summary)
	})

	outer.Run("writes should be visible on followers", func(t *testing.T) {
		if !server.IsCluster {
			t.Skip("Needs cluster")
		}
		var readCount, writeCount any

		driver := getDriver(server.URI())
		assertNil(t, err)

		session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeWrite})

		writeCount, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			writeResult, err := tx.Run(ctx, "MERGE (n:Person {name: 'John'}) RETURN 1", nil)
			if err != nil {
				return nil, err
			}

			if writeResult.Next(ctx) {
				return writeResult.Record().Values[0], nil
			}

			if err := writeResult.Err(); err != nil {
				return nil, err
			}

			return 0, nil
		})
		assertNil(t, err)
		assertTrue(t, writeCount == 1)

		readCount, err = session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			readResult, err := tx.Run(ctx, "MATCH (n:Person {name: 'John'}) RETURN COUNT(*) AS count", nil)
			if err != nil {
				return nil, err
			}

			if readResult.Next(ctx) {
				return readResult.Record().Values[0], nil
			}

			if err := readResult.Err(); err != nil {
				return nil, err
			}

			return 0, nil
		})
		assertNil(t, err)
		assertTrue(t, readCount == 1)
	})
}
