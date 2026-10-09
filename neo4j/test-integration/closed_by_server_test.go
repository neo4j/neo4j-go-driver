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
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/test-integration/dbserver"
)

// closedByServerAgent marks the connections the server is asked to close.
const closedByServerAgent = "go-driver-closed-by-server-test"

func TestConnectionClosedByServer(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}
	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	newDriver := func(t *testing.T, userAgent string) neo4j.Driver {
		driver, err := neo4j.NewDriver(server.BoltURI(), server.AuthToken(), func(c *config.Config) {
			c.MaxConnectionPoolSize = 1
			c.UserAgent = userAgent
		})
		assertNil(t, err)
		t.Cleanup(func() { assertNil(t, driver.Close(ctx)) })
		return driver
	}
	admin := newDriver(outer, closedByServerAgent+"-admin")
	agentParam := map[string]any{"agent": closedByServerAgent}

	countConnections := func(t *testing.T) int64 {
		result, err := neo4j.ExecuteQuery(ctx, admin,
			"CALL dbms.listConnections() YIELD userAgent WHERE userAgent = $agent RETURN count(*)",
			agentParam, neo4j.EagerResultTransformer)
		assertNil(t, err)
		return result.Records[0].Values[0].(int64)
	}

	closeConnections := func(t *testing.T) {
		_, err := neo4j.ExecuteQuery(ctx, admin,
			`CALL dbms.listConnections() YIELD connectionId, userAgent WHERE userAgent = $agent
			 CALL dbms.killConnection(connectionId) YIELD message RETURN message`,
			agentParam, neo4j.EagerResultTransformer)
		if err != nil {
			t.Skipf("server cannot close connections on request: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for countConnections(t) > 0 {
			if time.Now().After(deadline) {
				t.Fatal("server still lists the connections it was asked to close")
			}
			time.Sleep(50 * time.Millisecond)
		}
		// killConnection returns before the close reaches the client.
		time.Sleep(200 * time.Millisecond)
	}

	autoCommit := func(t *testing.T, driver neo4j.Driver) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)
		result, err := session.Run(ctx, "RETURN 1", nil)
		assertNil(t, err)
		_, err = result.Single(ctx)
		assertNil(t, err)
	}

	assertOnePooledConnection := func(t *testing.T) {
		if n := countConnections(t); n != 1 {
			t.Fatalf("expected one pooled connection, server lists %d", n)
		}
	}

	outer.Run("auto-commit transaction", func(t *testing.T) {
		driver := newDriver(t, closedByServerAgent)
		autoCommit(t, driver)
		closeConnections(t)

		autoCommit(t, driver)
		assertOnePooledConnection(t)
	})

	outer.Run("explicit transaction", func(t *testing.T) {
		driver := newDriver(t, closedByServerAgent)
		autoCommit(t, driver)
		closeConnections(t)

		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)
		tx, err := session.BeginTransaction(ctx)
		assertNil(t, err)
		_, err = tx.Run(ctx, "RETURN 1", nil)
		assertNil(t, err)
		assertNil(t, tx.Commit(ctx))
		assertOnePooledConnection(t)
	})
}
