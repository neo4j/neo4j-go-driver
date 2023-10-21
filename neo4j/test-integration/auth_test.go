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

func TestAuthentication(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	getDriverAndSession := func(ctx context.Context, token neo4j.AuthToken) (neo4j.DriverWithContext, neo4j.SessionWithContext) {
		driver, err := neo4j.NewDriverWithContext(server.URI(), token, server.ConfigFunc())
		if err != nil {
			panic(err)
		}

		return driver, driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	}

	outer.Run("when wrong credentials are provided, it should fail with authentication error", func(t *testing.T) {
		token := neo4j.BasicAuth("wrong", "wrong", "")
		driver, session := getDriverAndSession(ctx, token)
		defer driver.Close(ctx)
		defer session.Close(ctx)

		_, err := session.Run(ctx, "RETURN 1", nil)
		if err == nil {
			t.Fatal("Should NOT be able to connect")
		}
		if !neo4j.IsNeo4jError(err) {
			t.Fatalf("Should be Neo4jError but was: %s (%T)", err, err)
		}
		neo4jErr := err.(*neo4j.Neo4jError)
		if !neo4jErr.IsAuthenticationFailed() {
			t.Errorf("Should be authentication error but was: %s", neo4jErr)
		}
	})
}
