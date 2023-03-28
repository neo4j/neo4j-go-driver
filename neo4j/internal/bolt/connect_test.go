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
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"context"
	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"testing"

	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

var logger = &log.Void{}

func TestConnect(ot *testing.T) {
	// TODO: Test connect timeout

	auth := &db.ReAuthToken{
		FromSession: false,
		Manager: iauth.Token{Tokens: map[string]any{
			"scheme":      "basic",
			"principal":   "neo4j",
			"credentials": "pass",
		}},
	}

	ot.Run("Server rejects versions", func(t *testing.T) {
		// Doesn't matter what bolt version, shouldn't reach a bolt handler
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()

		// Simulate server that rejects whatever version the client supports
		go func() {
			srv.waitForHandshake()
			srv.rejectVersions()
			srv.closeConnection()
		}()

		_, err := Connect(context.Background(), "servername", conn, auth, "007", nil, logger, nil)
		AssertError(t, err)
	})

	ot.Run("Server answers with invalid version", func(t *testing.T) {
		// Doesn't matter what bolt version, shouldn't reach a bolt handler
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()

		// Simulate server that rejects whatever version the client supports
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(1, 0)
		}()

		boltconn, err := Connect(context.Background(), "servername", conn, auth, "007", nil, logger, nil)
		AssertError(t, err)
		if boltconn != nil {
			t.Error("Shouldn't returned conn")
		}
	})
}
