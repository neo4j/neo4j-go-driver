/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package bolt

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/log"
)

var logger = &log.ConsoleLogger{Errors: true, Infos: true, Warns: true}

func TestConnect(ot *testing.T) {
	// TODO: Test connect timeout

	auth := map[string]interface{}{
		"scheme":      "basic",
		"principal":   "neo4j",
		"credentials": "pass",
	}

	ot.Run("Bolt3 success", func(t *testing.T) {
		conn, srv, cleanup := setupBolt3Pipe(t)
		defer cleanup()

		// Simulate server with a succesful connect
		go func() {
			handshake := srv.waitForHandshake()
			// There should be a version 3 somewhere
			foundV3 := false
			for i := 0; i < 5; i++ {
				ver := handshake[(i * 4) : (i*4)+4]
				if ver[3] == 3 {
					foundV3 = true
				}
			}
			if !foundV3 {
				t.Fatalf("Didn't find version 3 in handshake: %+v", handshake)
			}

			// Accept bolt version 3
			srv.acceptVersion(3)
			srv.waitForHello()
			srv.acceptHello()
		}()

		// Pass the connection to bolt connect
		serverName := "theServerName"
		boltconn, err := Connect(serverName, conn, auth, logger)
		assertNoError(t, err)

		// Check some properties on the connection that is related to connecting
		if boltconn.ServerName() != serverName {
			t.Error("Wrong servername")
		}
		if !boltconn.IsAlive() {
			t.Error("New connection is dead")
		}

		boltconn.Close()
	})

	ot.Run("Bolt3 failed authentication", func(t *testing.T) {
		conn, srv, cleanup := setupBolt3Pipe(t)
		defer cleanup()

		// Simulate server with a succesful connect
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(3)
			srv.waitForHello()
			srv.rejectHelloUnauthorized()
		}()

		boltconn, err := Connect("serverName", conn, auth, logger)
		// Make sure that we get the right error type
		dbErr := err.(*connection.DatabaseError)
		if !dbErr.IsAuthentication() {
			t.Errorf("Should be authentication error: %s", dbErr)
		}
		if boltconn != nil {
			t.Error("Shouldn't returned conn")
		}
	})

	ot.Run("Server rejects versions", func(t *testing.T) {
		conn, srv, cleanup := setupBolt3Pipe(t)
		defer cleanup()

		// Simulate server that rejects whatever version the client supports
		go func() {
			srv.waitForHandshake()
			srv.rejectVersions()
			srv.closeConnection()
		}()

		_, err := Connect("servername", conn, auth, logger)
		assertError(t, err)
	})

	ot.Run("Server answers with invalid version", func(t *testing.T) {
		conn, srv, cleanup := setupBolt3Pipe(t)
		defer cleanup()

		// Simulate server that rejects whatever version the client supports
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(1)
		}()

		boltconn, err := Connect("servername", conn, auth, logger)
		assertError(t, err)
		if boltconn != nil {
			t.Error("Shouldn't returned conn")
		}
	})
}
