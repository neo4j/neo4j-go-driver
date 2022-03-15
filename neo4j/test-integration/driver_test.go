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
	"math"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestDriver(outer *testing.T) {
	server := dbserver.GetDbServer()

	outer.Run("VerifyConnectivity", func(inner *testing.T) {
		inner.Run("should return nil upon good connection", func(t *testing.T) {
			driver := server.Driver()
			defer driver.Close()
			assertNil(t, driver.VerifyConnectivity())
		})

		inner.Run("should return error upon bad connection", func(t *testing.T) {
			auth := neo4j.BasicAuth("bad user", "bad pass", "bad area")
			driver, err := neo4j.NewDriver(server.BoltURI(), auth, server.ConfigFunc())
			assertNil(t, err)
			defer driver.Close()
			err = driver.VerifyConnectivity()
			assertNotNil(t, err)
		})
	})

	outer.Run("Direct", func(inner *testing.T) {

		setUp := func() (neo4j.Driver, neo4j.Session) {
			driver := server.Driver()
			session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			return driver, session
		}

		tearDown := func(driver neo4j.Driver, session neo4j.Session) {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		}

		inner.Run("it should not allow work on existing sessions, after driver is closed", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(driver, session)

			result, err := session.Run("RETURN 1", nil)
			assertNil(t, err)

			if result.Next() {
				assertEquals(t, result.Record().Values[0], int64(1))
			}
			assertFalse(t, result.Next())
			assertNil(t, result.Err())

			err = driver.Close()
			assertNil(t, err)

			_, err = session.Run("RETURN 1", nil)
			assertNotNil(t, err)
		})

		inner.Run("it should not allow new sessions, after driver is closed", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(driver, session)
			result, err := session.Run("RETURN 1", nil)
			assertNil(t, err)

			if result.Next() {
				assertEquals(t, result.Record().Values[0], int64(1))
			}
			assertFalse(t, result.Next())
			assertNil(t, result.Err())

			err = driver.Close()
			assertNil(t, err)

			_ = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		})

	})

	outer.Run("Pooling without Connection Acquisition Timeout", func(inner *testing.T) {
		var (
			err    error
			driver neo4j.Driver
		)

		driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.ConfigFunc(), func(config *neo4j.Config) {
			config.MaxConnectionPoolSize = 2
			config.ConnectionAcquisitionTimeout = 0
		})
		assertNil(inner, err)

		defer func() {
			if driver != nil {
				driver.Close()
			}
		}()

		inner.Run("should return error when pool is full", func(t *testing.T) {
			// Open connection 1
			session1 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session1.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Open connection 2
			session2 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session2.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Try opening connection 3
			session3 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session3.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertTrue(t, neo4j.IsConnectivityError(err))
		})
	})

	outer.Run("Pooling with Connection Acquisition Timeout", func(inner *testing.T) {
		var (
			err    error
			driver neo4j.Driver
		)

		driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.ConfigFunc(), func(config *neo4j.Config) {
			config.MaxConnectionPoolSize = 2
			config.ConnectionAcquisitionTimeout = 10 * time.Second
		})
		assertNil(inner, err)

		defer func() {
			if driver != nil {
				driver.Close()
			}
		}()

		inner.Run("should return error when pool is full", func(t *testing.T) {
			// Open connection 1
			session1 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session1.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Open connection 2
			session2 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session2.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Try opening connection 3
			session3 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			start := time.Now()
			_, err = session3.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			elapsed := time.Since(start)
			assertTrue(t, neo4j.IsConnectivityError(err))
			assertTrue(t, math.Round(float64(elapsed/time.Second)) >= 10)
		})
	})
}
