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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"math"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestDriver(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}
	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	outer.Run("VerifyConnectivity", func(inner *testing.T) {
		inner.Run("should return nil upon good connection", func(t *testing.T) {
			driver := server.Driver()
			defer func() { _ = driver.Close(ctx) }()
			assertNil(t, driver.VerifyConnectivity(ctx))
		})

		inner.Run("should return error upon bad connection", func(t *testing.T) {
			auth := neo4j.BasicAuth("bad user", "bad pass", "bad area")
			driver, err := neo4j.NewDriverWithContext(server.BoltURI(), auth, server.ConfigFunc())
			assertNil(t, err)
			defer func() { _ = driver.Close(ctx) }()
			err = driver.VerifyConnectivity(ctx)
			assertNotNil(t, err)
		})
	})

	outer.Run("Direct", func(inner *testing.T) {

		setUp := func() (neo4j.DriverWithContext, neo4j.SessionWithContext) {
			driver := server.Driver()
			session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			return driver, session
		}

		tearDown := func(driver neo4j.DriverWithContext, session neo4j.SessionWithContext) {
			if session != nil {
				_ = session.Close(ctx)
			}

			if driver != nil {
				_ = driver.Close(ctx)
			}
		}

		inner.Run("it should not allow work on existing sessions, after driver is closed", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(driver, session)

			result, err := session.Run(ctx, "RETURN 1", nil)
			assertNil(t, err)

			if result.Next(ctx) {
				assertEquals(t, result.Record().Values[0], int64(1))
			}
			assertFalse(t, result.Next(ctx))
			assertNil(t, result.Err())

			err = driver.Close(ctx)
			assertNil(t, err)

			_, err = session.Run(ctx, "RETURN 1", nil)
			assertNotNil(t, err)
		})

		inner.Run("it should not allow new sessions, after driver is closed", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(driver, session)
			result, err := session.Run(ctx, "RETURN 1", nil)
			assertNil(t, err)

			if result.Next(ctx) {
				assertEquals(t, result.Record().Values[0], int64(1))
			}
			assertFalse(t, result.Next(ctx))
			assertNil(t, result.Err())

			err = driver.Close(ctx)
			assertNil(t, err)

			_ = driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		})

	})

	outer.Run("Pooling without Connection Acquisition Timeout", func(inner *testing.T) {
		var (
			err    error
			driver neo4j.DriverWithContext
		)

		driver, err = neo4j.NewDriverWithContext(server.BoltURI(), server.AuthToken(), server.ConfigFunc(), func(config *config.Config) {
			config.MaxConnectionPoolSize = 2
			config.ConnectionAcquisitionTimeout = 0
		})
		assertNil(inner, err)

		defer func() {
			if driver != nil {
				_ = driver.Close(ctx)
			}
		}()

		inner.Run("should return error when pool is full", func(t *testing.T) {
			// Open connection 1
			session1 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session1.Run(ctx, "UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Open connection 2
			session2 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session2.Run(ctx, "UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Try opening connection 3
			session3 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session3.Run(ctx, "UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertTrue(t, neo4j.IsConnectivityError(err))
		})
	})

	outer.Run("Pooling with Connection Acquisition Timeout", func(inner *testing.T) {
		var (
			err    error
			driver neo4j.DriverWithContext
		)

		driver, err = neo4j.NewDriverWithContext(server.BoltURI(), server.AuthToken(), server.ConfigFunc(), func(config *config.Config) {
			config.MaxConnectionPoolSize = 2
			config.ConnectionAcquisitionTimeout = 10 * time.Second
		})
		assertNil(inner, err)

		defer func() {
			if driver != nil {
				_ = driver.Close(ctx)
			}
		}()

		inner.Run("should return error when pool is full", func(t *testing.T) {
			// Open connection 1
			session1 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session1.Run(ctx, "UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Open connection 2
			session2 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err = session2.Run(ctx, "UNWIND RANGE(1, 100) AS N RETURN N", nil)
			assertNil(t, err)

			// Try opening connection 3
			session3 := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			start := time.Now()
			_, err = session3.Run(ctx, "UNWIND RANGE(1, 100) AS N RETURN N", nil)
			elapsed := time.Since(start)
			assertTrue(t, neo4j.IsConnectivityError(err))
			assertTrue(t, math.Round(float64(elapsed/time.Second)) >= 10)
		})
	})
}
