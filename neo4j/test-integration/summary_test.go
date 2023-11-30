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
	bm "github.com/neo4j/neo4j-go-driver/v5/neo4j/bookmarks"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestResultSummary(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	const extraDatabase = "extra"

	var server dbserver.DbServer
	var driver neo4j.DriverWithContext
	var bookmark string
	noParams := map[string]any{}
	ctx := context.Background()

	server = dbserver.GetDbServer(ctx)
	driver = server.Driver(func(config *config.Config) {
		config.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
	})
	assertNotNil(outer, driver)

	outer.Run("from single-tenant Neo4j servers", func(inner *testing.T) {
		if isMultiTenant(server) {
			inner.Skip(`Multi-tenant servers are covered in other tests`)
		}

		inner.Run("does not include any database information", func(t *testing.T) {
			session := driver.NewSession(ctx, config.SessionConfig{Bookmarks: bm.BookmarksFromRawValues(bookmark)})
			defer assertCloses(ctx, t, session)
			result, err := session.Run(ctx, "RETURN 42", noParams)
			assertNil(t, err)

			summary, err := result.Consume(ctx)
			assertNil(t, err)
			if server.Version.GreaterThanOrEqual(V4) {
				assertEquals(t, summary.Database().Name(), "neo4j")
			} else {
				assertNil(t, summary.Database())
			}
		})
	})

	outer.Run("from multi-tenant Neo4j servers", func(inner *testing.T) {
		if !isMultiTenant(server) {
			inner.Skip("Multi-tenancy is a Neo4j 4+ feature")
		}

		session := driver.NewSession(ctx, config.SessionConfig{DatabaseName: "system", BoltLogger: neo4j.ConsoleBoltLogger()})
		defer assertCloses(ctx, inner, session)
		res, err := session.Run(ctx, server.CreateDatabaseQuery(extraDatabase), map[string]any{})
		assertNil(inner, err)
		_, err = res.Consume(ctx) // consume result to obtain bookmark
		assertNil(inner, err)
		bookmarks := bm.BookmarksToRawValues(session.LastBookmarks())
		assertEquals(inner, len(bookmarks), 1)
		bookmark = bookmarks[0]

		defer func() {
			session := driver.NewSession(ctx, config.SessionConfig{DatabaseName: "system", Bookmarks: bm.BookmarksFromRawValues(bookmark)})
			defer assertCloses(ctx, inner, session)
			res, err := session.Run(ctx, server.DropDatabaseQuery(extraDatabase), map[string]any{})
			assertNil(inner, err)
			_, err = res.Consume(ctx)
			assertNil(inner, err)
			bookmark = ""
		}()

		inner.Run("includes the default database information", func(t *testing.T) {
			session := driver.NewSession(ctx, config.SessionConfig{Bookmarks: bm.BookmarksFromRawValues(bookmark)})
			defer assertCloses(ctx, t, session)
			result, err := session.Run(ctx, "RETURN 42", noParams)
			assertNil(t, err)

			summary, err := result.Consume(ctx)
			assertNil(t, err)
			assertEquals(t, summary.Database().Name(), "neo4j")
		})

		inner.Run("includes the database information, based on session configuration", func(t *testing.T) {
			session := driver.NewSession(ctx, config.SessionConfig{DatabaseName: extraDatabase, Bookmarks: bm.BookmarksFromRawValues(bookmark)})
			defer assertCloses(ctx, t, session)
			result, err := session.Run(ctx, "RETURN 42", noParams)
			assertNil(t, err)

			summary, err := result.Consume(ctx)
			assertNil(t, err)
			assertEquals(t, summary.Database().Name(), extraDatabase)
		})
	})

}

func isMultiTenant(server dbserver.DbServer) bool {
	return server.Version.GreaterThanOrEqual(V4) && server.IsEnterprise
}
