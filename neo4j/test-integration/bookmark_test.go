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
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestBookmark(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}
	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	createNodeInTx := func(driver neo4j.DriverWithContext) string {
		session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		defer session.Close(ctx)

		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, "CREATE ()", nil)
			assertNil(outer, err)

			summary, err := result.Consume(ctx)
			assertNil(outer, err)

			assertEquals(outer, summary.Counters().NodesCreated(), 1)

			return 0, nil
		})
		assertNil(outer, err)

		bookmarks := neo4j.BookmarksToRawValues(session.LastBookmarks())
		assertEquals(outer, len(bookmarks), 1)
		return bookmarks[0]
	}

	outer.Run("session constructed with no bookmarks", func(inner *testing.T) {
		setUp := func() (neo4j.DriverWithContext, neo4j.SessionWithContext) {
			driver := server.Driver()
			session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			return driver, session
		}

		tearDown := func(session neo4j.SessionWithContext, driver neo4j.DriverWithContext) {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}

		inner.Run("when a node is created in auto-commit mode, last bookmark should not be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			result, err := session.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)
			_, err = result.Consume(ctx)

			assertNil(t, err)
			assertStringsNotEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})

		inner.Run("when a node is created in explicit transaction and committed, last bookmark should not be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)

			result, err := tx.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)

			_, err = result.Consume(ctx)
			assertNil(t, err)

			err = tx.Commit(ctx)
			assertNil(t, err)

			assertStringsNotEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})

		inner.Run("when a node is created in explicit transaction and rolled back, last bookmark should be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)

			result, err := tx.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)

			_, err = result.Consume(ctx)
			assertNil(t, err)

			err = tx.Rollback(ctx)
			assertNil(t, err)

			assertStringsEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})

		inner.Run("when a node is created in transaction function, last bookmark should not be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
				assertNil(t, err)

				summary, err := result.Consume(ctx)
				assertNil(t, err)

				return summary.Counters().NodesCreated(), nil
			})

			assertNil(t, err)
			assertEquals(t, result, 1)
			assertStringsNotEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})

		inner.Run("when a node is created in transaction function and rolled back, last bookmark should be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			failWith := errors.New("some error")
			result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
				assertNil(t, err)

				_, err = result.Consume(ctx)
				assertNil(t, err)

				return 0, failWith
			})

			assertEquals(t, err, failWith)
			assertNil(t, result)
			assertStringsEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})

		inner.Run("when a node is queried in transaction function, last bookmark should not be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "MATCH (p:Person) RETURN count(p)", nil)
				assertNil(t, err)

				count := 0
				for result.Next(ctx) {
					count++
				}
				assertNil(t, result.Err())

				return count, nil
			})

			assertNil(t, err)
			assertEquals(t, result, 1)
			assertStringsNotEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})

		inner.Run("when a node is created in transaction function and rolled back, last bookmark should be empty", func(t *testing.T) {
			driver, session := setUp()
			defer tearDown(session, driver)

			failWith := errors.New("some error")
			result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "MATCH (p:Person) RETURN count(p)", nil)
				assertNil(t, err)

				count := 0
				for result.Next(ctx) {
					count++
				}
				assertNil(t, result.Err())

				return count, failWith
			})

			assertEquals(t, err, failWith)
			assertNil(t, result)
			assertStringsEmpty(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
		})
	})

	outer.Run("session constructed with one bookmark", func(inner *testing.T) {
		setUp := func() (neo4j.DriverWithContext, neo4j.SessionWithContext, string) {
			driver := server.Driver()
			bookmark := createNodeInTx(driver)
			session := driver.NewSession(ctx, neo4j.SessionConfig{
				AccessMode: neo4j.AccessModeWrite,
				Bookmarks:  neo4j.BookmarksFromRawValues(bookmark),
			})
			return driver, session, bookmark
		}

		tearDown := func(session neo4j.SessionWithContext, driver neo4j.DriverWithContext) {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}

		inner.Run("given bookmarks should be reported back by the server after BEGIN", func(t *testing.T) {
			driver, session, bookmark := setUp()
			defer tearDown(session, driver)

			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)
			defer tx.Close(ctx)

			assertEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark})
		})

		inner.Run("given bookmarks should be accessible after ROLLBACK", func(t *testing.T) {
			driver, session, bookmark := setUp()
			defer tearDown(session, driver)

			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)
			defer tx.Close(ctx)

			_, err = tx.Run(ctx, "CREATE ()", nil)
			assertNil(t, err)

			err = tx.Rollback(ctx)
			assertNil(t, err)

			assertEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark})
		})

		inner.Run("given bookmarks should be accessible when transaction fails", func(t *testing.T) {
			driver, session, bookmark := setUp()
			defer tearDown(session, driver)

			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)
			defer tx.Close(ctx)

			_, err = tx.Run(ctx, "RETURN", nil)
			assertNotNil(t, err)

			err = tx.Close(ctx)
			assertNil(t, err)
			assertEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark})
		})

		inner.Run("given bookmarks should be accessible after run", func(t *testing.T) {
			driver, session, bookmark := setUp()
			defer tearDown(session, driver)

			result, err := session.Run(ctx, "RETURN 1", nil)
			assertNil(t, err)

			_, err = result.Consume(ctx)
			assertNil(t, err)

			assertEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark})
		})

		inner.Run("given bookmarks should be accessible after failed run", func(t *testing.T) {
			driver, session, bookmark := setUp()
			defer tearDown(session, driver)

			_, err := session.Run(ctx, "RETURN", nil)
			assertNotNil(t, err)

			assertEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark})
		})

	})

	outer.Run("session constructed with two sets of bookmarks", func(inner *testing.T) {
		var (
			driver    neo4j.DriverWithContext
			session   neo4j.SessionWithContext
			bookmark1 string
			bookmark2 string
		)

		driver = server.Driver()

		bookmark1 = createNodeInTx(driver)
		bookmark2 = createNodeInTx(driver)
		assertNotEquals(inner, bookmark1, bookmark2)

		session = driver.NewSession(ctx, neo4j.SessionConfig{
			AccessMode: neo4j.AccessModeWrite,
			Bookmarks:  neo4j.BookmarksFromRawValues(bookmark1, bookmark2),
		})

		defer func() {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}()

		inner.Run("all bookmarks should be reported back by the server after BEGIN", func(t *testing.T) {
			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)
			defer tx.Close(ctx)

			assertEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark1, bookmark2})
		})

		inner.Run("new bookmark should be reported back by the server after committing", func(t *testing.T) {
			tx, err := session.BeginTransaction(ctx)
			assertNil(t, err)
			defer tx.Close(ctx)

			result, err := tx.Run(ctx, "CREATE ()", nil)
			assertNil(t, err)

			summary, err := result.Consume(ctx)
			assertNil(t, err)
			assertEquals(t, summary.Counters().NodesCreated(), 1)

			err = tx.Commit(ctx)
			assertNil(t, err)

			assertNotNil(t, neo4j.BookmarksToRawValues(session.LastBookmarks()))
			assertNotEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark1})
			assertNotEquals(t, neo4j.BookmarksToRawValues(session.LastBookmarks()), []string{bookmark2})
		})

	})

	outer.Run("session constructed with unreachable bookmark", func(inner *testing.T) {

		setUp := func() (neo4j.DriverWithContext, neo4j.SessionWithContext, string) {
			driver := server.Driver()

			bookmark := createNodeInTx(driver)

			session := driver.NewSession(ctx, neo4j.SessionConfig{
				AccessMode: neo4j.AccessModeWrite,
				Bookmarks:  neo4j.BookmarksFromRawValues(bookmark + "0"),
			})
			return driver, session, bookmark
		}

		tearDown := func(session neo4j.SessionWithContext, driver neo4j.DriverWithContext) {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}

		inner.Run("the request should fail", func(t *testing.T) {
			driver, session, _ := setUp()
			defer tearDown(session, driver)

			tx, err := session.BeginTransaction(ctx)

			assertNil(t, tx)
			neo4jErr := err.(*neo4j.Neo4jError)
			if server.Version.GreaterThan(V4) {
				// The error is not retryable since it is on the wrong format
				assertEquals(t, neo4jErr.Code, "Neo.ClientError.Transaction.InvalidBookmark")
			} else {
				assertTrue(t, neo4jErr.IsRetriableTransient())
				assertStringContains(t, neo4jErr.Msg, "not up to the requested version")
			}
		})

	})
}
