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
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestSession(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}
	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	outer.Run("with read access mode", func(inner *testing.T) {
		var (
			err     error
			driver  neo4j.DriverWithContext
			session neo4j.SessionWithContext
			result  neo4j.ResultWithContext
			summary neo4j.ResultSummary
		)

		driver = server.Driver(func(c *config.Config) {
			c.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
		})
		assertNotNil(inner, driver)

		session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeRead})
		assertNotNil(inner, session)

		defer func() {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}()

		inner.Run("when a query is executed, it should run and return summary with correct statement", func(t *testing.T) {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
			result, err = session.Run(ctx, stmt, nil)
			assertNil(t, err)
			assertNotNil(t, result)

			summary, err = result.Consume(ctx)
			assertNil(t, err)
			assertNotNil(t, summary)

			assertFalse(t, result.Next(ctx))

			assertEquals(t, summary.Query().Text(), stmt)
			assertNil(t, summary.Query().Parameters())
		})

		inner.Run("when a query is executed, it should run and return summary with correct statement and params", func(t *testing.T) {
			stmt := "UNWIND RANGE(0, $x) AS n RETURN n"
			params := map[string]any{"x": 1000}
			result, err = session.Run(ctx, stmt, params)
			assertNil(t, err)
			assertNotNil(t, result)

			summary, err = result.Consume(ctx)
			assertNil(t, err)
			assertNotNil(t, summary)

			assertFalse(t, result.Next(ctx))

			assertEquals(t, summary.Query().Text(), stmt)
			assertEquals(t, summary.Query().Parameters(), params)
		})

		inner.Run("when a query is executed, it should run and return summary when consumed", func(t *testing.T) {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
			result, err = session.Run(ctx, stmt, nil)
			assertNil(t, err)
			assertNotNil(t, result)

			summary, err = result.Consume(ctx)
			assertNil(t, err)
			assertNotNil(t, summary)

			assertFalse(t, result.Next(ctx))

			assertEquals(t, summary.StatementType(), neo4j.StatementTypeReadOnly)
		})

		inner.Run("when an invalid query is executed, it should return error", func(t *testing.T) {
			stmt := "UNWIND RANGE(0,100) RETURN N"

			result, err = session.Run(ctx, stmt, nil)
			assertNil(t, result)
			neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")
		})

		inner.Run("when a fail-on-streaming query is executed, it should run and return error when consuming", func(t *testing.T) {
			stmt := "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / 0"

			result, err = session.Run(ctx, stmt, nil)
			// Up to the db when the error occurs
			if err != nil {
				assertNil(t, result)
				//Expect(err).To(BeArithmeticError())
				neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
				assertTrue(t, isNeo4jErr)
				assertEquals(t, neo4jErr.Classification(), "ClientError")
				return
			}
			assertNotNil(t, result)
			summary, err = result.Consume(ctx)
			neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")
			//Expect(err).To(BeArithmeticError())
			assertNil(t, summary)

			assertFalse(t, result.Next(ctx))
			neo4jErr, isNeo4jErr = err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")
			//Expect(result.Err()).To(BeArithmeticError())
		})

		inner.Run("when a query is executed, the returned summary should contain correct timer values", func(t *testing.T) {
			stmt := "UNWIND RANGE(0, 10000) AS N RETURN N"

			result, err = session.Run(ctx, stmt, nil)
			assertNil(t, err)

			summary, err = result.Consume(ctx)
			assertNil(t, err)
			assertTrue(t, summary.ResultAvailableAfter() >= 0)
			assertTrue(t, summary.ResultConsumedAfter() >= 0)
		})
	})

	outer.Run("with write access mode", func(inner *testing.T) {
		var (
			err     error
			driver  neo4j.DriverWithContext
			session neo4j.SessionWithContext
			result  neo4j.ResultWithContext
			summary neo4j.ResultSummary
		)

		driver = server.Driver()
		session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeWrite})

		defer func() {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}()

		inner.Run("when nested queries are executed, all queries should run and return results from all queries", func(t *testing.T) {
			initialPropertyCountResult, err := session.Run(ctx, "MATCH (p:Property) RETURN count(p)", nil)
			assertNil(t, err)
			initialPropertyCountValueRecord, err := initialPropertyCountResult.Single(ctx)
			assertNil(t, err)
			rawInitialPropertyCount, ok := initialPropertyCountValueRecord.Get("count(p)")
			assertTrue(t, ok)
			initialPropertyCount, ok := rawInitialPropertyCount.(int64)
			assertTrue(t, ok)
			initialResourceCountResult, err := session.Run(ctx, "MATCH (r:Resource) RETURN count(r)", nil)
			assertNil(t, err)
			initialResourceCountValueRecord, err := initialResourceCountResult.Single(ctx)
			assertNil(t, err)
			rawInitialResourceCount, ok := initialResourceCountValueRecord.Get("count(r)")
			assertTrue(t, ok)
			initialResourceCount, ok := rawInitialResourceCount.(int64)
			assertTrue(t, ok)
			result, err = session.Run(ctx, "UNWIND range(1, 100) AS x CREATE (:Property {id: x})", nil)
			assertNil(t, err)
			_, err = result.Consume(ctx)
			assertNil(t, err)

			result, err = session.Run(ctx, "UNWIND range(1, 10) AS x CREATE (:Resource {id: x})", nil)
			assertNil(t, err)
			_, err = result.Consume(ctx)
			assertNil(t, err)

			seenProps := 0
			seenResources := 0
			properties, err := session.Run(ctx, "MATCH (p:Property) RETURN p", nil)
			assertNil(t, err)
			for properties.Next(ctx) { // initialPropertyCount+100
				assertNotNil(t, properties.Record())
				seenProps++

				resources, err := session.Run(ctx, "MATCH (r:Resource) RETURN r", nil)
				assertNil(t, err)
				for resources.Next(ctx) { // initialResourceCount+10
					assertNotNil(t, resources.Record())
					seenResources++
				}
				assertNil(t, resources.Err())
			}
			assertNil(t, properties.Err())

			assertEquals(t, seenProps, initialPropertyCount+100)
			assertEquals(t, seenResources, (initialPropertyCount+100)*(initialResourceCount+10))
		})

		inner.Run("when a node is created, summary should contain correct counter values", func(t *testing.T) {
			result, err = session.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)

			summary, err = result.Consume(ctx)
			fmt.Printf("%+v", summary)
			assertNil(t, err)

			assertEquals(t, summary.Counters().NodesCreated(), 1)
			assertEquals(t, summary.Counters().NodesDeleted(), 0)
			assertEquals(t, summary.Counters().RelationshipsCreated(), 0)
			assertEquals(t, summary.Counters().RelationshipsDeleted(), 0)
			assertEquals(t, summary.Counters().PropertiesSet(), 1)
			assertEquals(t, summary.Counters().LabelsAdded(), 1)
			assertEquals(t, summary.Counters().LabelsRemoved(), 0)
			assertEquals(t, summary.Counters().IndexesAdded(), 0)
			assertEquals(t, summary.Counters().IndexesRemoved(), 0)
			assertEquals(t, summary.Counters().ConstraintsAdded(), 0)
			assertEquals(t, summary.Counters().ConstraintsRemoved(), 0)
		})

		inner.Run("when a node is created, summary should contain correct timer values", func(t *testing.T) {
			result, err = session.Run(ctx, "CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)

			summary, err = result.Consume(ctx)
			assertNil(t, err)
			assertTrue(t, summary.ResultAvailableAfter() >= 0)
			assertTrue(t, summary.ResultConsumedAfter() >= 0)
		})

		inner.Run("on multiple runs summary counters should be correct", func(t *testing.T) {
			result, _ = session.Run(ctx, "CREATE (p:Person { name: 'one'})", nil)
			summary, _ = result.Consume(ctx)
			counters := summary.Counters()

			assertEquals(t, counters.NodesCreated(), 1)
			assertEquals(t, counters.NodesDeleted(), 0)
			assertEquals(t, counters.RelationshipsCreated(), 0)
			assertEquals(t, counters.RelationshipsDeleted(), 0)

			result, _ = session.Run(ctx, "MATCH (p:Person { name: 'one'}) RETURN p", nil)
			summary, _ = result.Consume(ctx)
			counters = summary.Counters()

			assertEquals(t, counters.NodesCreated(), 0)
			assertEquals(t, counters.NodesDeleted(), 0)
			assertEquals(t, counters.RelationshipsCreated(), 0)
			assertEquals(t, counters.RelationshipsDeleted(), 0)
		})

		inner.Run("when one statement fails, the next one should run successfully", func(t *testing.T) {
			result, err = session.Run(ctx, "Invalid Cypher", nil)
			assertNil(t, result)
			neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")

			result, err = session.Run(ctx, "RETURN 1", nil)
			assertNil(t, err)

			if result.Next(ctx) {
				assertEquals(t, result.Record().Values[0], 1)
			}
			assertFalse(t, result.Next(ctx))
			assertNil(t, result.Err())
		})

		inner.Run("when two statements are queued, the second one should cause first one's results to be cached", func(t *testing.T) {
			result1, err := session.Run(ctx, "UNWIND RANGE(1,10) AS N RETURN N", nil)
			assertNil(t, err)
			result2, err := session.Run(ctx, "UNWIND RANGE(11,20) AS N RETURN N", nil)
			assertNil(t, err)

			result2Values := []int(nil)
			for result2.Next(ctx) {
				if val, ok := result2.Record().Get("N"); ok {
					result2Values = append(result2Values, int(val.(int64)))
				}
			}
			assertNil(t, result2.Err())

			result1Values := []int(nil)
			for result1.Next(ctx) {
				if val, ok := result1.Record().Get("N"); ok {
					result1Values = append(result1Values, int(val.(int64)))
				}
			}
			assertNil(t, result1.Err())

			assertEquals(t, result1Values, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
			assertEquals(t, result2Values, []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		})

		inner.Run("when session is closed, pending query result should be discarded", func(t *testing.T) {
			var result neo4j.ResultWithContext
			var err error

			innerExecutor := func() error {
				session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeRead})
				defer session.Close(ctx)

				if result, err = session.Run(ctx, "UNWIND RANGE(1,100) AS N RETURN N", nil); err != nil {
					return err
				}

				return nil
			}

			assertNil(t, innerExecutor())

			records, _ := neo4j.CollectWithContext(ctx, result, err)
			assertNotNil(t, records)
			assertEquals(t, len(records), 0)
		})

		inner.Run("when session is closed, last pending result shoud be discarded", func(t *testing.T) {
			var result1, result2 neo4j.ResultWithContext
			var err1, err2 error

			innerExecutor := func() error {
				session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeRead})
				defer session.Close(ctx)

				if result1, err1 = session.Run(ctx, "UNWIND RANGE(1,100) AS N RETURN N", nil); err != nil {
					return err
				}

				if result2, err2 = session.Run(ctx, "UNWIND RANGE(1,100) AS N RETURN 'Text ' + N", nil); err != nil {
					return err
				}

				return nil
			}

			assertNil(t, innerExecutor())

			records1, _ := neo4j.CollectWithContext(ctx, result1, err1)
			assertNotNil(t, records1)
			assertEquals(t, len(records1), 100)
			assertEquals(t, records1[0].Values[0], 1)
			assertEquals(t, records1[99].Values[0], 100)

			records2, _ := neo4j.CollectWithContext(ctx, result2, err2)
			assertNotNil(t, records2)
			assertEquals(t, len(records2), 0)
		})

		inner.Run("when session is closed, pending explicit transaction should be rolled-back", func(t *testing.T) {
			var (
				err      error
				records1 []*neo4j.Record
				records2 []*neo4j.Record
			)

			innerExecutor := func() error {
				var tx neo4j.ExplicitTransaction

				session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeWrite})
				defer session.Close(ctx)

				if tx, err = session.BeginTransaction(ctx); err != nil {
					return err
				}

				result1, err := tx.Run(ctx, "UNWIND RANGE(1,100) AS N CREATE (n:TxRollbackOnClose { id: N, text: 'Text '+N }) RETURN N", nil)
				records1, err = neo4j.CollectWithContext(ctx, result1, err)
				if err != nil {
					return err
				}

				result2, err := tx.Run(ctx, "MATCH (n:TxRollbackOnClose) RETURN n.id, n.text ORDER BY n.id", nil)
				records2, err = neo4j.CollectWithContext(ctx, result2, err)
				if err != nil {
					return err
				}

				return nil
			}

			assertNil(t, innerExecutor())
			assertNotNil(t, records1)
			assertEquals(t, len(records1), 100)
			assertEquals(t, records1[0].Values[0], 1)
			assertEquals(t, records1[99].Values[0], 100)

			assertNotNil(t, records2)
			assertEquals(t, len(records2), 100)
			assertEquals(t, records2[0].Values[0], 1)
			assertEquals(t, records2[0].Values[1], "Text 1")
			assertEquals(t, records2[99].Values[0], 100)
			assertEquals(t, records2[99].Values[1], "Text 100")

			newSession := driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeRead})
			assertNotNil(t, newSession)
			defer newSession.Close(ctx)

			result3, err := newSession.Run(ctx, "MATCH (n:TxRollbackOnClose) RETURN n.id, n.text", nil)
			records3, _ := neo4j.CollectWithContext(ctx, result3, err)
			assertEquals(t, len(records3), 0)
		})
	})

	outer.Run("V3", func(inner *testing.T) {
		var (
			driver  neo4j.DriverWithContext
			session neo4j.SessionWithContext
		)

		driver = server.Driver()
		assertNotNil(inner, driver)

		if server.Version.LessThan(V350) {
			inner.Skip("this test is targeted for server version after neo4j 3.5.0")
		}

		session = driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeWrite})
		assertNotNil(inner, session)

		defer func() {
			if session != nil {
				session.Close(ctx)
			}

			if driver != nil {
				driver.Close(ctx)
			}
		}()

		inner.Run("should set transaction metadata on Session.Run", func(t *testing.T) {
			metadata := map[string]any{
				"m1": int64(1),
				"m2": "some string",
				"m3": 4.0,
				"m4": neo4j.LocalDateTimeOf(time.Now()),
			}

			if !server.IsEnterprise {
				t.Skip("Can not list transactions on non-enterprise version")
			}

			matched, err := session.ExecuteRead(ctx, listTransactionsAndMatchMetadataWork(ctx, server.Version, metadata), config.WithTxMetadata(metadata))

			assertNil(t, err)
			assertTrue(t, matched.(bool))
		})

		inner.Run("should set transaction metadata on ExecuteRead", func(t *testing.T) {
			metadata := map[string]any{
				"m1": int64(1),
				"m2": "some string",
				"m3": neo4j.DateOf(time.Now()),
			}

			if !server.IsEnterprise {
				t.Skip("Can not list transactions on non-enterprise version")
			}

			matched, err := session.ExecuteRead(ctx, listTransactionsAndMatchMetadataWork(ctx, server.Version, metadata), config.WithTxMetadata(metadata))
			assertNil(t, err)
			assertTrue(t, matched.(bool))
		})

		inner.Run("should set transaction metadata on ExecuteWrite", func(t *testing.T) {
			metadata := map[string]any{
				"m1": true,
				"m2": []byte{0x00, 0x01, 0x02},
				"m3": neo4j.OffsetTimeOf(time.Now()),
			}

			if !server.IsEnterprise {
				t.Skip("Can not list transactions on non-enterprise version")
			}

			matched, err := session.ExecuteWrite(ctx, listTransactionsAndMatchMetadataWork(ctx, server.Version, metadata), config.WithTxMetadata(metadata))
			assertNil(t, err)
			assertTrue(t, matched.(bool))
		})

		inner.Run("should set transaction timeout", func(t *testing.T) {
			createNode(ctx, t, session, "RunTxTimeOut", nil)

			session2, tx2 := newSessionAndTx(ctx, t, driver, config.AccessModeWrite)
			defer session2.Close(ctx)
			defer tx2.Close(ctx)
			updateNodeInTx(ctx, t, tx2, "RunTxTimeOut", map[string]any{"id": 1})

			session3 := driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeWrite})

			result3, err := session3.Run(ctx, "MATCH (n:RunTxTimeOut) SET n.id = 2", nil, config.WithTxTimeout(1*time.Second))
			// Up to db to determine when error occurs
			if err != nil {
				dbErr := err.(*db.Neo4jError)
				assertStringContains(t, dbErr.Msg, "terminated")
				return
			}

			_, err = result3.Consume(ctx)
			// Should be a database error of class Transient indicating that the transaction
			// has been terminated. For some reason this should not be considered transient
			// by the IsTransientError.
			dbErr := err.(*db.Neo4jError)
			assertStringContains(t, dbErr.Msg, "terminated")
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})

		inner.Run("should set transaction timeout on ExecuteWrite", func(t *testing.T) {
			createNode(ctx, t, session, "WriteTransactionTxTimeOut", nil)

			session2, tx2 := newSessionAndTx(ctx, t, driver, config.AccessModeWrite)
			defer session2.Close(ctx)
			defer tx2.Close(ctx)
			updateNodeInTx(ctx, t, tx2, "WriteTransactionTxTimeOut", map[string]any{"id": 1})

			session3 := driver.NewSession(ctx, config.SessionConfig{AccessMode: config.AccessModeWrite})

			_, err := session3.ExecuteWrite(ctx, updateNodeWork(ctx, t, "WriteTransactionTxTimeOut", map[string]any{"id": 2}), config.WithTxTimeout(1*time.Second))
			assertNotNil(t, err)
			dbErr := err.(*db.Neo4jError)
			assertStringContains(t, dbErr.Msg, "terminated")
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})
	})
}
