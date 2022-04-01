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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestSession(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}
	server := dbserver.GetDbServer()

	outer.Run("with read access mode", func(inner *testing.T) {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
			summary neo4j.ResultSummary
		)

		driver = server.Driver(func(c *neo4j.Config) {
			c.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
		})
		assertNotNil(inner, driver)

		session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
		assertNotNil(inner, session)

		defer func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		}()

		inner.Run("when a query is executed, it should run and return summary with correct statement", func(t *testing.T) {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
			result, err = session.Run(stmt, nil)
			assertNil(t, err)
			assertNotNil(t, result)

			summary, err = result.Consume()
			assertNil(t, err)
			assertNotNil(t, summary)

			assertFalse(t, result.Next())
			assertNil(t, result.Err())

			assertEquals(t, summary.Query().Text(), stmt)
			assertNil(t, summary.Query().Parameters())
		})

		inner.Run("when a query is executed, it should run and return summary with correct statement and params", func(t *testing.T) {
			stmt := "UNWIND RANGE(0, $x) AS n RETURN n"
			params := map[string]interface{}{"x": 1000}
			result, err = session.Run(stmt, params)
			assertNil(t, err)
			assertNotNil(t, result)

			summary, err = result.Consume()
			assertNil(t, err)
			assertNotNil(t, summary)

			assertFalse(t, result.Next())
			assertNil(t, result.Err())

			assertEquals(t, summary.Query().Text(), stmt)
			assertEquals(t, summary.Query().Parameters(), params)
		})

		inner.Run("when a query is executed, it should run and return summary when consumed", func(t *testing.T) {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
			result, err = session.Run(stmt, nil)
			assertNil(t, err)
			assertNotNil(t, result)

			summary, err = result.Consume()
			assertNil(t, err)
			assertNotNil(t, summary)

			assertFalse(t, result.Next())
			assertNil(t, result.Err())

			assertEquals(t, summary.StatementType(), neo4j.StatementTypeReadOnly)
		})

		inner.Run("when an invalid query is executed, it should return error", func(t *testing.T) {
			stmt := "UNWIND RANGE(0,100) RETURN N"

			result, err = session.Run(stmt, nil)
			assertNil(t, result)
			neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")
		})

		inner.Run("when a fail-on-streaming query is executed, it should run and return error when consuming", func(t *testing.T) {
			stmt := "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / 0"

			result, err = session.Run(stmt, nil)
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
			summary, err = result.Consume()
			neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")
			//Expect(err).To(BeArithmeticError())
			assertNil(t, summary)

			assertFalse(t, result.Next())
			neo4jErr, isNeo4jErr = err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")
			//Expect(result.Err()).To(BeArithmeticError())
		})

		inner.Run("when a query is executed, the returned summary should contain correct timer values", func(t *testing.T) {
			stmt := "UNWIND RANGE(0, 10000) AS N RETURN N"

			result, err = session.Run(stmt, nil)
			assertNil(t, err)

			summary, err = result.Consume()
			assertNil(t, err)
			assertTrue(t, summary.ResultAvailableAfter() >= 0)
			assertTrue(t, summary.ResultConsumedAfter() >= 0)
		})
	})

	outer.Run("with write access mode", func(inner *testing.T) {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
			summary neo4j.ResultSummary
		)

		driver = server.Driver()
		session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

		defer func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		}()

		inner.Run("when nested queries are executed, all queries should run and return results from all queries", func(t *testing.T) {
			result, err = session.Run("UNWIND range(1, 100) AS x CREATE (:Property {id: x})", nil)
			assertNil(t, err)
			_, err = result.Consume()
			assertNil(t, err)

			result, err = session.Run("UNWIND range(1, 10) AS x CREATE (:Resource {id: x})", nil)
			assertNil(t, err)
			_, err = result.Consume()
			assertNil(t, err)

			seenProps := 0
			seenResources := 0
			properties, err := session.Run("MATCH (p:Property) RETURN p", nil)
			assertNil(t, err)
			for properties.Next() {
				assertNotNil(t, properties.Record())
				seenProps++

				resources, err := session.Run("MATCH (r:Resource) RETURN r", nil)
				assertNil(t, err)
				for resources.Next() {
					assertNotNil(t, resources.Record())
					seenResources++
				}
				assertNil(t, resources.Err())
			}
			assertNil(t, properties.Err())

			assertEquals(t, seenProps, 100)
			assertEquals(t, seenResources, 1000)
		})

		inner.Run("when a node is created, summary should contain correct counter values", func(t *testing.T) {
			result, err = session.Run("CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)

			summary, err = result.Consume()
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
			result, err = session.Run("CREATE (p:Person { name: 'Test'})", nil)
			assertNil(t, err)

			summary, err = result.Consume()
			assertNil(t, err)
			assertTrue(t, summary.ResultAvailableAfter() >= 0)
			assertTrue(t, summary.ResultConsumedAfter() >= 0)
		})

		inner.Run("on multiple runs summary counters should be correct", func(t *testing.T) {
			result, _ = session.Run("CREATE (p:Person { name: 'one'})", nil)
			summary, _ = result.Consume()
			counters := summary.Counters()

			assertEquals(t, counters.NodesCreated(), 1)
			assertEquals(t, counters.NodesDeleted(), 0)
			assertEquals(t, counters.RelationshipsCreated(), 0)
			assertEquals(t, counters.RelationshipsDeleted(), 0)

			result, _ = session.Run("MATCH (p:Person { name: 'one'}) RETURN p", nil)
			summary, _ = result.Consume()
			counters = summary.Counters()

			assertEquals(t, counters.NodesCreated(), 0)
			assertEquals(t, counters.NodesDeleted(), 0)
			assertEquals(t, counters.RelationshipsCreated(), 0)
			assertEquals(t, counters.RelationshipsDeleted(), 0)
		})

		inner.Run("when one statement fails, the next one should run successfully", func(t *testing.T) {
			result, err = session.Run("Invalid Cypher", nil)
			assertNil(t, result)
			neo4jErr, isNeo4jErr := err.(*neo4j.Neo4jError)
			assertTrue(t, isNeo4jErr)
			assertEquals(t, neo4jErr.Classification(), "ClientError")

			result, err = session.Run("RETURN 1", nil)
			assertNil(t, err)

			if result.Next() {
				assertEquals(t, result.Record().Values[0], 1)
			}
			assertFalse(t, result.Next())
			assertNil(t, result.Err())
		})

		inner.Run("when two statements are queued, the second one should cause first one's results to be cached", func(t *testing.T) {
			result1, err := session.Run("UNWIND RANGE(1,10) AS N RETURN N", nil)
			assertNil(t, err)
			result2, err := session.Run("UNWIND RANGE(11,20) AS N RETURN N", nil)
			assertNil(t, err)

			result2Values := []int(nil)
			for result2.Next() {
				if val, ok := result2.Record().Get("N"); ok {
					result2Values = append(result2Values, int(val.(int64)))
				}
			}
			assertNil(t, result2.Err())

			result1Values := []int(nil)
			for result1.Next() {
				if val, ok := result1.Record().Get("N"); ok {
					result1Values = append(result1Values, int(val.(int64)))
				}
			}
			assertNil(t, result1.Err())

			assertEquals(t, result1Values, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
			assertEquals(t, result2Values, []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		})

		inner.Run("when session is closed, pending query result should be discarded", func(t *testing.T) {
			var result neo4j.Result
			var err error

			innerExecutor := func() error {
				session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
				defer session.Close()

				if result, err = session.Run("UNWIND RANGE(1,100) AS N RETURN N", nil); err != nil {
					return err
				}

				return nil
			}

			assertNil(t, innerExecutor())

			records, _ := neo4j.Collect(result, nil)
			assertNotNil(t, records)
			assertEquals(t, len(records), 0)
		})

		inner.Run("when session is closed, last pending result shoud be discarded", func(t *testing.T) {
			var result1, result2 neo4j.Result
			var err error

			innerExecutor := func() error {
				session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
				defer session.Close()

				if result1, err = session.Run("UNWIND RANGE(1,100) AS N RETURN N", nil); err != nil {
					return err
				}

				if result2, err = session.Run("UNWIND RANGE(1,100) AS N RETURN 'Text ' + N", nil); err != nil {
					return err
				}

				return nil
			}

			assertNil(t, innerExecutor())

			records1, _ := neo4j.Collect(result1, nil)
			assertNotNil(t, records1)
			assertEquals(t, len(records1), 100)
			assertEquals(t, records1[0].Values[0], 1)
			assertEquals(t, records1[99].Values[0], 100)

			records2, _ := neo4j.Collect(result2, nil)
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
				var tx neo4j.Transaction

				session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
				defer session.Close()

				if tx, err = session.BeginTransaction(); err != nil {
					return err
				}

				records1, err = neo4j.Collect(
					tx.Run("UNWIND RANGE(1,100) AS N CREATE (n:TxRollbackOnClose { id: N, text: 'Text '+N }) RETURN N", nil))
				if err != nil {
					return err
				}

				records2, err = neo4j.Collect(
					tx.Run("MATCH (n:TxRollbackOnClose) RETURN n.id, n.text ORDER BY n.id", nil))
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

			newSession := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
			assertNotNil(t, newSession)
			defer newSession.Close()

			records3, _ := neo4j.Collect(newSession.Run("MATCH (n:TxRollbackOnClose) RETURN n.id, n.text", nil))
			assertEquals(t, len(records3), 0)
		})
	})

	outer.Run("V3", func(inner *testing.T) {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
		)

		driver = server.Driver()
		assertNotNil(inner, driver)

		if server.Version.LessThan(V350) {
			inner.Skip("this test is targeted for server version after neo4j 3.5.0")
		}

		session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		assertNotNil(inner, session)

		defer func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		}()

		inner.Run("should set transaction metadata on Session.Run", func(t *testing.T) {
			metadata := map[string]interface{}{
				"m1": int64(1),
				"m2": "some string",
				"m3": 4.0,
				"m4": neo4j.LocalDateTimeOf(time.Now()),
			}

			if !server.IsEnterprise {
				t.Skip("Can not use dbms.listTransactions on non-enterprise version")
			}

			result, err = session.Run("CALL dbms.listTransactions()", nil, neo4j.WithTxMetadata(metadata))
			assertNil(t, err)

			matched := false
			for result.Next() {
				if txMetadataInt, ok := result.Record().Get("metaData"); ok {
					if txMetadata, ok := txMetadataInt.(map[string]interface{}); ok {
						if reflect.DeepEqual(metadata, txMetadata) {
							matched = true
							break
						}
					}
				}
			}
			assertNil(t, result.Err())

			assertTrue(t, matched)
		})

		inner.Run("should set transaction metadata on ExecuteRead", func(t *testing.T) {
			metadata := map[string]interface{}{
				"m1": int64(1),
				"m2": "some string",
				"m3": []interface{}{"a", "b", "c"},
				"m4": neo4j.DateOf(time.Now()),
			}

			if !server.IsEnterprise {
				t.Skip("Can not use dbms.listTransactions on non-enterprise version")
			}

			matched, err := session.ReadTransaction(listTransactionsAndMatchMetadataWork(metadata), neo4j.WithTxMetadata(metadata))
			assertNil(t, err)
			assertTrue(t, matched.(bool))
		})

		inner.Run("should set transaction metadata on ExecuteWrite", func(t *testing.T) {
			metadata := map[string]interface{}{
				"m1": true,
				"m2": []byte{0x00, 0x01, 0x02},
				"m3": []interface{}{"a", "b", "c"},
				"m4": neo4j.OffsetTimeOf(time.Now()),
			}

			if !server.IsEnterprise {
				t.Skip("Can not use dbms.listTransactions on non-enterprise version")
			}

			matched, err := session.WriteTransaction(listTransactionsAndMatchMetadataWork(metadata), neo4j.WithTxMetadata(metadata))
			assertNil(t, err)
			assertTrue(t, matched.(bool))
		})

		inner.Run("should set transaction timeout", func(t *testing.T) {
			createNode(t, session, "RunTxTimeOut", nil)

			session2, tx2 := newSessionAndTx(t, driver, neo4j.AccessModeWrite)
			defer session2.Close()
			defer tx2.Close()
			updateNodeInTx(t, tx2, "RunTxTimeOut", map[string]interface{}{"id": 1})

			session3 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			result3, err := session3.Run("MATCH (n:RunTxTimeOut) SET n.id = 2", nil, neo4j.WithTxTimeout(1*time.Second))
			// Up to db to determine when error occurs
			if err != nil {
				dbErr := err.(*db.Neo4jError)
				assertStringContains(t, dbErr.Msg, "terminated")
				return
			}

			_, err = result3.Consume()
			// Should be a database error of class Transient indicating that the transaction
			// has been terminated. For some reason this should not be considered transient
			// by the IsTransientError.
			dbErr := err.(*db.Neo4jError)
			assertStringContains(t, dbErr.Msg, "terminated")
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})

		inner.Run("should set transaction timeout on ExecuteWrite", func(t *testing.T) {
			createNode(t, session, "WriteTransactionTxTimeOut", nil)

			session2, tx2 := newSessionAndTx(t, driver, neo4j.AccessModeWrite)
			defer session2.Close()
			defer tx2.Close()
			updateNodeInTx(t, tx2, "WriteTransactionTxTimeOut", map[string]interface{}{"id": 1})

			session3 := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

			_, err := session3.WriteTransaction(updateNodeWork(t, "WriteTransactionTxTimeOut", map[string]interface{}{"id": 2}), neo4j.WithTxTimeout(1*time.Second))
			assertNotNil(t, err)
			dbErr := err.(*db.Neo4jError)
			assertStringContains(t, dbErr.Msg, "terminated")
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})
	})
}
