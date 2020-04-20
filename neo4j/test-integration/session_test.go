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
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"

	//. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Session", func() {
	var server *control.SingleInstance
	var err error

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())
	})

	Context("with read access mode", func() {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
			summary neo4j.ResultSummary
		)

		BeforeEach(func() {
			driver, err = server.Driver()
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			session, err = driver.Session(neo4j.AccessModeRead)
			Expect(err).To(BeNil())
			Expect(session).NotTo(BeNil())
		})

		AfterEach(func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		})

		Specify("when a query is executed, it should run and return summary with correct statement", func() {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())
			Expect(summary).NotTo(BeNil())

			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())

			Expect(summary.Statement().Text()).To(BeIdenticalTo(stmt))
			Expect(summary.Statement().Params()).To(BeNil())
		})

		Specify("when a query is executed, it should run and return summary with correct statement and params", func() {
			stmt := "UNWIND RANGE(0, $x) AS n RETURN n"
			params := map[string]interface{}{"x": 1000}
			result, err = session.Run(stmt, params)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())
			Expect(summary).NotTo(BeNil())

			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())

			Expect(summary.Statement().Text()).To(Equal(stmt))
			Expect(summary.Statement().Params()).To(Equal(params))
		})

		Specify("when a query is executed, it should run and return summary when consumed", func() {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())
			Expect(summary).NotTo(BeNil())

			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())

			Expect(summary.StatementType()).To(BeEquivalentTo(neo4j.StatementTypeReadOnly))
		})

		Specify("when an invalid query is executed, it should return error when consuming", func() {
			stmt := "UNWIND RANGE(0,100) RETURN N"

			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(neo4j.IsClientError(err)).To(BeTrue())
			//Expect(err).To(BeSyntaxError())
			Expect(summary).To(BeNil())

			Expect(result.Next()).To(BeFalse())
			Expect(neo4j.IsClientError(err)).To(BeTrue())
			//Expect(result.Err()).To(BeSyntaxError())
		})

		Specify("when a fail-on-streaming query is executed, it should run and return error when consuming", func() {
			stmt := "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / 0"

			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(neo4j.IsClientError(err)).To(BeTrue())
			//Expect(err).To(BeArithmeticError())
			Expect(summary).To(BeNil())

			Expect(result.Next()).To(BeFalse())
			Expect(neo4j.IsClientError(err)).To(BeTrue())
			//Expect(result.Err()).To(BeArithmeticError())
		})

		Specify("when a query is executed, the returned summary should contain correct timer values", func() {
			stmt := "UNWIND RANGE(0, 10000) AS N RETURN N"

			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())
			Expect(summary.ResultAvailableAfter()).To(BeNumerically(">=", 0))
			Expect(summary.ResultConsumedAfter()).To(BeNumerically(">=", 0))
		})
	})

	Context("with write access mode", func() {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
			summary neo4j.ResultSummary
		)

		BeforeEach(func() {
			driver, err = server.Driver()
			Expect(err).To(BeNil())

			session, err = driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		})

		Specify("when nested queries are executed, all queries should run and return results from all queries", func() {
			result, err = session.Run("UNWIND range(1, 100) AS x CREATE (:Property {id: x})", nil)
			Expect(err).To(BeNil())
			_, err = result.Consume()
			Expect(err).To(BeNil())

			result, err = session.Run("UNWIND range(1, 10) AS x CREATE (:Resource {id: x})", nil)
			Expect(err).To(BeNil())
			_, err = result.Consume()
			Expect(err).To(BeNil())

			seenProps := 0
			seenResources := 0
			properties, err := session.Run("MATCH (p:Property) RETURN p", nil)
			Expect(err).To(BeNil())
			for properties.Next() {
				Expect(properties.Record()).ToNot(BeNil())
				seenProps++

				resources, err := session.Run("MATCH (r:Resource) RETURN r", nil)
				Expect(err).To(BeNil())
				for resources.Next() {
					Expect(resources.Record()).ToNot(BeNil())
					seenResources++
				}
				Expect(resources.Err()).To(BeNil())
			}
			Expect(properties.Err()).To(BeNil())

			Expect(seenProps).To(BeIdenticalTo(100))
			Expect(seenResources).To(BeIdenticalTo(1000))
		})

		Specify("when a node is created, summary should contain correct counter values", func() {
			result, err = session.Run("CREATE (p:Person { name: 'Test'})", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			fmt.Printf("%+v", summary)
			Expect(err).To(BeNil())

			Expect(summary.Counters().NodesCreated()).To(BeIdenticalTo(1))
			Expect(summary.Counters().NodesDeleted()).To(BeZero())
			Expect(summary.Counters().RelationshipsCreated()).To(BeZero())
			Expect(summary.Counters().RelationshipsDeleted()).To(BeZero())
			Expect(summary.Counters().PropertiesSet()).To(BeIdenticalTo(1))
			Expect(summary.Counters().LabelsAdded()).To(BeIdenticalTo(1))
			Expect(summary.Counters().LabelsRemoved()).To(BeZero())
			Expect(summary.Counters().IndexesAdded()).To(BeZero())
			Expect(summary.Counters().IndexesRemoved()).To(BeZero())
			Expect(summary.Counters().ConstraintsAdded()).To(BeZero())
			Expect(summary.Counters().ConstraintsRemoved()).To(BeZero())
		})

		Specify("when a node is created, summary should contain correct timer values", func() {
			result, err = session.Run("CREATE (p:Person { name: 'Test'})", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())
			Expect(summary.ResultAvailableAfter()).To(BeNumerically(">=", 0))
			Expect(summary.ResultConsumedAfter()).To(BeNumerically(">=", 0))
		})

		Specify("when one statement fails, the next one should run successfully", func() {
			result, err = session.Run("Invalid Cypher", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(neo4j.IsClientError(err)).To(BeTrue())
			//Expect(err).To(BeSyntaxError())

			result, err = session.Run("RETURN 1", nil)
			Expect(err).To(BeNil())

			if result.Next() {
				Expect(result.Record().GetByIndex(0)).To(BeEquivalentTo(1))
			}
			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())
		})

		Specify("when two statements are queued, the second one should cause first one's results to be cached", func() {
			result1, err := session.Run("UNWIND RANGE(1,10) AS N RETURN N", nil)
			Expect(err).To(BeNil())
			result2, err := session.Run("UNWIND RANGE(11,20) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			result2Values := []int(nil)
			for result2.Next() {
				if val, ok := result2.Record().Get("N"); ok {
					result2Values = append(result2Values, int(val.(int64)))
				}
			}
			Expect(result2.Err()).To(BeNil())

			result1Values := []int(nil)
			for result1.Next() {
				if val, ok := result1.Record().Get("N"); ok {
					result1Values = append(result1Values, int(val.(int64)))
				}
			}
			Expect(result1.Err()).To(BeNil())

			Expect(result1Values).To(BeEquivalentTo([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
			Expect(result2Values).To(BeEquivalentTo([]int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}))
		})

		Specify("when session is closed, pending query should be executed", func() {
			var result neo4j.Result
			var err error

			innerExecutor := func() error {
				if session, err = driver.Session(neo4j.AccessModeRead); err != nil {
					return err
				}
				defer session.Close()

				if result, err = session.Run("UNWIND RANGE(1,100) AS N RETURN N", nil); err != nil {
					return err
				}

				return nil
			}

			Expect(innerExecutor()).To(Succeed())

			records, _ := neo4j.Collect(result, nil)
			Expect(records).NotTo(BeNil())
			Expect(records).To(HaveLen(100))
			Expect(records[0].GetByIndex(0)).To(BeEquivalentTo(1))
			Expect(records[99].GetByIndex(0)).To(BeEquivalentTo(100))
		})

		Specify("when session is closed, pending queries should be executed", func() {
			var result1, result2 neo4j.Result
			var err error

			innerExecutor := func() error {
				if session, err = driver.Session(neo4j.AccessModeRead); err != nil {
					return err
				}
				defer session.Close()

				if result1, err = session.Run("UNWIND RANGE(1,100) AS N RETURN N", nil); err != nil {
					return err
				}

				if result2, err = session.Run("UNWIND RANGE(1,100) AS N RETURN 'Text ' + N", nil); err != nil {
					return err
				}

				return nil
			}

			Expect(innerExecutor()).To(Succeed())

			records1, _ := neo4j.Collect(result1, nil)
			Expect(records1).NotTo(BeNil())
			Expect(records1).To(HaveLen(100))
			Expect(records1[0].GetByIndex(0)).To(BeEquivalentTo(1))
			Expect(records1[99].GetByIndex(0)).To(BeEquivalentTo(100))

			records2, _ := neo4j.Collect(result2, nil)
			Expect(records2).NotTo(BeNil())
			Expect(records2).To(HaveLen(100))
			Expect(records2[0].GetByIndex(0)).Should(Equal("Text 1"))
			Expect(records2[99].GetByIndex(0)).Should(Equal("Text 100"))
		})

		Specify("when session is closed, pending explicit transaction should be rolled-back", func() {
			var result1, result2 neo4j.Result
			var err error

			innerExecutor := func() error {
				var tx neo4j.Transaction

				if session, err = driver.Session(neo4j.AccessModeWrite); err != nil {
					return err
				}
				defer session.Close()

				if tx, err = session.BeginTransaction(); err != nil {
					return err
				}

				if result1, err = tx.Run("UNWIND RANGE(1,100) AS N CREATE (n:TxRollbackOnClose { id: N, text: 'Text '+N }) RETURN N", nil); err != nil {
					return err
				}

				if result2, err = tx.Run("MATCH (n:TxRollbackOnClose) RETURN n.id, n.text ORDER BY n.id", nil); err != nil {
					return err
				}

				return nil
			}

			Expect(innerExecutor()).To(Succeed())

			records1, _ := neo4j.Collect(result1, nil)
			Expect(records1).NotTo(BeNil())
			Expect(records1).To(HaveLen(100))
			Expect(records1[0].GetByIndex(0)).To(BeEquivalentTo(1))
			Expect(records1[99].GetByIndex(0)).To(BeEquivalentTo(100))

			records2, _ := neo4j.Collect(result2, nil)
			Expect(records2).NotTo(BeNil())
			Expect(records2).To(HaveLen(100))
			Expect(records2[0].GetByIndex(0)).To(BeEquivalentTo(1))
			Expect(records2[0].GetByIndex(1)).Should(Equal("Text 1"))
			Expect(records2[99].GetByIndex(0)).To(BeEquivalentTo(100))
			Expect(records2[99].GetByIndex(1)).Should(Equal("Text 100"))

			newSession, err := driver.Session(neo4j.AccessModeRead)
			Expect(err).To(BeNil())
			Expect(newSession).NotTo(BeNil())
			defer newSession.Close()

			records3, _ := neo4j.Collect(newSession.Run("MATCH (n:TxRollbackOnClose) RETURN n.id, n.text", nil))
			Expect(records3).To(HaveLen(0))
		})
	})

	Context("V3", func() {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
		)

		BeforeEach(func() {
			driver, err = server.Driver()
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			if versionOfDriver(driver).LessThan(V350) {
				Skip("this test is targeted for server version after neo4j 3.5.0")
			}

			session, err = driver.Session(neo4j.AccessModeRead)
			Expect(err).To(BeNil())
			Expect(session).NotTo(BeNil())
		})

		AfterEach(func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		})

		It("should set transaction metadata on Session.Run", func() {
			metadata := map[string]interface{}{
				"m1": int64(1),
				"m2": "some string",
				"m3": 4.0,
				"m4": neo4j.LocalDateTimeOf(time.Now()),
			}

			result, err = session.Run("CALL dbms.listTransactions()", nil, neo4j.WithTxMetadata(metadata))
			Expect(err).To(BeNil())

			var matched bool = false
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
			Expect(result.Err()).To(BeNil())

			Expect(matched).To(BeTrue(), fmt.Sprintf("dbms.listTransactions did not include a metadata of %v", metadata))
		})

		It("should set transaction metadata on ReadTransaction", func() {
			metadata := map[string]interface{}{
				"m1": int64(1),
				"m2": "some string",
				"m3": []interface{}{"a", "b", "c"},
				"m4": neo4j.DateOf(time.Now()),
			}

			matched, err := session.ReadTransaction(listTransactionsAndMatchMetadataWork(metadata), neo4j.WithTxMetadata(metadata))
			Expect(err).To(BeNil())
			Expect(matched).To(BeTrue(), fmt.Sprintf("dbms.listTransactions did not include a metadata of %v", metadata))
		})

		It("should set transaction metadata on WriteTransaction", func() {
			metadata := map[string]interface{}{
				"m1": true,
				"m2": []byte{0x00, 0x01, 0x02},
				"m3": []interface{}{"a", "b", "c"},
				"m4": neo4j.OffsetTimeOf(time.Now()),
			}

			matched, err := session.WriteTransaction(listTransactionsAndMatchMetadataWork(metadata), neo4j.WithTxMetadata(metadata))
			Expect(err).To(BeNil())
			Expect(matched).To(BeTrue(), fmt.Sprintf("dbms.listTransactions did not include a metadata of %v", metadata))
		})

		It("should set transaction timeout", func() {
			createNode(session, "RunTxTimeOut", nil)

			session2, tx2 := newSessionAndTx(driver, neo4j.AccessModeWrite)
			defer session2.Close()
			defer tx2.Close()
			updateNodeInTx(tx2, "RunTxTimeOut", map[string]interface{}{"id": 1})

			session3 := newSession(driver, neo4j.AccessModeWrite)

			result3, err := session3.Run("MATCH (n:RunTxTimeOut) SET n.id = 2", nil, neo4j.WithTxTimeout(1*time.Second))
			Expect(err).To(BeNil())

			_, err = result3.Consume()
			// Should be a database error of class Transient indicating that the transaction
			// has been terminated. For some reason this should not be considered transient
			// by the IsTransientError.
			dbErr := err.(*db.DatabaseError)
			Expect(dbErr.Msg).To(ContainSubstring("terminated"))
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})

		It("should set transaction timeout on WriteTransaction", func() {
			createNode(session, "WriteTransactionTxTimeOut", nil)

			session2, tx2 := newSessionAndTx(driver, neo4j.AccessModeWrite)
			defer session2.Close()
			defer tx2.Close()
			updateNodeInTx(tx2, "WriteTransactionTxTimeOut", map[string]interface{}{"id": 1})

			session3 := newSession(driver, neo4j.AccessModeWrite)

			_, err := session3.WriteTransaction(updateNodeWork("WriteTransactionTxTimeOut", map[string]interface{}{"id": 2}), neo4j.WithTxTimeout(1*time.Second))
			Expect(err).ToNot(BeNil())
			dbErr := err.(*db.DatabaseError)
			Expect(dbErr.Msg).To(ContainSubstring("terminated"))
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})
	})

	/*
		Context("V3 API on V1 & V2", func() {
			var (
				err     error
				driver  neo4j.Driver
				session neo4j.Session
			)

			metadata := map[string]interface{}{"id": 4, "name": "x"}

			BeforeEach(func() {
				driver, err = server.Driver()
				Expect(err).To(BeNil())
				Expect(driver).NotTo(BeNil())

				if versionOfDriver(driver).GreaterThanOrEqual(V350) {
					Skip("this test is targeted for server versions less than neo4j 3.5.0")
				}

				session, err = driver.Session(neo4j.AccessModeRead)
				Expect(err).To(BeNil())
				Expect(session).NotTo(BeNil())
			})

			AfterEach(func() {
				if session != nil {
					session.Close()
				}

				if driver != nil {
					driver.Close()
				}
			})

				It("should fail when transaction timeout is set for Session.Run", func() {
					_, err := session.Run("RETURN 1", nil, neo4j.WithTxTimeout(1*time.Second))
					Expect(err).To(BeConnectorErrorWithCode(0x504))
				})

				It("should fail when transaction timeout is set for Session.ReadTransaction", func() {
					_, err := session.ReadTransaction(createNodeWork("Test", nil), neo4j.WithTxTimeout(1*time.Second))
					Expect(err).To(BeConnectorErrorWithCode(0x504))
				})

				It("should fail when transaction timeout is set for Session.WriteTransaction", func() {
					_, err := session.WriteTransaction(createNodeWork("Test", nil), neo4j.WithTxTimeout(1*time.Second))
					Expect(err).To(BeConnectorErrorWithCode(0x504))
				})

				It("should fail when transaction metadata is set for Session.Run", func() {
					_, err := session.Run("RETURN 1", nil, neo4j.WithTxMetadata(metadata))
					Expect(err).To(BeConnectorErrorWithCode(0x504))
				})

				It("should fail when transaction metadata is set for Session.ReadTransaction", func() {
					_, err := session.ReadTransaction(createNodeWork("Test", nil), neo4j.WithTxMetadata(metadata))
					Expect(err).To(BeConnectorErrorWithCode(0x504))
				})

				It("should fail when transaction metadata is set for Session.WriteTransaction", func() {
					_, err := session.WriteTransaction(createNodeWork("Test", nil), neo4j.WithTxMetadata(metadata))
					Expect(err).To(BeConnectorErrorWithCode(0x504))
				})
		})
	*/

})
