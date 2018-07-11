/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

package integration_tests

import (
	. "github.com/neo4j/neo4j-go-driver/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/neo4j/neo4j-go-driver"
)

var _ = Describe("SessionIT", func() {
	Context("with a read access mode session", func() {
		var (
			err     error
			driver  Driver
			session *Session
			result  *Result
			summary *ResultSummary
		)

		BeforeEach(func() {
			driver, err = NewDriver(SingleInstanceUri, BasicAuth(Username, Password, ""))
			if err != nil {
				Expect(err).To(BeNil())
			}

			session, err = driver.Session(AccessModeRead)
			if err != nil {
				Expect(err).To(BeNil())
			}
		})

		AfterEach(func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		})

		When("`UNWIND [1, 2, 3, 4, 5] AS x RETURN x` is sent", func() {
			stmt := "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"

			It("should run and return summary when consumed", func() {
				result, err = session.Run(stmt, nil)
				Expect(err).To(BeNil())
				Expect(result).NotTo(BeNil())

				summary, err = result.Consume()
				Expect(err).To(BeNil())
				Expect(summary).NotTo(BeNil())

				Expect(result.Next()).To(BeFalse())

				Expect(result.Err()).To(BeNil())

				Expect(summary.Statement().Cypher()).To(BeIdenticalTo(stmt))
				Expect(summary.Statement().Params()).To(BeNil())

				Expect(summary.StatementType()).To(BeIdenticalTo(StatementTypeReadOnly))
			})
		})

		When("`UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / x` is sent", func() {
			stmt := "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / 0"

			It("should run and return error when consuming", func() {
				result, err = session.Run(stmt, nil)
				Expect(err).To(BeNil())
				Expect(result).NotTo(BeNil())

				summary, err = result.Consume()
				Expect(err).To(BeArithmeticError())

				Expect(result.Next()).To(BeFalse())
				Expect(result.Err()).To(BeArithmeticError())

				Expect(summary.Statement().Cypher()).To(BeIdenticalTo(stmt))
				Expect(summary.Statement().Params()).To(BeNil())

				Expect(summary.StatementType()).To(BeIdenticalTo(StatementTypeReadOnly))
			})
		})

		When("a node is created", func() {
			Context("summary", func() {
				It("should contain correct counter values", func() {
					result, err = session.Run("CREATE (p:Person { Name: 'Test'})", nil)
					Expect(err).To(BeNil())

					summary, err = result.Consume()
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
			})
		})

		When("a node is created", func() {
			Context("summary", func() {
				It("should contain correct timer values", func() {
					result, err = session.Run("CREATE (p:Person { Name: 'Test'})", nil)
					Expect(err).To(BeNil())

					summary, err = result.Consume()
					Expect(err).To(BeNil())

					Expect(summary.ResultAvailableAfter()).To(BeNumerically(">=", 0))
					Expect(summary.ResultConsumedAfter()).To(BeNumerically(">=", 0))
				})
			})
		})
	})

	Context("with a write access mode session", func() {
		var (
			err     error
			driver  Driver
			session *Session
			result  *Result
		)

		BeforeEach(func() {
			driver, err = NewDriver(SingleInstanceUri, BasicAuth(Username, Password, ""))
			if err != nil {
				Expect(err).To(BeNil())
			}

			session, err = driver.Session(AccessModeWrite)
			if err != nil {
				Expect(err).To(BeNil())
			}
		})

		AfterEach(func() {
			if session != nil {
				session.Close()
			}

			if driver != nil {
				driver.Close()
			}
		})

		When("nested queries are executed", func() {
			Context("all queries", func() {
				It("should run and return from all queries", func() {
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
			})
		})

	})

})
