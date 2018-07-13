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
	. "github.com/neo4j/neo4j-go-driver"
	. "github.com/neo4j/neo4j-go-driver/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Session", func() {
	Context("with read access mode", func() {
		var (
			err     error
			driver  Driver
			session *Session
			result  *Result
			summary *ResultSummary
		)

		BeforeEach(func() {
			driver, err = NewDriver(singleInstanceUri, BasicAuth(username, password, ""))
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			session, err = driver.Session(AccessModeRead)
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

			Expect(summary.Statement().Cypher()).To(BeIdenticalTo(stmt))
			Expect(summary.Statement().Params()).To(BeNil())
		})

		Specify("when a query is executed, it should run and return summary with correct statement and params", func() {
			stmt := "UNWIND RANGE(0, $x) AS n RETURN n"
			params := map[string]interface{}{"x": 1000}
			result, err = session.Run(stmt, &params)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())
			Expect(summary).NotTo(BeNil())

			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())

			Expect(summary.Statement().Cypher()).To(Equal(stmt))
			Expect(summary.Statement().Params()).To(Equal(&params))
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

			Expect(summary.StatementType()).To(BeEquivalentTo(StatementTypeReadOnly))
		})

		Specify("when an invalid query is executed, it should return error when consuming", func() {
			stmt := "UNWIND RANGE(0,100) RETURN N"

			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeSyntaxError())

			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeSyntaxError())

			Expect(summary.StatementType()).To(BeEquivalentTo(StatementTypeUnknown))
		})

		Specify("when a fail-on-streaming query is executed, it should run and return error when consuming", func() {
			stmt := "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / 0"

			result, err = session.Run(stmt, nil)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeArithmeticError())

			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeArithmeticError())

			Expect(summary.StatementType()).To(BeEquivalentTo(StatementTypeUnknown))
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
			driver  Driver
			session *Session
			result  *Result
			summary *ResultSummary
		)

		BeforeEach(func() {
			driver, err = NewDriver(singleInstanceUri, BasicAuth(username, password, ""))
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

		Specify("when a node is created, summary should contain correct timer values", func() {
			result, err = session.Run("CREATE (p:Person { Name: 'Test'})", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())

			Expect(summary.ResultAvailableAfter()).To(BeNumerically(">=", 0))
			Expect(summary.ResultConsumedAfter()).To(BeNumerically(">=", 0))
		})
	})

})
