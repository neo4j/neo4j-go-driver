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
	"errors"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/dbserver"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bookmark", func() {
	server := dbserver.GetDbServer()

	createNodeInTx := func(driver neo4j.Driver) string {
		session, err := driver.Session(neo4j.AccessModeWrite)
		Expect(err).To(BeNil())
		defer session.Close()

		_, err = session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("CREATE ()", nil)
			Expect(err).To(BeNil())

			summary, err := result.Consume()
			Expect(err).To(BeNil())

			Expect(summary.Counters().NodesCreated()).To(Equal(1))

			return 0, nil
		})
		Expect(err).To(BeNil())

		return session.LastBookmark()
	}

	Context("session constructed with no bookmarks", func() {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
			summary neo4j.ResultSummary
		)

		BeforeEach(func() {
			driver = server.Driver()
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

		Specify("when a node is created in auto-commit mode, last bookmark should not be empty", func() {
			result, err = session.Run("CREATE (p:Person { name: 'Test'})", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())

			Expect(session.LastBookmark()).NotTo(BeEmpty())
		})

		Specify("when a node is created in explicit transaction and committed, last bookmark should not be empty", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())

			result, err = tx.Run("CREATE (p:Person { name: 'Test'})", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())

			err = tx.Commit()
			Expect(err).To(BeNil())

			Expect(session.LastBookmark()).NotTo(BeEmpty())
		})

		Specify("when a node is created in explicit transaction and rolled back, last bookmark should be empty", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())

			result, err = tx.Run("CREATE (p:Person { name: 'Test'})", nil)
			Expect(err).To(BeNil())

			summary, err = result.Consume()
			Expect(err).To(BeNil())

			err = tx.Rollback()
			Expect(err).To(BeNil())

			Expect(session.LastBookmark()).To(BeEmpty())
		})

		Specify("when a node is created in transaction function, last bookmark should not be empty", func() {
			result, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
				result, err := tx.Run("CREATE (p:Person { name: 'Test'})", nil)
				Expect(err).To(BeNil())

				summary, err = result.Consume()
				Expect(err).To(BeNil())

				return summary.Counters().NodesCreated(), nil
			})

			Expect(err).To(BeNil())
			Expect(result).To(Equal(1))
			Expect(session.LastBookmark()).NotTo(BeEmpty())
		})

		Specify("when a node is created in transaction function and rolled back, last bookmark should be empty", func() {
			failWith := errors.New("some error")
			result, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
				result, err := tx.Run("CREATE (p:Person { name: 'Test'})", nil)
				Expect(err).To(BeNil())

				summary, err = result.Consume()
				Expect(err).To(BeNil())

				return 0, failWith
			})

			Expect(err).To(Equal(failWith))
			Expect(result).To(BeNil())
			Expect(session.LastBookmark()).To(BeEmpty())
		})

		Specify("when a node is queried in transaction function, last bookmark should not be empty", func() {
			result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
				result, err := tx.Run("MATCH (p:Person) RETURN count(p)", nil)
				Expect(err).To(BeNil())

				count := 0
				for result.Next() {
					count++
				}
				Expect(result.Err()).To(BeNil())

				return count, nil
			})

			Expect(err).To(BeNil())
			Expect(result).To(Equal(1))
			Expect(session.LastBookmark()).NotTo(BeEmpty())
		})

		Specify("when a node is created in transaction function and rolled back, last bookmark should be empty", func() {
			failWith := errors.New("some error")
			result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
				result, err := tx.Run("MATCH (p:Person) RETURN count(p)", nil)
				Expect(err).To(BeNil())

				count := 0
				for result.Next() {
					count++
				}
				Expect(result.Err()).To(BeNil())

				return count, failWith
			})

			Expect(err).To(Equal(failWith))
			Expect(result).To(BeNil())
			Expect(session.LastBookmark()).To(BeEmpty())
		})
	})

	Context("session constructed with one bookmark", func() {
		var (
			err      error
			driver   neo4j.Driver
			session  neo4j.Session
			bookmark string
		)

		BeforeEach(func() {
			driver = server.Driver()

			bookmark = createNodeInTx(driver)

			session, err = driver.Session(neo4j.AccessModeWrite, bookmark)
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

		Specify("given bookmark should be reported back by the server after BEGIN", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())
			defer tx.Close()

			Expect(session.LastBookmark()).To(Equal(bookmark))
		})

		Specify("given bookmark should be accessible after ROLLBACK", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())
			defer tx.Close()

			_, err = tx.Run("CREATE ()", nil)
			Expect(err).To(BeNil())

			err = tx.Rollback()
			Expect(err).To(BeNil())

			Expect(session.LastBookmark()).To(Equal(bookmark))
		})

		Specify("given bookmark should be accessible when transaction fails", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())
			defer tx.Close()

			_, err = tx.Run("RETURN", nil)
			Expect(err).To(Not(BeNil()))

			err = tx.Close()
			Expect(err).To(Not(BeNil()))
			Expect(session.LastBookmark()).To(Equal(bookmark))
		})

		Specify("given bookmark should be accessible after run", func() {
			result, err := session.Run("RETURN 1", nil)
			Expect(err).To(BeNil())

			_, err = result.Consume()
			Expect(err).To(BeNil())

			Expect(session.LastBookmark()).To(Equal(bookmark))
		})

		Specify("given bookmark should be accessible after failed run", func() {
			_, err := session.Run("RETURN", nil)
			Expect(err).To(Not(BeNil()))

			Expect(session.LastBookmark()).To(Equal(bookmark))
		})

	})

	Context("session constructed with two bookmarks", func() {
		var (
			err       error
			driver    neo4j.Driver
			session   neo4j.Session
			bookmark1 string
			bookmark2 string
		)

		BeforeEach(func() {
			driver = server.Driver()

			bookmark1 = createNodeInTx(driver)
			bookmark2 = createNodeInTx(driver)
			Expect(bookmark1).NotTo(Equal(bookmark2))

			session, err = driver.Session(neo4j.AccessModeWrite, bookmark1, bookmark2)
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

		Specify("highest bookmark should be reported back by the server after BEGIN", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())
			defer tx.Close()

			Expect(session.LastBookmark()).To(Equal(bookmark2))
		})

		Specify("new bookmark should be reported back by the server after committing", func() {
			tx, err := session.BeginTransaction()
			Expect(err).To(BeNil())
			defer tx.Close()

			result, err := tx.Run("CREATE ()", nil)
			Expect(err).To(BeNil())

			summary, err := result.Consume()
			Expect(err).To(BeNil())
			Expect(summary.Counters().NodesCreated()).To(Equal(1))

			err = tx.Commit()
			Expect(err).To(BeNil())

			Expect(session.LastBookmark()).ToNot(BeNil())
			Expect(session.LastBookmark()).ToNot(Equal(bookmark1))
			Expect(session.LastBookmark()).ToNot(Equal(bookmark2))
		})
	})

	Context("session constructed with unreachable bookmark", func() {
		var (
			err      error
			driver   neo4j.Driver
			session  neo4j.Session
			bookmark string
		)

		BeforeEach(func() {
			driver = server.Driver()

			bookmark = createNodeInTx(driver)

			session, err = driver.Session(neo4j.AccessModeWrite, bookmark+"0")
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

		Specify("the request should fail", func() {
			tx, err := session.BeginTransaction()

			Expect(tx).To(BeNil())
			neo4jErr := err.(*neo4j.Neo4jError)
			if server.Version.GreaterThan(V4) {
				// The error is not retriable since it is on the wrong format
				Expect(neo4jErr.Code, Equal("Neo.ClientError.Transaction.InvalidBookmark"))
			} else {
				Expect(neo4jErr.IsRetriableTransient()).To(BeTrue())
				Expect(neo4jErr.Msg).To(ContainSubstring("not up to the requested version"))
			}
		})
	})

})
