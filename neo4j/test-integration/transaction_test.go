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
	"time"

	//"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/utils"
	"github.com/pkg/errors"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"

	"github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transaction", func() {
	var server *control.SingleInstance
	var err error
	var driver neo4j.Driver
	var session neo4j.Session
	var tx neo4j.Transaction
	var result neo4j.Result

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())

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

	Context("Retry Mechanism", func() {
		transientError := test.NewDatabaseErrorForTest("Neo.TransientError.Transaction.Outdated")

		It("should work on WriteTransaction", func() {
			times := 0
			_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
				times++
				time.Sleep(1 * time.Second)
				return nil, transientError
			})

			Expect(err).ToNot(BeNil())
			//Expect(err).To(BeGenericError(And(ContainSubstring("retryable operation failed to complete after"), ContainSubstring("Neo.TransientError.Transaction.Outdated"))))
			Expect(times).To(BeNumerically(">", 10))
		})

		It("should work on ReadTransaction", func() {
			times := 0
			_, err = session.ReadTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
				times++
				time.Sleep(1 * time.Second)
				return nil, transientError
			})

			Expect(err).ToNot(BeNil())
			//Expect(err).To(BeGenericError(And(ContainSubstring("retryable operation failed to complete after"), ContainSubstring("Neo.TransientError.Transaction.Outdated"))))
			Expect(times).To(BeNumerically(">", 10))
		})
	})

	It("should commit if work function doesn't return error", func() {
		createResult := writeTransactionWithIntWork(session, intReturningWork("CREATE (n:Person1) RETURN count(n)", nil))
		Expect(createResult).To(BeEquivalentTo(1))

		matchResult := readTransactionWithIntWork(session, intReturningWork("MATCH (n:Person1) RETURN count(n)", nil))
		Expect(matchResult).To(BeEquivalentTo(1))
	})

	It("should rollback if work function returns error", func() {
		createWork := intReturningWork("CREATE (n:Person2) RETURN count(n)", nil)
		createResult, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			innerResult, err := createWork(tx)
			Expect(err).To(BeNil())
			Expect(innerResult).To(BeEquivalentTo(1))

			return nil, errors.New("some error")
		})
		Expect(err).NotTo(BeNil())
		Expect(createResult).To(BeNil())

		matchResult := readTransactionWithIntWork(session, intReturningWork("MATCH (n:Person2) RETURN count(n)", nil))
		Expect(matchResult).To(BeEquivalentTo(0))
	})

	It("should have keys available after run", func() {
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil())
		defer tx.Close()

		result, err = tx.Run("RETURN 1 AS N, 2 AS M", nil)
		Expect(err).To(BeNil())

		Expect(result.Keys()).To(BeEquivalentTo([]string{"N", "M"}))
	})

	It("should have keys available after run and consume", func() {
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil())
		defer tx.Close()

		result, err = tx.Run("RETURN 1 AS N, 2 AS M", nil)
		Expect(err).To(BeNil())

		Expect(result.Keys()).To(BeEquivalentTo([]string{"N", "M"}))

		_, err = result.Consume()
		Expect(err).To(BeNil())

		Expect(result.Keys()).To(BeEquivalentTo([]string{"N", "M"}))
	})

	It("should have keys available for consecutive runs", func() {
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil())
		defer tx.Close()

		result1, err := tx.Run("RETURN 1 AS N, 2 AS M", nil)
		Expect(err).To(BeNil())

		result2, err := tx.Run("RETURN 1 AS X, 2 AS Y", nil)
		Expect(err).To(BeNil())

		Expect(result1.Keys()).To(BeEquivalentTo([]string{"N", "M"}))
		Expect(result2.Keys()).To(BeEquivalentTo([]string{"X", "Y"}))
	})

	It("should have keys available for consecutive runs and consumes", func() {
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil())
		defer tx.Close()

		result1, err := tx.Run("RETURN 1 AS N, 2 AS M", nil)
		Expect(err).To(BeNil())

		result2, err := tx.Run("RETURN 1 AS X, 2 AS Y", nil)
		Expect(err).To(BeNil())

		_, err = result1.Consume()
		Expect(err).To(BeNil())
		_, err = result2.Consume()
		Expect(err).To(BeNil())

		Expect(result1.Keys()).To(BeEquivalentTo([]string{"N", "M"}))
		Expect(result2.Keys()).To(BeEquivalentTo([]string{"X", "Y"}))
	})

	It("should have keys available for consecutive runs independent of order", func() {
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil())
		defer tx.Close()

		result1, err := tx.Run("RETURN 1 AS N, 2 AS M", nil)
		Expect(err).To(BeNil())

		result2, err := tx.Run("RETURN 1 AS X, 2 AS Y", nil)
		Expect(err).To(BeNil())

		Expect(result2.Keys()).To(BeEquivalentTo([]string{"X", "Y"}))
		Expect(result1.Keys()).To(BeEquivalentTo([]string{"N", "M"}))
	})

	It("should have keys available for consecutive runs and consumes independent of order", func() {
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil())
		defer tx.Close()

		result1, err := tx.Run("RETURN 1 AS N, 2 AS M", nil)
		Expect(err).To(BeNil())

		result2, err := tx.Run("RETURN 1 AS X, 2 AS Y", nil)
		Expect(err).To(BeNil())

		_, err = result1.Consume()
		Expect(err).To(BeNil())
		_, err = result2.Consume()
		Expect(err).To(BeNil())

		Expect(result2.Keys()).To(BeEquivalentTo([]string{"X", "Y"}))
		Expect(result1.Keys()).To(BeEquivalentTo([]string{"N", "M"}))
	})

	Context("V3", func() {

		BeforeEach(func() {
			if versionOfDriver(driver).LessThan(V350) {
				Skip("this test is targeted for server version after neo4j 3.5.0")
			}
		})

		It("should set transaction metadata", func() {
			metadata := map[string]interface{}{
				"m1": int64(1),
				"m2": "some string",
				"m3": 4.0,
				"m4": neo4j.LocalDateTimeOf(time.Now()),
			}

			tx, err = session.BeginTransaction(neo4j.WithTxMetadata(metadata))
			Expect(err).To(BeNil())
			defer tx.Close()

			number := transactionWithIntWork(tx, intReturningWork("RETURN $x", map[string]interface{}{"x": 1}))
			Expect(number).To(BeEquivalentTo(1))

			session2 := newSession(driver, neo4j.AccessModeRead)
			defer session2.Close()
			matched, err := session2.ReadTransaction(listTransactionsAndMatchMetadataWork(metadata))
			Expect(err).To(BeNil())
			Expect(matched).To(BeTrue())
		})

		It("should set transaction timeout", func() {
			createNode(session, "TxTimeOut", nil)

			session2, tx2 := newSessionAndTx(driver, neo4j.AccessModeWrite)
			defer session2.Close()
			defer tx2.Close()

			updateNodeInTx(tx2, "TxTimeOut", map[string]interface{}{"id": 1})

			session3, tx3 := newSessionAndTx(driver, neo4j.AccessModeWrite, neo4j.WithTxTimeout(1*time.Second))
			defer session3.Close()
			defer tx3.Close()

			_, err := updateNodeWork("TxTimeOut", map[string]interface{}{"id": 2})(tx3)
			Expect(err).ToNot(BeNil())
			//Expect(err).To(BeTransientError(nil, ContainSubstring("terminated")))
		})

	})

	Context("V3 API on V1 & V2", func() {
		BeforeEach(func() {
			if versionOfDriver(driver).GreaterThanOrEqual(V350) {
				Skip("this test is targeted for server versions less than neo4j 3.5.0")
			}
		})

		It("should fail when transaction timeout is set for Session.BeginTransaction", func() {
			_, err := session.BeginTransaction(neo4j.WithTxTimeout(1 * time.Second))
			Expect(err).ToNot(BeNil())
			//Expect(err).To(BeConnectorErrorWithCode(0x504))
		})

		It("should fail when transaction metadata is set for Session.BeginTransaction", func() {
			_, err := session.BeginTransaction(neo4j.WithTxMetadata(map[string]interface{}{"x": 1}))
			Expect(err).ToNot(BeNil())
			//Expect(err).To(BeConnectorErrorWithCode(0x504))
		})
	})
})
