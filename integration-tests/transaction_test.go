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
	"time"

	"github.com/neo4j-drivers/neo4j-go-connector"
	. "github.com/neo4j/neo4j-go-driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("Transaction", func() {
	var (
		err     error
		driver  Driver
		session *Session
		tx      *Transaction
		result  *Result
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

	singleResultWork := func(query string, params *map[string]interface{}) TransactionWork {
		return func(tx *Transaction) (interface{}, error) {
			create, err := tx.Run(query, params)
			Expect(err).To(BeNil())

			returnValue := 0
			if create.Next() {
				returnValue = int(create.Record().GetByIndex(0).(int64))
			}
			Expect(create.Next()).To(BeFalse())
			Expect(create.Err()).To(BeNil())

			return returnValue, nil
		}
	}

	writeAndGetSingleIntResult := func(work TransactionWork) int {
		result, err := session.WriteTransaction(work)
		Expect(err).To(BeNil())

		return result.(int)
	}

	readAndGetSingleIntResult := func(work TransactionWork) int {
		result, err := session.ReadTransaction(work)
		Expect(err).To(BeNil())

		return result.(int)
	}

	Context("Retry Mechanism", func() {
		transientError := seabolt.NewDatabaseError(map[string]interface{}{
			"code":    "Neo.TransientError.Transaction.Outdated",
			"message": "some transient error",
		})

		It("should work on WriteTransaction", func() {
			times := 0
			_, err = session.WriteTransaction(func(transaction *Transaction) (interface{}, error) {
				times++
				time.Sleep(1 * time.Second)
				return nil, transientError
			})

			Expect(err).To(Equal(transientError))
			Expect(times).To(BeNumerically(">", 10))
		})

		It("should work on ReadTransaction", func() {
			times := 0
			_, err = session.ReadTransaction(func(transaction *Transaction) (interface{}, error) {
				times++
				time.Sleep(1 * time.Second)
				return nil, transientError
			})

			Expect(err).To(Equal(transientError))
			Expect(times).To(BeNumerically(">", 10))
		})
	})

	It("should commit if work function doesn't return error", func() {
		createResult := writeAndGetSingleIntResult(singleResultWork("CREATE (n:Person1) RETURN count(n)", nil))
		Expect(createResult).To(BeEquivalentTo(1))

		matchResult := readAndGetSingleIntResult(singleResultWork("MATCH (n:Person1) RETURN count(n)", nil))
		Expect(matchResult).To(BeEquivalentTo(1))
	})

	It("should rollback if work function returns error", func() {
		createWork := singleResultWork("CREATE (n:Person2) RETURN count(n)", nil)
		createResult, err := session.WriteTransaction(func(tx *Transaction) (interface{}, error) {
			innerResult, err := createWork(tx)
			Expect(err).To(BeNil())
			Expect(innerResult).To(BeEquivalentTo(1))

			return nil, errors.New("some error")
		})
		Expect(err).NotTo(BeNil())
		Expect(createResult).To(BeNil())

		matchResult := readAndGetSingleIntResult(singleResultWork("MATCH (n:Person2) RETURN count(n)", nil))
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
})

