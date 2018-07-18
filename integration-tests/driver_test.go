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

var _ = Describe("Driver", func() {

	Context("Direct", func() {
		var (
			err     error
			driver  Driver
			session *Session
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

		It("it should not allow work on existing sessions, after driver is closed", func() {
			result, err = session.Run("RETURN 1", nil)
			Expect(err).To(BeNil())

			if result.Next() {
				Expect(result.Record().GetByIndex(0)).To(BeEquivalentTo(1))
			}
			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())

			err := driver.Close()
			Expect(err).To(BeNil())

			_, err = session.Run("RETURN 1", nil)
			Expect(err).NotTo(BeNil())
		})

		It("it should not allow new sessions, after driver is closed", func() {
			result, err = session.Run("RETURN 1", nil)
			Expect(err).To(BeNil())

			if result.Next() {
				Expect(result.Record().GetByIndex(0)).To(BeEquivalentTo(1))
			}
			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())

			err := driver.Close()
			Expect(err).To(BeNil())

			_, err = driver.Session(AccessModeWrite)
			Expect(err).NotTo(BeNil())
		})

	})

	Context("Pooling", func() {
		var (
			err     error
			driver  Driver
		)

		BeforeEach(func() {
			driver, err = NewDriver(singleInstanceUri, BasicAuth(username, password, ""), func(config *Config) {
				config.MaxConnectionPoolSize = 2
			})
			if err != nil {
				Expect(err).To(BeNil())
			}
		})

		AfterEach(func() {
			if driver != nil {
				driver.Close()
			}
		})

		It("should return error when pool is full", func() {
			// Open connection 1
			session1, err := driver.Session(AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session1.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			// Open connection 2
			session2, err := driver.Session(AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session2.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			// Try opening connection 3
			session3, err := driver.Session(AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session3.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BePoolFullError())
		})
	})

})