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
	"math"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/control"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Driver", func() {
	var server *control.SingleInstance
	var err error

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())
	})

	Context("VerifyConnectivity", func() {
		It("should return nil upon good connection", func() {
			driver, _ := server.Driver()
			defer driver.Close()
			err := driver.VerifyConnectivity()
			Expect(err).To(BeNil())
		})

		It("should return error upon bad connection", func() {
			auth := neo4j.BasicAuth("bad user", "bad pass", "bad area")
			driver, err := neo4j.NewDriver(server.BoltURI(), auth, server.Config())
			Expect(err).To(BeNil())
			defer driver.Close()
			err = driver.VerifyConnectivity()
			Expect(err).ToNot(BeNil())
		})
	})

	Context("Direct", func() {
		var (
			err     error
			driver  neo4j.Driver
			session neo4j.Session
			result  neo4j.Result
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

			_, err = driver.Session(neo4j.AccessModeWrite)
			Expect(err).NotTo(BeNil())
		})

	})

	Context("Pooling without Connection Acquisition Timeout", func() {
		var (
			err    error
			driver neo4j.Driver
		)

		BeforeEach(func() {
			driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.Config(), func(config *neo4j.Config) {
				config.MaxConnectionPoolSize = 2
				config.ConnectionAcquisitionTimeout = 0
			})
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			if driver != nil {
				driver.Close()
			}
		})

		It("should return error when pool is full", func() {
			// Open connection 1
			session1, err := driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session1.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			// Open connection 2
			session2, err := driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session2.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			// Try opening connection 3
			session3, err := driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session3.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(neo4j.IsServiceUnavailable(err)).To(BeTrue())
			//Expect(err).To(BeConnectorErrorWithCode(0x600))
		})
	})

	Context("Pooling with Connection Acquisition Timeout", func() {
		var (
			err    error
			driver neo4j.Driver
		)

		BeforeEach(func() {
			driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.Config(), func(config *neo4j.Config) {
				config.MaxConnectionPoolSize = 2
				config.ConnectionAcquisitionTimeout = 10 * time.Second
			})
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			if driver != nil {
				driver.Close()
			}
		})

		It("should return error when pool is full", func() {
			// Open connection 1
			session1, err := driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session1.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			// Open connection 2
			session2, err := driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())

			_, err = session2.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			Expect(err).To(BeNil())

			// Try opening connection 3
			session3, err := driver.Session(neo4j.AccessModeWrite)
			Expect(err).To(BeNil())

			start := time.Now()
			_, err = session3.Run("UNWIND RANGE(1, 100) AS N RETURN N", nil)
			elapsed := time.Since(start)
			Expect(neo4j.IsServiceUnavailable(err)).To(BeTrue())
			//Expect(err).To(BeConnectorErrorWithCode(0x601))
			Expect(math.Round(float64(elapsed / time.Second))).To(BeNumerically(">=", 10))
		})
	})
})
