/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test_integration

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Timeout and Lifetime", func() {
	var err error
	var log *utils.MemoryLogging
	var server *control.SingleInstance

	BeforeEach(func() {
		log = &utils.MemoryLogging{}

		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())
	})

	It("should error when ConnectionAcquisitionTimeout is hit", func() {
		var err error
		var driver neo4j.Driver
		var session1, session2 neo4j.Session

		driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.Config(), func(config *neo4j.Config) {
			config.Log = log
			config.ConnectionAcquisitionTimeout = 1 * time.Second
			config.MaxConnectionPoolSize = 1
		})
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())
		defer driver.Close()

		session1, _ = newSessionAndTx(driver, neo4j.AccessModeRead)
		defer session1.Close()

		session2, err = driver.Session(neo4j.AccessModeRead)
		Expect(err).To(BeNil())
		Expect(session2).NotTo(BeNil())
		defer session2.Close()

		_, err = session2.Run("RETURN 1", nil)
		Expect(err).ToNot(BeNil())
		//Expect(err).To(test.BeConnectorErrorWithCode(0x601))
	})

	/*
		It("should close connection when MaxConnectionLifetime is hit", func() {
			var err error
			var driver neo4j.Driver
			var session1, session2 neo4j.Session

			driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.Config(), func(config *neo4j.Config) {
				config.Log = log
				config.MaxConnectionLifetime = 5 * time.Second
				config.MaxConnectionPoolSize = 1
			})
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())
			defer driver.Close()

			session1, _ = newSessionAndTx(driver, neo4j.AccessModeRead)
			time.Sleep(5 * time.Second)
			session1.Close()

			session2, _ = newSessionAndTx(driver, neo4j.AccessModeRead)
			defer session2.Close()

			Skip("Log not impl")
			Expect(log.Infos).Should(ContainElement(ContainSubstring("reached its maximum lifetime")))
		})
	*/

	It("should timeout connection when SocketConnectTimeout is hit", func() {
		var err error
		var driver neo4j.Driver
		var session neo4j.Session

		driver, err = neo4j.NewDriver("bolt://10.255.255.1:8080", server.AuthToken(), server.Config(), func(config *neo4j.Config) {
			config.Log = log
			config.SocketConnectTimeout = 1 * time.Second
		})
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())
		defer driver.Close()

		session = newSession(driver, neo4j.AccessModeRead)
		defer session.Close()

		_, err = session.BeginTransaction()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(test.BeConnectorErrorWithCode(6))
	})

})
