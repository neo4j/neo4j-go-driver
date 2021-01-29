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
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"

	. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Authentication", func() {
	var server *control.SingleInstance
	var err error

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())
	})

	Specify("when wrong credentials are provided, it should fail with authentication error", func() {
		token := neo4j.BasicAuth("wrong", "wrong", "")
		driver, err := neo4j.NewDriver(server.BoltURI(), token, server.Config())
		Expect(err).To(BeNil())
		defer driver.Close()

		session, err := driver.Session(neo4j.AccessModeRead)
		Expect(err).To(BeNil())
		defer session.Close()

		_, err = session.Run("RETURN 1", nil)
		Expect(err).To(BeAuthenticationError())
	})

	verifyConnect := func(token neo4j.AuthToken) func() {
		return func() {
			driver, err := neo4j.NewDriver(server.BoltURI(), token)
			Expect(err).To(BeNil())
			defer driver.Close()

			session, err := driver.Session(neo4j.AccessModeRead)
			Expect(err).To(BeNil())
			defer session.Close()

			result, err := session.Run("RETURN 1", nil)
			Expect(err).To(BeNil())

			if result.Next() {
				Expect(result.Record().GetByIndex(0)).Should(BeEquivalentTo(1))
			}
			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())
		}
	}

	When("when credentials are provided as a basic token with realm", func() {
		token := neo4j.BasicAuth(server.Username(), server.Password(), "native")

		Specify("it should be able to connect", verifyConnect(token))
	})

	When("when credentials are provided as a custom token", func() {
		token := neo4j.CustomAuth("basic", server.Username(), server.Password(), "native", nil)

		Specify("it should be able to connect", verifyConnect(token))
	})

	When("when credentials are provided as a custom token with parameters", func() {
		token := neo4j.CustomAuth("basic", server.Username(), server.Password(), "native", map[string]interface{}{
			"otp": "12345",
		})

		Specify("it should be able to connect", verifyConnect(token))
	})
})
