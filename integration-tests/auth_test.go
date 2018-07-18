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

var _ = Describe("Authentication", func() {

	Specify("when wrong credentials are provided, it should fail with authentication error", func() {
		token := BasicAuth("wrong", "wrong", "")
		driver, err := NewDriver(singleInstanceUri, token)
		Expect(err).To(BeNil())
		defer driver.Close()

		session, err := driver.Session(AccessModeRead)
		Expect(err).To(BeNil())
		defer session.Close()

		_, err = session.Run("RETURN 1", nil)
		Expect(err).To(BeAuthenticationError())
	})

	When("when credentials are provided as a basic token with realm", func() {
		token := BasicAuth(username, password, "native")

		Specify("it should be able to connect", verifyConnect(&token))
	})

	When("when credentials are provided as a custom token", func() {
		token := CustomAuth("basic", username, password, "native", nil)

		Specify("it should be able to connect", verifyConnect(&token))
	})

	When("when credentials are provided as a custom token with parameters", func() {
		token := CustomAuth("basic", username, password, "native", &map[string]interface{}{
			"otp": "12345",
		})

		Specify("it should be able to connect", verifyConnect(&token))
	})
})

func verifyConnect(token *AuthToken) func() {
	return func() {
		driver, err := NewDriver(singleInstanceUri, *token)
		Expect(err).To(BeNil())
		defer driver.Close()

		session, err := driver.Session(AccessModeRead)
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
