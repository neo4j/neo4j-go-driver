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

package neo4j

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Authentication Tokens", func() {
	Context("NoAuth", func() {
		It("should only contain scheme=none", func() {
			token := NoAuth()

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(1))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeNone))
		})
	})

	Context("BasicAuth", func() {
		It("should include scheme, username and password only when realm is empty", func() {
			token := BasicAuth("test", "1234", "")

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(3))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeBasic))
			Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "test"))
			Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "1234"))
		})

		It("should include scheme, username, password and realm", func() {
			token := BasicAuth("test", "1234", "a_realm")

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(4))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeBasic))
			Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "test"))
			Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "1234"))
			Expect(tokenMap).To(HaveKeyWithValue(keyRealm, "a_realm"))
		})
	})

	Context("KerberosAuth", func() {
		It("should include provided ticket", func() {
			token := KerberosAuth("ticket_data")

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(2))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeKerberos))
			Expect(tokenMap).To(HaveKeyWithValue(keyTicket, "ticket_data"))
		})
	})

	Context("CustomAuth", func() {
		It("should include scheme, username, password", func() {
			token := CustomAuth("custom", "un", "pw", "", nil)

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(3))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, "custom"))
			Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "un"))
			Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "pw"))
		})

		It("should include scheme, username, password and realm", func() {
			token := CustomAuth("custom", "un", "pw", "realm", nil)

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(4))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, "custom"))
			Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "un"))
			Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "pw"))
			Expect(tokenMap).To(HaveKeyWithValue(keyRealm, "realm"))
		})

		It("should include scheme, username, password, realm and parameters", func() {
			token := CustomAuth("custom", "un", "pw", "realm", &map[string]interface{}{
				"user_id":     "1234",
				"user_emails": []string{"a@b.com", "b@c.com"},
			})

			tokenMap := token.tokens

			Expect(tokenMap).To(HaveLen(5))
			Expect(tokenMap).To(HaveKeyWithValue(keyScheme, "custom"))
			Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "un"))
			Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "pw"))
			Expect(tokenMap).To(HaveKeyWithValue(keyRealm, "realm"))
			Expect(tokenMap).To(HaveKeyWithValue("parameters", map[string]interface{}{
				"user_id":     "1234",
				"user_emails": []string{"a@b.com", "b@c.com"},
			}))
		})
	})
})
