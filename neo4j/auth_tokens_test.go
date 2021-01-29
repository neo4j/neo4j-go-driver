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

package neo4j

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AuthTokens", func() {
	Describe("NoAuth", func() {
		When("invoked", func() {
			token := NoAuth()

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 1 item", func() {
					Expect(tokenMap).To(HaveLen(1))
				})

				It("should contain scheme=none", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeNone))
				})
			})
		})
	})

	Describe("BasicAuth", func() {
		When("invoked with username and password only", func() {
			token := BasicAuth("test", "1234", "")

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 3 items", func() {
					Expect(tokenMap).To(HaveLen(3))
				})

				It("should contain scheme=basic", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeBasic))
				})

				It("should contain principal=test", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "test"))
				})

				It("should contain credentials=1234", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "1234"))
				})
			})
		})

		When("invoked with username, password and realm", func() {
			token := BasicAuth("test", "1234", "a_realm")

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 4 items", func() {
					Expect(tokenMap).To(HaveLen(4))
				})

				It("should contain scheme=basic", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeBasic))
				})

				It("should contain principal=test", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "test"))
				})

				It("should contain credentials=1234", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "1234"))
				})

				It("should contain realm=a_realm", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyRealm, "a_realm"))
				})
			})
		})
	})

	Describe("KerberosAuth", func() {
		When("invoked with ticket data", func() {
			token := KerberosAuth("ticket_data")

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 2 items", func() {
					Expect(tokenMap).To(HaveLen(2))
				})

				It("should contain scheme=kerberos", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, schemeKerberos))
				})

				It("should contain ticket=ticket_data", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyTicket, "ticket_data"))
				})
			})
		})
	})

	Describe("CustomAuth", func() {
		When("invoked with scheme, username and password", func() {
			token := CustomAuth("custom", "un", "pw", "", nil)

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 3 items", func() {
					Expect(tokenMap).To(HaveLen(3))
				})

				It("should contain scheme=custom", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, "custom"))
				})

				It("should contain principal=un", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "un"))
				})

				It("should contain credentials=pw", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "pw"))
				})
			})
		})

		When("invoked with scheme, username, password and realm", func() {
			token := CustomAuth("custom", "un", "pw", "realm", nil)

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 4 items", func() {
					Expect(tokenMap).To(HaveLen(4))
				})

				It("should contain scheme=custom", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, "custom"))
				})

				It("should contain principal=un", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "un"))
				})

				It("should contain credentials=pw", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "pw"))
				})

				It("should contain realm=realm", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyRealm, "realm"))
				})
			})
		})

		When("invoked with scheme, username, password, realm and parameters", func() {
			params := map[string]interface{}{
				"user_id":     "1234",
				"user_emails": []string{"a@b.com", "b@c.com"},
			}

			token := CustomAuth("custom", "un", "pw", "realm", params)

			tokenMap := token.tokens

			Context("the token", func() {
				It("should have 5 items", func() {
					Expect(tokenMap).To(HaveLen(5))
				})

				It("should contain scheme=custom", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyScheme, "custom"))
				})

				It("should contain principal=un", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyPrincipal, "un"))
				})

				It("should contain credentials=pw", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyCredentials, "pw"))
				})

				It("should contain realm=realm", func() {
					Expect(tokenMap).To(HaveKeyWithValue(keyRealm, "realm"))
				})

				It("should contain given parameters", func() {
					Expect(tokenMap).To(HaveKeyWithValue("parameters", params))
				})
			})
		})
	})
})
