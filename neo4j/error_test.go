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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

import (
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Error", func() {

	Context("IsSecurityError", func() {
		When("provided with a ConnectorError with BOLT_TLS_ERROR code", func() {
			const (
				BOLT_TLS_ERROR = 13
				BOLT_DEFUNCT   = 4
			)

			err := newConnectorError(BOLT_DEFUNCT, BOLT_TLS_ERROR, "BOLT_TLS_ERROR", "some context", "some description")

			It("should return true", func() {
				Expect(IsSecurityError(err)).To(BeTrue())
			})
		})

		When("provided with a DatabaseError with Neo.ClientError.Security.Unauthorized code", func() {
			err := newDatabaseError("ClientError", "Neo.ClientError.Security.Unauthorized", "unauthorized")

			It("should return true", func() {
				Expect(IsSecurityError(err)).To(BeTrue())
			})
		})

		When("provided with a ConnectorError with random code", func() {
			const (
				BOLT_CONNECTION_RESET = 4
				BOLT_DEFUNCT          = 4
			)
			err := newConnectorError(BOLT_DEFUNCT, BOLT_CONNECTION_RESET, "BOLT_CONNECTION_RESET", "some context", "some description")

			It("should return false", func() {
				Expect(IsSecurityError(err)).To(BeFalse())
			})
		})

		When("provided with a DatabaseError with random code", func() {
			err := newDatabaseError("TransientError", "Neo.TransientError.Transaction.Terminated", "terminated")

			It("should return false", func() {
				Expect(IsSecurityError(err)).To(BeFalse())
			})
		})

		When("provided with generic error type", func() {
			err := newDriverError("some error")

			It("should return false", func() {
				Expect(IsSecurityError(err)).To(BeFalse())
			})
		})

		When("provided with another error type", func() {
			err := errors.New("some error")

			It("should return false", func() {
				Expect(IsSecurityError(err)).To(BeFalse())
			})
		})
	})

	Context("IsAuthenticationError", func() {
		When("provided with a ConnectorError with BOLT_PERMISSION_DENIED code", func() {
			const (
				BOLT_PERMISSION_DENIED = 7
				BOLT_DEFUNCT           = 4
			)

			err := newConnectorError(BOLT_DEFUNCT, BOLT_PERMISSION_DENIED, "BOLT_PERMISSION_DENIED", "some context", "some description")

			It("should return true", func() {
				Expect(IsAuthenticationError(err)).To(BeTrue())
			})
		})

		When("provided with a DatabaseError with Neo.ClientError.Security.Unauthorized code", func() {
			err := newDatabaseError("ClientError", "Neo.ClientError.Security.Unauthorized", "unauthorized")

			It("should return true", func() {
				Expect(IsAuthenticationError(err)).To(BeTrue())
			})
		})

		When("provided with a ConnectorError with random code", func() {
			const (
				BOLT_CONNECTION_RESET = 4
				BOLT_DEFUNCT          = 4
			)
			err := newConnectorError(BOLT_DEFUNCT, BOLT_CONNECTION_RESET, "BOLT_CONNECTION_RESET", "some context", "some description")

			It("should return false", func() {
				Expect(IsAuthenticationError(err)).To(BeFalse())
			})
		})

		When("provided with a DatabaseError with random code", func() {
			err := newDatabaseError("TransientError", "Neo.TransientError.Transaction.Terminated", "terminated")

			It("should return false", func() {
				Expect(IsAuthenticationError(err)).To(BeFalse())
			})
		})

		When("provided with generic error type", func() {
			err := newDriverError("some error")

			It("should return false", func() {
				Expect(IsAuthenticationError(err)).To(BeFalse())
			})
		})

		When("provided with another error type", func() {
			err := errors.New("some error")

			It("should return false", func() {
				Expect(IsAuthenticationError(err)).To(BeFalse())
			})
		})
	})

	Context("IsClientError", func() {
		When("provided with a DatabaseError with ClientError classification", func() {
			err := newDatabaseError("ClientError", "Neo.ClientError.Statement.Invalid", "invalid statement")

			It("should return true", func() {
				Expect(IsClientError(err)).To(BeTrue())
			})
		})

		When("provided with a generic error", func() {
			err := newDriverError("some error")

			It("should return true", func() {
				Expect(IsClientError(err)).To(BeTrue())
			})
		})

		When("provided with a DatabaseError with Neo.ClientError.Security.Unauthorized code", func() {
			err := newDatabaseError("ClientError", "Neo.ClientError.Security.Unauthorized", "unauthorized")

			It("should return false", func() {
				Expect(IsClientError(err)).To(BeFalse())
			})
		})

		When("provided with a ConnectorError", func() {
			err := newConnectorError(0, 13, "some text", "some context", "some description")

			It("should return false", func() {
				Expect(IsClientError(err)).To(BeFalse())
			})
		})

		When("provided with another error type", func() {
			err := errors.New("some error")

			It("should return false", func() {
				Expect(IsClientError(err)).To(BeFalse())
			})
		})

	})

	Context("IsTransientError", func() {
		When("provided with a DatabaseError with TransientError classification", func() {
			err := newDatabaseError("TransientError", "Neo.TransientError.Transaction.Timeout", "timeout")

			It("should return true", func() {
				Expect(IsTransientError(err)).To(BeTrue())
			})
		})

		When("provided with a DatabaseError with Neo.TransientError.Transaction.Terminated code", func() {
			err := newDatabaseError("TransientError", "Neo.TransientError.Transaction.Terminated", "terminated")

			It("should return false", func() {
				Expect(IsTransientError(err)).To(BeFalse())
			})
		})

		When("provided with a DatabaseError with Neo.TransientError.Transaction.LockClientStopped code", func() {
			err := newDatabaseError("TransientError", "Neo.TransientError.Transaction.LockClientStopped", "terminated")

			It("should return false", func() {
				Expect(IsTransientError(err)).To(BeFalse())
			})
		})

		When("provided with a generic error", func() {
			err := newDriverError("some error")

			It("should return false", func() {
				Expect(IsTransientError(err)).To(BeFalse())
			})
		})

		When("provided with a ConnectorError", func() {
			err := newConnectorError(0, 13, "some text", "some context", "some description")

			It("should return false", func() {
				Expect(IsTransientError(err)).To(BeFalse())
			})
		})

		When("provided with another error type", func() {
			err := errors.New("some error")

			It("should return false", func() {
				Expect(IsTransientError(err)).To(BeFalse())
			})
		})
	})

	Context("IsSessionExpired", func() {
		When("provided with a sessionExpiredError", func() {
			err := &sessionExpiredError{message: "error"}

			It("should return true", func() {
				Expect(IsSessionExpired(err)).To(BeTrue())
			})
		})

		When("provided with a ConnectorError with BOLT_ROUTING_NO_SERVERS_TO_SELECT code", func() {
			const (
				BOLT_ROUTING_NO_SERVERS_TO_SELECT = 0x801
				BOLT_DEFUNCT                      = 4
			)

			err := newConnectorError(BOLT_DEFUNCT, BOLT_ROUTING_NO_SERVERS_TO_SELECT, "BOLT_ROUTING_NO_SERVERS_TO_SELECT", "some context", "some description")

			It("should return true", func() {
				Expect(IsSessionExpired(err)).To(BeTrue())
			})
		})

		When("provided with a ConnectorError with random code", func() {
			const (
				BOLT_TLS_ERROR = 13
				BOLT_DEFUNCT   = 4
			)

			err := newConnectorError(BOLT_DEFUNCT, BOLT_TLS_ERROR, "BOLT_TLS_ERROR", "some context", "some description")

			It("should return true", func() {
				Expect(IsSessionExpired(err)).To(BeFalse())
			})
		})

		When("provided with a database error", func() {
			err := newDatabaseError("ClientError", "Neo.TransientError.Transaction.LockClientStopped", "some error")

			It("should return false", func() {
				Expect(IsSessionExpired(err)).To(BeFalse())
			})
		})

		When("provided with a generic error", func() {
			err := newDriverError("some error")

			It("should return false", func() {
				Expect(IsSessionExpired(err)).To(BeFalse())
			})
		})

		When("provided with a ConnectorError", func() {
			err := newConnectorError(0, 13, "some text", "some context", "some description")

			It("should return false", func() {
				Expect(IsSessionExpired(err)).To(BeFalse())
			})
		})

		When("provided with another error type", func() {
			err := errors.New("some error")

			It("should return false", func() {
				Expect(IsSessionExpired(err)).To(BeFalse())
			})
		})
	})

	Context("IsServiceUnavailable", func() {
		Context("should return true", func() {
			DescribeTable("when provided with a connector error code",
				func(code int) {
					err := newConnectorError(4, code, "some text", "some context", "some description")

					Expect(IsServiceUnavailable(err)).To(BeTrue())
				},
				Entry("BOLT_INTERRUPTED", 3),
				Entry("BOLT_CONNECTION_RESET", 4),
				Entry("BOLT_NO_VALID_ADDRESS", 5),
				Entry("BOLT_TIMED_OUT", 6),
				Entry("BOLT_CONNECTION_REFUSED", 11),
				Entry("BOLT_NETWORK_UNREACHABLE", 12),
				Entry("BOLT_TLS_ERROR", 13),
				Entry("BOLT_END_OF_TRANSMISSION", 15),
				Entry("BOLT_POOL_FULL", 0x600),
				Entry("BOLT_ADDRESS_NOT_RESOLVED", 0x700),
				Entry("BOLT_ROUTING_UNABLE_TO_RETRIEVE_ROUTING_TABLE", 0x800),
				Entry("BOLT_ROUTING_UNABLE_TO_REFRESH_ROUTING_TABLE", 0x803),
				Entry("BOLT_ROUTING_NO_SERVERS_TO_SELECT", 0x801),
			)
		})

		Context("should return false", func() {
			DescribeTable("when provided with an unrelevant connector error code",
				func(code int) {
					err := newConnectorError(4, code, "some text", "some context", "some description")

					Expect(IsServiceUnavailable(err)).To(BeFalse())
				},
				Entry("BOLT_SERVER_FAILURE", 16),
				Entry("BOLT_UNKNOWN_ERROR", 1),
				Entry("BOLT_TRANSPORT_UNSUPPORTED", 0x400),
				Entry("BOLT_PROTOCOL_VIOLATION", 0x500),
			)
		})

		When("provided with a database error", func() {
			err := newDatabaseError("ClientError", "Neo.TransientError.Transaction.LockClientStopped", "some error")

			It("should return false", func() {
				Expect(IsServiceUnavailable(err)).To(BeFalse())
			})
		})

		When("provided with a generic error", func() {
			err := newDriverError("some error")

			It("should return false", func() {
				Expect(IsServiceUnavailable(err)).To(BeFalse())
			})
		})

		When("provided with another error type", func() {
			err := errors.New("some error")

			It("should return false", func() {
				Expect(IsServiceUnavailable(err)).To(BeFalse())
			})
		})
	})
})
