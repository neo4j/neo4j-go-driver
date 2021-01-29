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
	"math"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	Context("defaultConfig", func() {
		config := defaultConfig()

		It("should have encryption turned on", func() {
			Expect(config.Encrypted).To(BeTrue())
		})

		It("should have trust strategy equal to TrustAny(false)", func() {
			Expect(config.TrustStrategy).To(Equal(TrustAny(false)))
		})

		It("should have max transaction retry duration as 30s", func() {
			Expect(config.MaxTransactionRetryTime).To(BeIdenticalTo(30 * time.Second))
		})

		It("should have max connection pool size to be 100", func() {
			Expect(config.MaxConnectionPoolSize).To(BeEquivalentTo(100))
		})

		It("should have max connection lifetime to be 1h", func() {
			Expect(config.MaxConnectionLifetime).To(BeIdenticalTo(1 * time.Hour))
		})

		It("should have connection acquisition timeout to be 1m", func() {
			Expect(config.ConnectionAcquisitionTimeout).To(BeIdenticalTo(1 * time.Minute))
		})

		It("should have socket connect timeout to be 5s", func() {
			Expect(config.SocketConnectTimeout).To(BeIdenticalTo(5 * time.Second))
		})

		It("should have socket keep alive enabled", func() {
			Expect(config.SocketKeepalive).To(BeTrue())
		})

		It("should have non-nil logger", func() {
			Expect(config.Log).NotTo(BeNil())
		})

		It("should have an internalLogger logger with level set to 0", func() {
			logger, ok := config.Log.(*internalLogger)
			Expect(ok).To(BeTrue())

			Expect(logger.level).To(BeZero())
		})
	})

	Context("validateAndNormaliseConfig", func() {
		It("should return error when MaxTransactionRetryTime is less than 0", func() {
			config := defaultConfig()
			config.MaxTransactionRetryTime = -1 * time.Second

			err := validateAndNormaliseConfig(config)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("maximum transaction retry time cannot be smaller than 0"))
		})

		It("should return error when MaxConnectionPoolSize is 0", func() {
			config := defaultConfig()
			config.MaxConnectionPoolSize = 0

			err := validateAndNormaliseConfig(config)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("maximum connection pool size cannot be 0"))
		})

		It("should normalize MaxConnectionPoolSize to MaxInt32 when negative", func() {
			config := defaultConfig()
			config.MaxConnectionPoolSize = -1

			err := validateAndNormaliseConfig(config)
			Expect(err).To(BeNil())

			Expect(config.MaxConnectionPoolSize).To(Equal(math.MaxInt32))
		})

		It("should normalize ConnectionAcquisitionTimeout to -1ns when negative", func() {
			config := defaultConfig()
			config.ConnectionAcquisitionTimeout = -1 * time.Second

			err := validateAndNormaliseConfig(config)
			Expect(err).To(BeNil())

			Expect(config.ConnectionAcquisitionTimeout).To(Equal(-1 * time.Nanosecond))
		})

		It("should normalize SocketConnectTimeout to 0 when negative", func() {
			config := defaultConfig()
			config.SocketConnectTimeout = -1 * time.Second

			err := validateAndNormaliseConfig(config)
			Expect(err).To(BeNil())

			Expect(config.SocketConnectTimeout).To(Equal(0 * time.Nanosecond))
		})
	})

})
