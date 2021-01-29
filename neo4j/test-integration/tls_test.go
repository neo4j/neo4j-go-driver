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

package test_integration

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"
	"github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Trust", func() {
	var err error
	var server *control.SingleInstance

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())
	})

	verifySuccessfulConnection := func(uri string, strategy neo4j.TrustStrategy) {
		var driver neo4j.Driver
		var session neo4j.Session
		var result neo4j.Result
		var resultSummary neo4j.ResultSummary
		var err error

		driver, err = neo4j.NewDriver(uri, server.AuthToken(), server.Config(), func(config *neo4j.Config) {
			config.TrustStrategy = strategy
		})
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())
		defer driver.Close()

		session, err = driver.Session(neo4j.AccessModeRead)
		Expect(err).To(BeNil())
		Expect(session).NotTo(BeNil())
		defer session.Close()

		result, err = session.Run("RETURN 1", nil)
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())

		resultSummary, err = result.Consume()
		Expect(err).To(BeNil())
		Expect(resultSummary).NotTo(BeNil())
	}

	verifyFailedConnection := func(uri string, strategy neo4j.TrustStrategy, expectedCode uint32) {
		var driver neo4j.Driver
		var session neo4j.Session
		var err error

		driver, err = neo4j.NewDriver(uri, server.AuthToken(), server.Config(), func(config *neo4j.Config) {
			config.TrustStrategy = strategy
		})
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())
		defer driver.Close()

		session, err = driver.Session(neo4j.AccessModeRead)
		Expect(err).To(BeNil())
		Expect(session).NotTo(BeNil())
		defer session.Close()

		_, err = session.Run("RETURN 1", nil)
		Expect(err).To(test.BeConnectorErrorWithCode(expectedCode))
	}

	Context("TrustAny", func() {
		It("should connect when hostname verification is enabled", func() {
			verifySuccessfulConnection("bolt://localhost:7687", neo4j.TrustAny(true))
		})

		It("should not connect when hostname verification is enabled", func() {
			verifyFailedConnection("bolt://127.0.0.1:7687", neo4j.TrustAny(true), 13)
		})

		It("should connect when hostname verification is disabled", func() {
			verifySuccessfulConnection("bolt://localhost:7687", neo4j.TrustAny(false))
		})

		It("should connect when hostname verification is disabled", func() {
			verifySuccessfulConnection("bolt://127.0.0.1:7687", neo4j.TrustAny(false))
		})
	})

	Context("TrustOnly", func() {
		It("should not connect when certificate is not provided - bolt://127.0.0.1:7687", func() {
			verifyFailedConnection("bolt://127.0.0.1:7687", neo4j.TrustOnly(false), 13)
		})

		It("should not connect when certificate is not provided - bolt://localhost:7687", func() {
			verifyFailedConnection("bolt://localhost:7687", neo4j.TrustOnly(false), 13)
		})

		It("should connect when hostname verification is enabled", func() {
			verifySuccessfulConnection("bolt://localhost:7687", neo4j.TrustOnly(true, server.TLSCertificate()))
		})

		It("should not connect when hostname verification is enabled", func() {
			verifyFailedConnection("bolt://127.0.0.1:7687", neo4j.TrustOnly(true, server.TLSCertificate()), 13)
		})

		It("should connect when hostname verification is disabled", func() {
			verifySuccessfulConnection("bolt://localhost:7687", neo4j.TrustOnly(false, server.TLSCertificate()))
		})

		It("should connect when hostname verification is disabled", func() {
			verifySuccessfulConnection("bolt://127.0.0.1:7687", neo4j.TrustOnly(false, server.TLSCertificate()))
		})
	})
})
