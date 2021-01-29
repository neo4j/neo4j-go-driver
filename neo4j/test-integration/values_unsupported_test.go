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
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"

	. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Unsupported Types [V1]", func() {
	const (
		WGS84SrID   int = 4326
		WGS843DSrID int = 4979
	)

	var server *control.SingleInstance
	var err error
	var driver neo4j.Driver
	var session neo4j.Session

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())

		driver, err = server.Driver()
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())

		if versionOfDriver(driver).GreaterThanOrEqual(V340) {
			Skip("this test is targeted for server version less than neo4j 3.4.0")
		}

		session, err = driver.Session(neo4j.AccessModeWrite)
		Expect(err).To(BeNil())
		Expect(session).NotTo(BeNil())
	})

	AfterEach(func() {
		if session != nil {
			session.Close()
		}

		if driver != nil {
			driver.Close()
		}
	})

	testSend := func(data interface{}) {
		_, err = session.Run("WITH $x RETURN 1", map[string]interface{}{"x": data})
		Expect(err).To(BeConnectorErrorWithCode(0x501))
		Expect(err).To(BeConnectorErrorWithDescription("unable to generate run message"))
	}

	Context("Send", func() {
		It("should fail sending Point (2D)", func() {
			testSend(neo4j.NewPoint2D(WGS84SrID, 1.0, 1.0))
		})

		It("should fail sending Point (3D)", func() {
			testSend(neo4j.NewPoint3D(WGS843DSrID, 1.0, 1.0, 1.0))
		})

		It("should fail sending Duration", func() {
			testSend(neo4j.DurationOf(1, 1, 1, 1))
		})

		It("should fail sending Date", func() {
			testSend(neo4j.DateOf(time.Now()))
		})

		It("should fail sending LocalDateTime", func() {
			testSend(neo4j.LocalDateTimeOf(time.Now()))
		})

		It("should fail sending LocalTime", func() {
			testSend(neo4j.LocalTimeOf(time.Now()))
		})

		It("should fail sending OffsetTime", func() {
			testSend(neo4j.OffsetTimeOf(time.Now()))
		})

		It("should fail sending DateTime with Offset", func() {
			testSend(time.Now().In(time.FixedZone("Offset", 2400)))
		})

		It("should fail sending DateTime with Zone", func() {
			testSend(time.Now().In(time.Local))
		})
	})
})
