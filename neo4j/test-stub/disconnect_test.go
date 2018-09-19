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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test_stub

import (
	"github.com/neo4j/neo4j-go-driver/neo4j/test-stub/control"
	"github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"path"
)

var _ = Describe("Disconnect", func() {
	var stub *control.StubServer

	AfterEach(func() {
		if stub != nil {
			Expect(stub.Finished()).To(BeTrue())

			stub.Close()
		}
	})

	Context("V1", func() {
		It("should return error when server disconnects after INIT", func() {
			stub = control.NewStubServer(9001, path.Join("v1", "disconnect_after_init.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			summary, err := result.Consume()

			Expect(err).To(test.BeServiceUnavailableError())
			Expect(summary).ToNot(BeNil())
		})

		It("should return error when server disconnects after RUN", func() {
			stub = control.NewStubServer(9001, path.Join("v1", "disconnect_on_run.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			summary, err := result.Consume()

			Expect(err).To(test.BeServiceUnavailableError())
			Expect(summary).ToNot(BeNil())
		})

		It("should return error when server disconnects after PULL_ALL", func() {
			stub = control.NewStubServer(9001, path.Join("v1", "disconnect_on_pull_all.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			summary, err := result.Consume()

			Expect(err).To(test.BeServiceUnavailableError())
			Expect(summary).ToNot(BeNil())
		})
	})

	Context("V3", func() {
		It("should return error when server disconnects after HELLO", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "disconnect_after_hello.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			summary, err := result.Consume()

			Expect(err).To(test.BeServiceUnavailableError())
			Expect(summary).ToNot(BeNil())
		})

		It("should return error when server disconnects after RUN", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "disconnect_on_run.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			summary, err := result.Consume()

			Expect(err).To(test.BeServiceUnavailableError())
			Expect(summary).ToNot(BeNil())
		})

		It("should return error when server disconnects after PULL_ALL", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "disconnect_on_pull_all.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			summary, err := result.Consume()

			Expect(err).To(test.BeServiceUnavailableError())
			Expect(summary).ToNot(BeNil())
		})
	})

})
