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
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	. "github.com/neo4j/neo4j-go-driver/internal/testing"
)

var _ = Describe("Direct Driver", func() {
	Context("with Stub Server", func() {
		type TestCase struct {
			script   string
			testFunc func()
		}

		DescribeTable("", func(testCase TestCase) {
			stub := NewStubServer(9001, testCase.script)
			defer stub.Close()

			testCase.testFunc()

			Expect(stub.Finished()).To(BeTrue())
		}, Entry("should return error when server disconnects after RUN", TestCase{
			script:   "disconnect_on_run.script",
			testFunc: consumeShouldFailOnServerDisconnects,
		}), Entry("should return error when server disconnects after PULL_ALL", TestCase{
			script:   "disconnect_on_pull_all.script",
			testFunc: consumeShouldFailOnServerDisconnects,
		}), Entry("should execute simple query", TestCase{
			script:   "return_1.script",
			testFunc: shouldExecuteReturn1,
		}))
	})
})

func consumeShouldFailOnServerDisconnects() {
	driver := createDirectDriver()
	defer driver.Close()

	session := createSession(driver)
	defer session.Close()

	result, err := session.Run("RETURN $x", &map[string]interface{}{"x": 1})
	Expect(err).To(BeNil())

	summary, err := result.Consume()

	Expect(err).To(BeServiceUnavailableError())
	Expect(summary).ToNot(BeNil())
}

func shouldExecuteReturn1() {
	driver := createDirectDriver()
	defer driver.Close()

	session := createSession(driver)
	defer session.Close()

	result, err := session.Run("RETURN $x", &map[string]interface{}{"x": 1})
	Expect(err).To(BeNil())

	var count int64
	for result.Next() {
		if x, ok := result.Record().Get("x"); ok {
			count += x.(int64)
		}
	}

	Expect(result.Err()).To(BeNil())
	Expect(count).To(BeIdenticalTo(int64(1)))
}

func createDirectDriver() Driver {
	driver, err := NewDriver("bolt://localhost:9001", NoAuth(), func(config *Config) {
		config.Encrypted = false
		config.Log = ConsoleLogger(DEBUG)
	})
	Expect(err).To(BeNil())

	return driver
}

func createSession(driver Driver) *Session {
	session, err := driver.Session(AccessModeWrite)
	Expect(err).To(BeNil())

	return session
}
