/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
	"path"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-stub/control"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query", func() {
	var stub *control.StubServer

	AfterEach(func() {
		if stub != nil {
			Expect(stub.Finished()).To(BeTrue())

			stub.Close()
		}
	})

	Context("V1", func() {
		It("should execute simple query", func() {
			stub = control.NewStubServer(9001, path.Join("v1", "return_1.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			var count int64
			for result.Next() {
				if x, ok := result.Record().Get("x"); ok {
					count += x.(int64)
				}
			}

			Expect(result.Err()).To(BeNil())
			Expect(count).To(BeIdenticalTo(int64(1)))
		})
	})

	Context("V3", func() {
		It("should execute simple query", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "return_1.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			var count int64
			for result.Next() {
				if x, ok := result.Record().Get("x"); ok {
					count += x.(int64)
				}
			}

			Expect(result.Err()).To(BeNil())
			Expect(count).To(BeIdenticalTo(int64(1)))
		})

		It("should run with bookmarks", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "run_with_bookmarks.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver, "foo", "bar")
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
			Expect(err).To(BeNil())

			var count int64
			for result.Next() {
				if x, ok := result.Record().Get("x"); ok {
					count += x.(int64)
				}
			}

			Expect(result.Err()).To(BeNil())
			Expect(count).To(BeIdenticalTo(int64(1)))
		})

		It("should run with metadata", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "run_with_metadata.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1}, neo4j.WithTxMetadata(map[string]interface{}{"mode": "r"}))
			Expect(err).To(BeNil())

			var count int64
			for result.Next() {
				if x, ok := result.Record().Get("x"); ok {
					count += x.(int64)
				}
			}

			Expect(result.Err()).To(BeNil())
			Expect(count).To(BeIdenticalTo(int64(1)))
		})

		It("should run with timeout", func() {
			stub = control.NewStubServer(9001, path.Join("v3", "run_with_timeout.script"))

			driver := newDriver("bolt://localhost:9001")
			defer driver.Close()

			session := createSession(driver)
			defer session.Close()

			result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1}, neo4j.WithTxTimeout(12340*time.Millisecond))
			Expect(err).To(BeNil())

			var count int64
			for result.Next() {
				if x, ok := result.Record().Get("x"); ok {
					count += x.(int64)
				}
			}

			Expect(result.Err()).To(BeNil())
			Expect(count).To(BeIdenticalTo(int64(1)))
		})

	})

})
