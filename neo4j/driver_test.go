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
	. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Driver", func() {

	Context("URI", func() {
		It("should support bolt:// scheme", func() {
			driver, err := NewDriver("bolt://localhost:7687", NoAuth())

			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())
			Expect(driver.Target().Scheme).To(BeIdenticalTo("bolt"))
		})

		It("should support bolt+routing:// scheme", func() {
			driver, err := NewDriver("bolt+routing://localhost:7687", NoAuth())

			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())
			Expect(driver.Target().Scheme).To(BeIdenticalTo("bolt+routing"))
		})

		It("should support neo4j:// scheme", func() {
			driver, err := NewDriver("neo4j://localhost:7687", NoAuth())

			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())
			Expect(driver.Target().Scheme).To(BeIdenticalTo("neo4j"))
		})

		It("should error anotherscheme:// scheme", func() {
			driver, err := NewDriver("anotherscheme://localhost:7687", NoAuth())

			Expect(driver).To(BeNil())
			Expect(err).To(BeGenericError(ContainSubstring("url scheme anotherscheme is not supported")))
		})
	})

	When("constructed with bolt://localhost:7687", func() {
		driver, err := NewDriver("bolt://localhost:7687", NoAuth())

		It("no errors should be returned", func() {
			Expect(err).To(BeNil())
		})

		When("closed", func() {
			err := driver.Close()
			It("no errors should be returned", func() {
				Expect(err).To(BeNil())
			})

			It("should not allow new read mode sessions", func() {
				session, err := driver.Session(AccessModeRead)
				Expect(err).NotTo(BeNil())
				Expect(session).To(BeNil())
			})

			It("should not allow new write mode sessions", func() {
				session, err := driver.Session(AccessModeWrite)
				Expect(err).NotTo(BeNil())
				Expect(session).To(BeNil())
			})

			It("should allow more close calls", func() {
				err := driver.Close()
				Expect(err).To(BeNil())
			})
		})

		driverTarget := driver.Target()

		Context("driver's", func() {
			It("target scheme should be bolt", func() {
				Expect(driverTarget.Scheme).To(BeIdenticalTo("bolt"))
			})

			It("target hostname should be localhost", func() {
				Expect(driverTarget.Hostname()).To(BeIdenticalTo("localhost"))
			})

			It("target port should be 7687", func() {
				Expect(driverTarget.Port()).To(BeIdenticalTo("7687"))
			})
		})
	})

	Describe("Session", func() {
		When("constructed", func() {
			type SessionTestCase struct {
				uri       string
				mode      AccessMode
				bookmarks []string
			}

			DescribeTable("should conform to given parameters", func(testCase SessionTestCase) {
				driver, err := NewDriver(testCase.uri, NoAuth())
				Expect(err).To(BeNil())

				session, err := driver.Session(testCase.mode, testCase.bookmarks...)
				Expect(err).To(BeNil())

				neoSession := session.(*neoSession)

				Expect(neoSession.accessMode).To(BeIdenticalTo(testCase.mode))

				Expect(neoSession.bookmarks).To(HaveLen(len(testCase.bookmarks)))
				Expect(neoSession.bookmarks).To(ConsistOf(testCase.bookmarks))
			}, Entry("(write, no_bookmark)", SessionTestCase{
				uri:       "bolt://localhost:7687",
				mode:      AccessModeWrite,
				bookmarks: []string(nil),
			}), Entry("(read, no_bookmark)", SessionTestCase{
				uri:       "bolt://localhost:7687",
				mode:      AccessModeRead,
				bookmarks: []string(nil),
			}), Entry("(write, one bookmark)", SessionTestCase{
				uri:       "bolt://localhost:7687",
				mode:      AccessModeWrite,
				bookmarks: []string{"B1"},
			}), Entry("(read, one bookmark)", SessionTestCase{
				uri:       "bolt://localhost:7687",
				mode:      AccessModeRead,
				bookmarks: []string{"B1"},
			}), Entry("(write, multiple bookmarks)", SessionTestCase{
				uri:       "bolt://localhost:7687",
				mode:      AccessModeWrite,
				bookmarks: []string{"B1", "B2"},
			}), Entry("(read, multiple bookmarks)", SessionTestCase{
				uri:       "bolt://localhost:7687",
				mode:      AccessModeRead,
				bookmarks: []string{"B3", "B4"},
			}))

		})
	})
})
