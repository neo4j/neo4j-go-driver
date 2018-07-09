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
)

var _ = Describe("Driver", func() {

	Context("Construction", func() {
		It("Target should match provided URI", func() {
			uri := "bolt://localhost:7687"
			driver, err := NewDriver(uri, NoAuth())
			Expect(err).To(BeNil())

			driverTarget := driver.Target()

			Expect(driverTarget.Scheme).To(BeIdenticalTo("bolt"))
			Expect(driverTarget.Hostname()).To(BeIdenticalTo("localhost"))
			Expect(driverTarget.Port()).To(BeIdenticalTo("7687"))
		})
	})

	Context("Session", func() {
		type SessionTestCase struct {
			uri       string
			mode      AccessMode
			bookmarks []string
		}

		DescribeTable("With Parameters", func(testCase SessionTestCase) {
			driver, err := NewDriver(testCase.uri, NoAuth())
			Expect(err).To(BeNil())

			session, err := driver.Session(testCase.mode, testCase.bookmarks...)
			Expect(err).To(BeNil())

			Expect(session.accessMode).To(BeIdenticalTo(testCase.mode))
			Expect(session.bookmarks).To(HaveLen(len(testCase.bookmarks)))
			Expect(session.bookmarks).To(ConsistOf(testCase.bookmarks))
		}, Entry("direct/write/no_bookmark", SessionTestCase{
			uri:       "bolt://localhost:7687",
			mode:      AccessModeWrite,
			bookmarks: []string(nil),
		}), Entry("direct/read/no_bookmark", SessionTestCase{
			uri:       "bolt://localhost:7687",
			mode:      AccessModeRead,
			bookmarks: []string(nil),
		}), Entry("direct/write/one bookmark", SessionTestCase{
			uri:       "bolt://localhost:7687",
			mode:      AccessModeWrite,
			bookmarks: []string{"B1"},
		}), Entry("direct/read/one bookmark", SessionTestCase{
			uri:       "bolt://localhost:7687",
			mode:      AccessModeRead,
			bookmarks: []string{"B1"},
		}), Entry("direct/write/multiple bookmarks", SessionTestCase{
			uri:       "bolt://localhost:7687",
			mode:      AccessModeWrite,
			bookmarks: []string{"B1", "B2"},
		}), Entry("direct/read/multiple bookmarks", SessionTestCase{
			uri:       "bolt://localhost:7687",
			mode:      AccessModeRead,
			bookmarks: []string{"B3", "B4"},
		}))
	})
})
