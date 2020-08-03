/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
	"reflect"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/router"
)

func assertNoRouter(t *testing.T, d Driver) {
	t.Helper()
	_, isDirectRouter := d.(*driver).router.(*directRouter)
	if !isDirectRouter {
		t.Error("Expected no router")
	}
}

func assertRouter(t *testing.T, d Driver) {
	t.Helper()
	_, isRouter := d.(*driver).router.(*router.Router)
	if !isRouter {
		t.Error("Expected router")
	}
}

func assertRouterContext(t *testing.T, d Driver, context map[string]string) {
	t.Helper()
	r := d.(*driver).router.(*router.Router)
	c := r.Context()
	if !reflect.DeepEqual(c, context) {
		t.Errorf("Router contexts differ: %#v vs %#v", c, context)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Expected no error but was %s", err)
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected error")
	}
}

func assertStringEqual(t *testing.T, s1, s2 string) {
	t.Helper()
	if s1 != s2 {
		t.Errorf("Expected %s to equal %s", s1, s2)
	}
}

func assertStringContain(t *testing.T, str, substr string) {
	t.Helper()
	if !strings.Contains(str, substr) {
		t.Errorf("Expected %s to contain %s", str, substr)
	}
}

func TestDriverN(t0 *testing.T) {
	t0.Run("URI", func(t1 *testing.T) {
		t1.Run("Should support bolt:// scheme", func(t *testing.T) {
			driver, err := NewDriver("bolt://localhost:7687", NoAuth())

			assertNoError(t, err)
			assertStringEqual(t, driver.Target().Scheme, "bolt")
			assertNoRouter(t, driver)
		})

		t1.Run("Should support bolt+routing:// sceme", func(t *testing.T) {
			driver, err := NewDriver("bolt+routing://localhost:7687", NoAuth())

			assertNoError(t, err)
			assertStringEqual(t, driver.Target().Scheme, "bolt+routing")
			assertRouter(t, driver)
		})

		t1.Run("should support neo4j:// scheme", func(t *testing.T) {
			driver, err := NewDriver("neo4j://localhost:7687", NoAuth())

			assertNoError(t, err)
			assertStringEqual(t, driver.Target().Scheme, "neo4j")
			assertRouter(t, driver)
		})

		t1.Run("should error anotherscheme:// scheme", func(t *testing.T) {
			_, err := NewDriver("anotherscheme://localhost:7687", NoAuth())

			assertError(t, err)
			assertStringContain(t, err.Error(), "scheme")
		})

		t1.Run("should support neo4j:// routing context", func(t *testing.T) {
			driver, err := NewDriver("neo4j://localhost:7687?x=y&a=b", NoAuth())

			assertNoError(t, err)
			assertRouterContext(t, driver, map[string]string{"x": "y", "a": "b"})
		})

		t1.Run("should error neo4j:// routing context with duplicate keys", func(t *testing.T) {
			_, err := NewDriver("neo4j://localhost:7687?x=y&x=b", NoAuth())

			assertError(t, err)
		})

		t1.Run("should default to port 7687 neo4j://localhost", func(t *testing.T) {
			driver, err := NewDriver("neo4j://localhost?x=y&a=b", NoAuth())
			driverTarget := driver.Target()

			assertNoError(t, err)
			assertStringEqual(t, driverTarget.Port(), "7687")
		})
	})
}

var _ = Describe("Driver", func() {
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

				sessi, err := driver.Session(testCase.mode, testCase.bookmarks...)
				Expect(err).To(BeNil())

				sess := sessi.(*session)

				Expect(AccessMode(sess.defaultMode)).To(BeIdenticalTo(testCase.mode))

				Expect(sess.bookmarks).To(HaveLen(len(testCase.bookmarks)))
				Expect(sess.bookmarks).To(ConsistOf(testCase.bookmarks))
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
