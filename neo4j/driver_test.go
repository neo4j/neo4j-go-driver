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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/router"
	. "github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
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

var uriSchemeTests = []struct {
	name    string
	scheme  string
	testing string
	router  bool
}{
	{"bolt://", "bolt", "bolt://localhost:7687", false},
	{"bolt+s://", "bolt", "bolt://localhost:7687", false},
	{"bolt+ssc://", "bolt", "bolt://localhost:7687", false},
	{"neo4j://", "neo4j", "neo4j://localhost:7687", true},
	{"neo4j+s://", "neo4j", "neo4j://localhost:7687", true},
	{"neo4j+ssc://", "neo4j", "neo4j://localhost:7687", true},
}

func TestDriverURISchemesX(t *testing.T) {
	for _, tt := range uriSchemeTests {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := NewDriver(tt.testing, NoAuth())

			AssertNoError(t, err)
			AssertStringEqual(t, driver.Target().Scheme, tt.scheme)
			if !tt.router {
				assertNoRouter(t, driver)
			} else {
				assertRouter(t, driver)
			}
		})
	}
}

var invalidURISchemeTests = []struct {
	name    string
	scheme  string
	testing string
}{
	{"bolt+routing://", "bolt+routing", "bolt+routing://localhost:7687"},
	{"invalid://", "invalid", "invalid://localhost:7687"},
}

func TestDriverInvalidURISchemesX(t *testing.T) {
	for _, tt := range invalidURISchemeTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDriver(tt.testing, NoAuth())

			AssertError(t, err)
			assertUsageError(t, err)
			AssertStringContain(t, err.Error(), "scheme")
		})
	}
}

func TestDriverURIRoutingContext(t *testing.T) {
	t.Run("Extracts keys", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost:7687?x=y&a=b", NoAuth())

		AssertNoError(t1, err)
		assertRouterContext(t1, driver, map[string]string{"x": "y", "a": "b", "address": "localhost:7687"})
	})

	t.Run("Duplicate keys should error", func(t1 *testing.T) {
		_, err := NewDriver("neo4j://localhost:7687?x=y&x=b", NoAuth())

		AssertError(t, err)
		assertUsageError(t, err)
	})

	t.Run("Reserved key 'address' should error", func(t *testing.T) {
		_, err := NewDriver("neo4j://localhost:7687?x=y&address=b", NoAuth())

		AssertError(t, err)
		assertUsageError(t, err)
	})
}

func TestDriverDefaultPort(t *testing.T) {

	t.Run("neo4j://localhost should default to port 7687", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost", NoAuth())
		driverTarget := driver.Target()

		AssertNoError(t1, err)
		AssertStringEqual(t1, driverTarget.Port(), "7687")
		assertRouterContext(t1, driver, map[string]string{"address": "localhost:7687"})
	})
}

func TestNewDriverAndClose(t *testing.T) {

	driver, err := NewDriver("bolt://localhost:7687", NoAuth())
	AssertNoError(t, err)

	driverTarget := driver.Target()

	if driverTarget.Scheme != "bolt" {
		t.Errorf("the URI scheme was not properly set %v", driverTarget.Scheme)
	}

	if driverTarget.Hostname() != "localhost" {
		t.Errorf("the hostname is not the expected %v", driverTarget.Hostname())
	}

	if driverTarget.Port() != "7687" {
		t.Errorf("the port is not the expected %v", driverTarget.Port())
	}

	err = driver.Close()
	AssertNoError(t, err)

	session := driver.NewSession(SessionConfig{})
	_, err = session.Run("cypher", nil)
	if !IsUsageError(err) {
		t.Errorf("should not allow new session after driver being closed")
	}

	err = driver.Close()
	if err != nil {
		t.Errorf("should allow the close call on a closed driver")
	}
}

var driverSessionCreationTests = []struct {
	name      string
	testing   string
	mode      AccessMode
	bookmarks []string
}{
	{"case one", "bolt://localhost:7687", AccessModeWrite, []string(nil)},
	{"case two", "bolt://localhost:7687", AccessModeRead, []string(nil)},
	{"case three", "bolt://localhost:7687", AccessModeWrite, []string{"B1", "B2", "B3"}},
	{"case four", "bolt://localhost:7687", AccessModeRead, []string{"B1", "B2", "B3", "B4"}},
}

func TestDriverSessionCreationX(t *testing.T) {
	for _, tt := range driverSessionCreationTests {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := NewDriver(tt.testing, NoAuth())
			AssertNoError(t, err)

			sessi := driver.NewSession(SessionConfig{AccessMode: tt.mode, Bookmarks: tt.bookmarks})
			sess := sessi.(*session)

			if AccessMode(sess.defaultMode) != tt.mode {
				t.Errorf("the defaultMode was not correctly set %v", AccessMode(sess.defaultMode))
			}

			if len(sess.bookmarks) != len(tt.bookmarks) {
				t.Errorf("the bookmarks was not correctly set %v", sess.bookmarks)
			}
		})
	}
}
