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
	"context"
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/router"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func assertNoRouter(t *testing.T, d Driver) {
	t.Helper()
	_, isDirectRouter := d.(*driver).router.(*directRouter)
	if !isDirectRouter {
		t.Error("Expected no router")
	}
}
func assertNoRouterAddress(t *testing.T, d Driver, address string) {
	t.Helper()
	direct := d.(*driver).router.(*directRouter)
	if direct.address != address {
		t.Errorf("Address mismatch %s vs %s", address, direct.address)
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

func assertSkipEncryption(t *testing.T, d Driver, skipEncryption bool) {
	t.Helper()
	c := d.(*driver).connector
	if c.SkipEncryption != skipEncryption {
		t.Errorf("SkipEncryption mismatch, %t vs %t", skipEncryption, c.SkipEncryption)
	}
}

func assertSkipVerify(t *testing.T, d Driver, skipVerify bool) {
	t.Helper()
	c := d.(*driver).connector
	if c.SkipVerify != skipVerify {
		t.Errorf("SkipVerify mismatch, %t vs %t", skipVerify, c.SkipVerify)
	}
}

func assertNetwork(t *testing.T, d Driver, network string) {
	t.Helper()
	c := d.(*driver).connector
	if c.Network != network {
		t.Errorf("Network mismatch, %s vs %s", network, c.Network)
	}
}

func TestDriverURISchemes(t *testing.T) {
	uriSchemeTests := []struct {
		scheme         string
		testing        string
		router         bool
		skipEncryption bool
		skipVerify     bool
		network        string
		address        string
	}{
		{"bolt", "bolt://localhost:7687", false, true, false, "tcp", "localhost:7687"},
		{"bolt+s", "bolt+s://localhost:7687", false, false, false, "tcp", "localhost:7687"},
		{"bolt+ssc", "bolt+ssc://localhost:7687", false, false, true, "tcp", "localhost:7687"},
		{"bolt+unix", "bolt+unix:///tmp/a.socket", false, true, false, "unix", "/tmp/a.socket"},
		{"neo4j", "neo4j://localhost:7687", true, true, false, "tcp", ""},
		{"neo4j+s", "neo4j+s://localhost:7687", true, false, false, "tcp", ""},
		{"neo4j+ssc", "neo4j+ssc://localhost:7687", true, false, true, "tcp", ""},
	}

	for _, tt := range uriSchemeTests {
		t.Run(tt.scheme, func(t *testing.T) {
			driver, err := NewDriver(tt.testing, NoAuth())

			AssertNoError(t, err)
			AssertStringEqual(t, driver.Target().Scheme, tt.scheme)
			if !tt.router {
				assertNoRouter(t, driver)
				assertNoRouterAddress(t, driver, tt.address)
			} else {
				assertRouter(t, driver)
			}
			assertSkipEncryption(t, driver, tt.skipEncryption)
			if !tt.skipEncryption {
				assertSkipVerify(t, driver, tt.skipVerify)
			}
			assertNetwork(t, driver, tt.network)
		})
	}
}

func TestDriverInvalidURISchemes(t *testing.T) {
	invalidURISchemeTests := []struct {
		name    string
		scheme  string
		testing string
	}{
		{"bolt+routing://", "bolt+routing", "bolt+routing://localhost:7687"},
		{"invalid://", "invalid", "invalid://localhost:7687"},
	}

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
	_, err = session.Run(context.TODO(), "cypher", nil)
	if !IsUsageError(err) {
		t.Errorf("should not allow new session after driver being closed")
	}

	err = driver.Close()
	if err != nil {
		t.Errorf("should allow the close call on a closed driver")
	}
}

func TestDriverSessionCreation(t *testing.T) {
	driverSessionCreationTests := []struct {
		name      string
		testing   string
		mode      AccessMode
		bookmarks []string
	}{
		{"Write", "bolt://localhost:7687", AccessModeWrite, []string(nil)},
		{"Read", "bolt://localhost:7687", AccessModeRead, []string(nil)},
		{"Write+bookmarks", "bolt://localhost:7687", AccessModeWrite, []string{"B1", "B2", "B3"}},
		{"Read+bookmarks", "bolt://localhost:7687", AccessModeRead, []string{"B1", "B2", "B3", "B4"}},
	}

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
