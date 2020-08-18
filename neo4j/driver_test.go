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

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/router"
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

func TestDriverURISchemes(t *testing.T) {

	t.Run("bolt://", func(t1 *testing.T) {
		driver, err := NewDriver("bolt://localhost:7687", NoAuth())

		assertNoError(t1, err)
		assertStringEqual(t1, driver.Target().Scheme, "bolt")
		assertNoRouter(t1, driver)
	})

	t.Run("neo4j://", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost:7687", NoAuth())

		assertNoError(t1, err)
		assertStringEqual(t1, driver.Target().Scheme, "neo4j")
		assertRouter(t1, driver)
	})

	//old bolt+routing should be removed in 4.0 and only use neo4j scheme
	t.Run("bolt+routing://", func(t1 *testing.T) {
		driver, err := NewDriver("bolt+routing://localhost:7687", NoAuth())

		assertNoError(t1, err)
		assertStringEqual(t1, driver.Target().Scheme, "bolt+routing")
		assertRouter(t1, driver)
	})

	//TODO
	//bolt+ssc://
	//neo4j+ssc://
	//bolt+s://
	//neo4j+s://
}

func TestDriverInvalidURISchemes(t *testing.T) {

	t.Run("anotherscheme://", func(t1 *testing.T) {
		_, err := NewDriver("anotherscheme://localhost:7687", NoAuth())

		assertError(t1, err)
		assertStringContain(t1, err.Error(), "scheme")
	})

}

func TestDriverURIRoutingContext(t *testing.T) {

	t.Run("neo4j:// routing context query support", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost:7687?x=y&a=b", NoAuth())

		assertNoError(t1, err)
		assertRouterContext(t1, driver, map[string]string{"x": "y", "a": "b"})
	})

	t.Run("neo4j:// routing context with duplicate keys should error", func(t1 *testing.T) {
		_, err := NewDriver("neo4j://localhost:7687?x=y&x=b", NoAuth())

		assertError(t, err)
	})

}

func TestDriverDefaultPort(t *testing.T) {

	t.Run("neo4j://localhost should default to port 7687", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost?x=y&a=b", NoAuth())
		driverTarget := driver.Target()

		assertNoError(t1, err)
		assertStringEqual(t1, driverTarget.Port(), "7687")
	})

}

func TestNewDriverAndClose(t *testing.T) {

	driver, err := NewDriver("bolt://localhost:7687", NoAuth())
	assertNoError(t, err)

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
	assertNoError(t, err)

	session, err := driver.Session(AccessModeRead)
	assertError(t, err)

	if session != nil {
		t.Errorf("should not allow new session (AccessModeRead) after driver being closed")
	}

	session, err = driver.Session(AccessModeWrite)
	assertError(t, err)

	if session != nil {
		t.Errorf("should not allow new session (AccessModeWrite) after driver being closed")
	}

	err = driver.Close()
	if err != nil {
		t.Errorf("should allow the close call on a closed driver")
	}

}

func TestDriverSessionCreation(t *testing.T) {

	t.Run("case one", func(t1 *testing.T) {
		uri := "bolt://localhost:7687"
		mode := AccessModeWrite
		bookmarks := []string(nil)

		driver, err := NewDriver(uri, NoAuth())
		assertNoError(t1, err)

		sessi, err := driver.Session(mode, bookmarks...)
		assertNoError(t1, err)

		sess := sessi.(*session)

		if AccessMode(sess.defaultMode) != mode {
			t1.Errorf("the defaultMode was not correctly set %v", AccessMode(sess.defaultMode))
		}

		if len(sess.bookmarks) != len(bookmarks) {
			t1.Errorf("the bookmarks was not correctly set %v", sess.bookmarks)
		}
	})

	t.Run("case two", func(t1 *testing.T) {
		uri := "bolt://localhost:7687"
		mode := AccessModeRead
		bookmarks := []string(nil)

		driver, err := NewDriver(uri, NoAuth())
		assertNoError(t1, err)

		sessi, err := driver.Session(mode, bookmarks...)
		assertNoError(t1, err)

		sess := sessi.(*session)

		if AccessMode(sess.defaultMode) != mode {
			t1.Errorf("the defaultMode was not correctly set %v", AccessMode(sess.defaultMode))
		}

		if len(sess.bookmarks) != len(bookmarks) {
			t1.Errorf("the bookmarks was not correctly set %v", sess.bookmarks)
		}
	})

	t.Run("case three", func(t1 *testing.T) {
		uri := "bolt://localhost:7687"
		mode := AccessModeWrite
		bookmarks := []string{"B1", "B2", "B3"}

		driver, err := NewDriver(uri, NoAuth())
		assertNoError(t1, err)

		sessi, err := driver.Session(mode, bookmarks...)
		assertNoError(t1, err)

		sess := sessi.(*session)

		if AccessMode(sess.defaultMode) != mode {
			t1.Errorf("the defaultMode was not correctly set %v", AccessMode(sess.defaultMode))
		}

		if len(sess.bookmarks) != len(bookmarks) {
			t1.Errorf("the bookmarks was not correctly set %v", sess.bookmarks)
		}
	})

	t.Run("case four", func(t1 *testing.T) {
		uri := "bolt://localhost:7687"
		mode := AccessModeRead
		bookmarks := []string{"B1", "B2", "B3", "B4"}

		driver, err := NewDriver(uri, NoAuth())
		assertNoError(t1, err)

		sessi, err := driver.Session(mode, bookmarks...)
		assertNoError(t1, err)

		sess := sessi.(*session)

		if AccessMode(sess.defaultMode) != mode {
			t1.Errorf("the defaultMode was not correctly set %v", AccessMode(sess.defaultMode))
		}

		if len(sess.bookmarks) != len(bookmarks) {
			t1.Errorf("the bookmarks was not correctly set %v", sess.bookmarks)
		}
	})
}
