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
	"testing"
)

func TestDriverTarget(t *testing.T) {
	uri := "bolt://localhost:7687"
	driver, err := NewDriver(uri, NoAuth(), nil)
	assertNil(t, err)

	driverTarget := driver.Target()
	if driverTarget.Scheme != "bolt" {
		t.Errorf("driver.uri.scheme = %q", driver.Target().Scheme)
	}

	if driverTarget.Hostname() != "localhost" {
		t.Errorf("driver.uri.host = %q", driver.Target().Host)
	}

	if driverTarget.Port() != "7687" {
		t.Errorf("driver.uri.host = %q", driver.Target().Host)
	}
}

func TestDriver_Session(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
	}{
		{"direct", "bolt://localhost:7687"},
		//{"routing", "bolt+routing://localhost:7687"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			verifySessionParameters(t, func() *Session {
				driver, err := NewDriver(testCase.uri, NoAuth(), nil)
				if err != nil {
					t.Fatalf("unable to construct driver: %s", err)
				}

				session, err := driver.Session(AccessModeWrite)
				if err != nil {
					t.Fatalf("unable to obtain session: %s", err)
				}

				return session
			}, AccessModeWrite, []string(nil))
		})
	}
}

func TestDriver_SessionWithAccessMode(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		mode AccessMode
	}{
		{"direct/read", "bolt://localhost:7687", AccessModeRead},
		{"direct/write", "bolt://localhost:7687", AccessModeWrite},
		//{"routing/read", "bolt+routing://localhost:7687", AccessModeRead},
		//{"routing/write", "bolt+routing://localhost:7687", AccessModeWrite},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			verifySessionParameters(t, func() *Session {
				driver, err := NewDriver(testCase.uri, NoAuth(), nil)
				if err != nil {
					t.Fatalf("unable to construct driver: %s", err)
				}

				session, err := driver.Session(testCase.mode)
				if err != nil {
					t.Fatalf("unable to obtain session: %s", err)
				}

				return session
			}, testCase.mode, []string(nil))
		})
	}
}

func TestDirectDriver_SessionWithBookmark(t *testing.T) {
	testCases := []struct {
		name     string
		uri      string
		bookmark string
	}{
		{"direct/none", "bolt://localhost:7687", ""},
		{"direct/b1", "bolt://localhost:7687", "B1"},
		//{"routing/none", "bolt+routing://localhost:7687", ""},
		//{"routing/b1", "bolt+routing://localhost:7687", "B1"},
	}

	for _, testCase := range testCases {
		bookmarks := []string(nil)
		if len(testCase.bookmark) > 0 {
			bookmarks = append(bookmarks, testCase.bookmark)
		}

		t.Run(testCase.name, func(t *testing.T) {
			verifySessionParameters(t, func() *Session {
				driver, err := NewDriver(testCase.uri, NoAuth(), nil)
				if err != nil {
					t.Fatalf("unable to construct driver: %s", err)
				}

				session, err := driver.Session(AccessModeWrite, testCase.bookmark)
				if err != nil {
					t.Fatalf("unable to obtain session: %s", err)
				}

				return session
			}, AccessModeWrite, bookmarks)
		})
	}
}

func verifySessionParameters(t *testing.T, sessionFactory func() *Session, expectedMode AccessMode, bookmarks []string) {
	session := sessionFactory()

	if session.accessMode != expectedMode {
		t.Fatalf("expected session.accessMode [%d] to be %d", session.accessMode, expectedMode)
	}

	if len(session.bookmarks) != len(bookmarks) {
		t.Fatalf("expected session.bookmarks to be of length %d", len(bookmarks))
	}

	for i, val := range bookmarks {
		if val != session.bookmarks[i] {
			t.Fatalf("expected session.bookmark[%d] to be %s, where it is %s", i, val, session.bookmarks[i])
		}
	}
}
