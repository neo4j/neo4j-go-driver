/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbtype

import (
	"encoding/json"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

var (
	sampleUUID      = UUID{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	sampleCanonical = "550e8400-e29b-41d4-a716-446655440000"
	nilUUID         = UUID{}
	nilCanonical    = "00000000-0000-0000-0000-000000000000"
	maxUUID         = UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	maxCanonical    = "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

func TestUUIDString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		uuid     UUID
		expected string
	}{
		{"sample", sampleUUID, sampleCanonical},
		{"nil", nilUUID, nilCanonical},
		{"max", maxUUID, maxCanonical},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testutil.AssertDeepEquals(t, tc.uuid.String(), tc.expected)
		})
	}
}

func TestParseUUID(t *testing.T) {
	t.Parallel()

	t.Run("valid inputs round-trip", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name     string
			input    string
			expected UUID
		}{
			{"sample", sampleCanonical, sampleUUID},
			{"nil", nilCanonical, nilUUID},
			{"max", maxCanonical, maxUUID},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				parsed, err := ParseUUID(tc.input)
				testutil.AssertNoError(t, err)
				testutil.AssertDeepEquals(t, parsed, tc.expected)
			})
		}
	})

	t.Run("invalid inputs return error", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name  string
			input string
		}{
			{"empty", ""},
			{"too short", "550e8400-e29b-41d4-a716-44665544000"},
			{"too long", "550e8400-e29b-41d4-a716-4466554400000"},
			{"wrong separator", "550e8400xe29b-41d4-a716-446655440000"},
			{"non-hex", "zz0e8400-e29b-41d4-a716-446655440000"},
			{"missing hyphens", "550e8400e29b41d4a716446655440000abcd"},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				_, err := ParseUUID(tc.input)
				testutil.AssertError(t, err)
			})
		}
	})
}

func TestUUIDTextMarshaling(t *testing.T) {
	t.Parallel()

	t.Run("MarshalText returns canonical form", func(t *testing.T) {
		t.Parallel()
		got, err := sampleUUID.MarshalText()
		testutil.AssertNoError(t, err)
		testutil.AssertDeepEquals(t, string(got), sampleCanonical)
	})

	t.Run("UnmarshalText round-trips canonical form", func(t *testing.T) {
		t.Parallel()
		var u UUID
		testutil.AssertNoError(t, u.UnmarshalText([]byte(sampleCanonical)))
		testutil.AssertDeepEquals(t, u, sampleUUID)
	})

	t.Run("JSON encoding uses canonical form", func(t *testing.T) {
		t.Parallel()
		got, err := json.Marshal(sampleUUID)
		testutil.AssertNoError(t, err)
		testutil.AssertDeepEquals(t, string(got), `"`+sampleCanonical+`"`)

		var u UUID
		testutil.AssertNoError(t, json.Unmarshal(got, &u))
		testutil.AssertDeepEquals(t, u, sampleUUID)
	})
}
