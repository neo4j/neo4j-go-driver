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

package timezone

import "testing"

// TestLoad pins which ids resolve and which are refused.
func TestLoad(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		resolves bool
	}{
		{name: "a named zone", id: "Europe/London", resolves: true},
		{name: "utc", id: "UTC", resolves: true},
		{name: "an empty id", id: "", resolves: false},
		{name: "an id of Local", id: "Local", resolves: false},
		{name: "an id that is not a zone", id: "Fake/Zone", resolves: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// the first call fills the cache, the second reads it back
			for range 2 {
				zone, err := Load(test.id)
				if test.resolves && (err != nil || zone.String() != test.id) {
					t.Fatalf("Load is (%v, %v), want the zone", zone, err)
				}
				if !test.resolves && err == nil {
					t.Fatalf("Load is (%v, nil), want an error", zone)
				}
			}
		})
	}
}
