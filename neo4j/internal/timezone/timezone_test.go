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

import (
	"testing"
	"time"
)

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

// TestID pins when a time is sent by its zone id rather than its offset.
func TestID(t *testing.T) {
	london, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Fatal(err)
	}
	winter := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	summer := time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		time time.Time
		id   string
	}{
		{name: "a named zone", time: summer.In(london), id: "Europe/London"},
		{name: "a fixed zone whose name gives its offset", time: winter.In(time.FixedZone("CET", 3600)), id: "CET"},
		{name: "a fixed zone whose name gives another offset", time: winter.In(time.FixedZone("UTC", 3600))},
		{name: "a fixed zone whose name gives its offset in winter only", time: summer.In(time.FixedZone("CET", 3600))},
		{name: "a name that is not a zone", time: winter.In(time.FixedZone("Fake/Zone", 0))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id, ok := ID(test.time)
			if id != test.id || ok != (test.id != "") {
				t.Fatalf("ID is (%q, %v), want %q", id, ok, test.id)
			}
		})
	}
}
