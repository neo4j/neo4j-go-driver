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
	"errors"
	"sync"
	"time"
)

// known caches lookups, because time.LoadLocation parses a zone file on every call.
var known sync.Map

// time.LoadLocation resolves these two to whatever the reader's own host says.
var (
	errEmptyZone = errors.New(`unknown time zone ""`)
	errLocalZone = errors.New(`unknown time zone "Local"`)
)

type result struct {
	zone *time.Location
	err  error
}

// Load resolves a time zone id, refusing "" and "Local".
func Load(id string) (*time.Location, error) {
	switch id {
	case "":
		return nil, errEmptyZone
	case "Local":
		return nil, errLocalZone
	}
	if cached, seen := known.Load(id); seen {
		r := cached.(result)
		return r.zone, r.err
	}
	zone, err := time.LoadLocation(id)
	known.Store(id, result{zone, err})
	return zone, err
}

// ID returns t's zone id, or false if the zone fails to load or has another offset at that time.
func ID(t time.Time) (string, bool) {
	id := t.Location().String()
	zone, err := Load(id)
	if err != nil {
		return "", false
	}
	_, offset := t.Zone()
	if _, zoneOffset := t.In(zone).Zone(); zoneOffset != offset {
		return "", false
	}
	return id, true
}
