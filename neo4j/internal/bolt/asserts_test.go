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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/dbtype"
)

func assertOnlyRecord(t *testing.T, rec *connection.Record, sum *connection.Summary, err error) {
	t.Helper()
	if rec == nil {
		t.Errorf("Expected record")
	}
	if sum != nil {
		t.Errorf("Didn't expect summary")
	}
	if err != nil {
		t.Errorf("Didn't expect error: %s", err)
	}
}

func assertOnlySummary(t *testing.T, rec *connection.Record, sum *connection.Summary, err error) {
	t.Helper()
	if rec != nil {
		t.Errorf("Didn't expect record")
	}
	if sum == nil {
		t.Errorf("Expected summary")
	}
	if err != nil {
		t.Errorf("Didn't expect error")
	}
}

func assertOnlyError(t *testing.T, rec *connection.Record, sum *connection.Summary, err error) {
	t.Helper()
	if rec != nil {
		t.Errorf("Didn't expect record")
	}
	if sum != nil {
		t.Errorf("Didn't expect summary")
	}
	if err == nil {
		t.Errorf("Expected error")
	}
}

func assertKeys(t *testing.T, ekeys []interface{}, s *connection.Stream) {
	t.Helper()
	if s == nil {
		t.Fatal("No stream")
	}
	for i, k := range s.Keys {
		if k != ekeys[i] {
			t.Errorf("Stream keys differ")
		}
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Should be err but wasn't")
	}
}

func assertDatabaseError(t *testing.T, err error) {
	t.Helper()
	assertError(t, err)
	_, isDbErr := err.(*connection.DatabaseError)
	if !isDbErr {
		t.Errorf("Expected database error but was %T: %s", err, err)
	}
}

func assertDateTimeSame(t *testing.T, t1, t2 time.Time) {
	t.Helper()
	if !t1.Equal(t2) {
		t.Errorf("Times differ: %s vs %s (%d vs %d", t1, t2, t1.UnixNano(), t2.UnixNano())
	}
}

func assertDateSame(t *testing.T, t1, t2 time.Time) {
	t.Helper()
	if t1.Year() != t2.Year() {
		t.Errorf("Wrong year")
	}
	if t1.Month() != t2.Month() {
		t.Errorf("Wrong Month")
	}
	if t1.Day() != t2.Day() {
		t.Errorf("Wrong Day")
	}
}

func assertTimeSame(t *testing.T, t1, t2 time.Time) {
	t.Helper()
	if t1.Hour() != t2.Hour() {
		t.Errorf("Wrong Hour, %d vs %d", t1.Hour(), t2.Hour())
	}
	if t1.Minute() != t2.Minute() {
		t.Errorf("Wrong Minute")
	}
	if t1.Second() != t2.Second() {
		t.Errorf("Wrong Second")
	}
	if t1.Nanosecond() != t2.Nanosecond() {
		t.Errorf("Wrong.Nanosecond")
	}
}

func assertTimeLocationSame(t *testing.T, t1, t2 time.Time) {
	t.Helper()
	if t1.Location().String() != t2.Location().String() {
		t.Errorf("Locations differ, %s vs %s", t1.Location(), t2.Location())
	}
}

func assertZoneOffsetSame(t *testing.T, t1, t2 time.Time) {
	t.Helper()
	_, off1 := t1.Zone()
	_, off2 := t2.Zone()
	if off1 != off2 {
		t.Errorf("Offsets differ, %d vs %d", off1, off2)
	}
}

func assertDurationSame(t *testing.T, d1, d2 dbtype.Duration) {
	t.Helper()
	if d1.Months != d2.Months {
		t.Errorf("Months differ, %d vs %d", d1.Months, d2.Months)
	}
	if d1.Days != d2.Days {
		t.Errorf("Days differ, %d vs %d", d1.Days, d2.Days)
	}
	if d1.Seconds != d2.Seconds {
		t.Errorf("Seconds differ, %d vs %d", d1.Seconds, d2.Seconds)
	}
	if d1.Nanos != d2.Nanos {
		t.Errorf("Nanos differ, %d vs %d", d1.Nanos, d2.Nanos)
	}
}
