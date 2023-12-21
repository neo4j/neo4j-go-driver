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
	"fmt"
	"time"
)

// Cypher DateTime corresponds to Go time.Time

type (
	Time          time.Time // Time since start of day with timezone information
	Date          time.Time // Date value, without a time zone and time related components.
	LocalTime     time.Time // Time since start of day in local timezone
	LocalDateTime time.Time // Date and time in local timezone
)

// Time casts LocalDateTime to time.Time
//
// Note that the resulting time.Time will have its location set to time.Local.
// From the DBMS's perspective, however, a LocalDateTime is considered to not have any timezone information.
func (t LocalDateTime) Time() time.Time {
	return time.Time(t)
}

// String returns the string representation of this LocalDateTime in ISO-8601 compliant form:
// `YYYY-MM-DDThh:mm:ss.nnnnnnnnn`.
func (t LocalDateTime) String() string {
	return t.Time().Format("2006-01-02T15:04:05.999999999")
}

// Time casts LocalTime to time.Time
//
// Note that the resulting time.Time will have its location set to time.Local.
// From the DBMS's perspective, however, a LocalTime is considered to not have any timezone information.
func (t LocalTime) Time() time.Time {
	return time.Time(t)
}

// String returns the string representation of this LocalTime in ISO-8601 compliant form:
// `hh:mm:ss.nnnnnnnnn`.
func (t LocalTime) String() string {
	return t.Time().Format("15:04:05.999999999")
}

// Time casts Date to time.Time
func (t Date) Time() time.Time {
	return time.Time(t)
}

// String returns the string representation of this Date in ISO-8601 compliant form:
// `YYYY-MM-DD`.
func (t Date) String() string {
	return t.Time().Format("2006-01-02")
}

// Time casts Time to time.Time
func (t Time) Time() time.Time {
	return time.Time(t)
}

// String returns the string representation of this Time in ISO-8601 compliant form:
// `hh:mm:ss.nnnnnnnnn±Z/hh:mm`.
func (t Time) String() string {
	return t.Time().Format("15:04:05.999999999Z07:00")
}

// Duration represents temporal amount containing months, days, seconds and nanoseconds.
// Supports longer durations than time.Duration
type Duration struct {
	Months  int64
	Days    int64
	Seconds int64
	Nanos   int
}

// String returns the string representation of this Duration in ISO-8601 compliant form.
func (d Duration) String() string {
	sign := ""
	if d.Seconds < 0 && d.Nanos > 0 {
		d.Seconds++
		d.Nanos = int(time.Second) - d.Nanos

		if d.Seconds == 0 {
			sign = "-"
		}
	}

	timePart := ""
	if d.Nanos == 0 {
		timePart = fmt.Sprintf("%s%d", sign, d.Seconds)
	} else {
		timePart = fmt.Sprintf("%s%d.%09d", sign, d.Seconds, d.Nanos)
	}

	return fmt.Sprintf("P%dM%dDT%sS", d.Months, d.Days, timePart)
}

func (d1 Duration) Equal(d2 Duration) bool {
	return d1.Months == d2.Months && d1.Days == d2.Days && d1.Seconds == d2.Seconds && d1.Nanos == d2.Nanos
}
