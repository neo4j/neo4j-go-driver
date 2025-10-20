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
	"strings"
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

func divMod(a, b int64) (int64, int64) {
	return a / b, a % b
}

// sign returns -1 for negative numbers and 1 for positive numbers (including 0)
func sign(x int64) int64 {
	/*
	 * x >> 63: only keep sign bit (0...01 for negative, 0...0 for positive)
	 * -1:      "copy" sign bit to everywhere: 0...0 for negative, 1...1 for positive
	 * ^:       flip all bits: 1...1 for negative, 0...0 for positive
	 * |1:      set LSB: 1...1 (=-1) for negative, 0...1 (=1) for positive
	 */

	/*
	 * x >> 63: only keep sign bit (0...01 for negative, 0...0 for positive)
	 * -1:      "copy" sign bit to everywhere: 0...0 for negative, 1...1 for positive
	 * ^:       flip all bits: 1...1 for negative, 0...0 for positive
	 * |1:      set LSB: 1...1 (=-1) for negative, 0...1 (=1) for positive
	 */
	// This bit-juggling implementation tends to be ever so slightly faster than
	// the equivalent branching implementation:
	//
	// if x < 0 {
	// 	return -1
	// }
	// return 1
	//
	// In the context of string formatting this is likely negligible, yet a fun exercise ;)
	return int64(^(uint64(x)>>63 - 1) | 1)
}

// String returns the string representation of this Duration in ISO-8601 compliant form.
func (d Duration) String() string {
	hasTime := d.Seconds|int64(d.Nanos) != 0
	hasDate := d.Months|d.Days != 0
	if !hasTime && !hasDate {
		return "PT0S"
	}
	res := []byte("P")

	if hasDate {
		years, months := divMod(d.Months, 12)
		if years != 0 {
			res = fmt.Append(res, years, "Y")
		}
		if months != 0 {
			res = fmt.Append(res, months, "M")
		}
		if d.Days != 0 {
			res = fmt.Append(res, d.Days, "D")
		}
	}
	if hasTime {
		res = fmt.Append(res, "T")

		minutes1, seconds := divMod(d.Seconds, 60)
		secondsNanos, nanos := divMod(int64(d.Nanos), int64(time.Second))
		seconds = seconds + secondsNanos

		// Wrapping seconds again after nanos have been folded into seconds.
		// If we had int128 at our disposal simple divMod operations would suffice as nanos could be
		// folded into seconds as the first step without having to worry about overflows.
		minutes2, seconds := divMod(seconds, 60)
		minutes := minutes1 + minutes2
		if minutes != 0 && seconds != 0 {
			signMinutes := sign(minutes)
			signSeconds := sign(seconds)
			if signMinutes != signSeconds {
				minutes += signSeconds
				seconds += 60 * signMinutes
			}
		}
		hours, minutes := divMod(minutes, 60)

		if hours != 0 {
			res = fmt.Append(res, hours, "H")
		}
		if minutes != 0 {
			res = fmt.Append(res, minutes, "M")
		}
		if nanos < 0 {
			if seconds < 0 {
				nanosStr := strings.TrimRight(fmt.Sprintf(".%09d", -nanos), "0")
				res = fmt.Append(res, seconds, nanosStr, "S")
			} else if seconds > 0 {
				seconds--
				nanos = int64(time.Second) + nanos
				nanosStr := strings.TrimRight(fmt.Sprintf(".%09d", nanos), "0")
				res = fmt.Append(res, seconds, nanosStr, "S")
			} else {
				nanosStr := strings.TrimRight(fmt.Sprintf(".%09d", -nanos), "0")
				res = fmt.Append(res, "-0", nanosStr, "S")
			}
		} else if nanos > 0 {
			if seconds < -1 {
				seconds++
				nanos = int64(time.Second) - nanos
				nanosStr := strings.TrimRight(fmt.Sprintf(".%09d", nanos), "0")
				res = fmt.Append(res, seconds, nanosStr, "S")
			} else if seconds > -1 {
				// 0 100_000_000
				nanosStr := strings.TrimRight(fmt.Sprintf(".%09d", nanos), "0")
				res = fmt.Append(res, seconds, nanosStr, "S")
			} else {
				seconds++
				nanos = int64(time.Second) - nanos
				nanosStr := strings.TrimRight(fmt.Sprintf(".%09d", nanos), "0")
				res = fmt.Append(res, "-0", nanosStr, "S")
			}
		} else if seconds != 0 {
			res = fmt.Append(res, seconds, "S")
		}
	}

	return string(res)
}

func (d1 Duration) Equal(d2 Duration) bool {
	return d1.Months == d2.Months && d1.Days == d2.Days && d1.Seconds == d2.Seconds && d1.Nanos == d2.Nanos
}
