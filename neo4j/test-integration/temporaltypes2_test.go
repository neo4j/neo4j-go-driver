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

package test_integration

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/dbserver"
)

func TestTemporalTypes(tt *testing.T) {
	server := dbserver.GetDbServer()
	driver := server.Driver()
	if server.Version.LessThan(V340) {
		tt.Skip("Temporal types are only available after neo4j 3.4.0 release")
	}

	sendAndReceive := func(t *testing.T, p interface{}) interface{} {
		return single(t, driver, "CREATE (n:Node {x: $p}) RETURN n.x", map[string]interface{}{"p": p})
	}

	// Checks that the adjusted hh:mm:ss nano are the same regardless of time zone.
	assertTimePart := func(t *testing.T, t1, t2 time.Time) {
		if t1.Hour() != t2.Hour() || t1.Minute() != t2.Minute() || t1.Second() != t2.Second() || t1.Nanosecond() != t2.Nanosecond() {
			t.Errorf("Time parts differs: %v vs %v", t1, t2)
		}
	}

	// Checks that the adjusted year, month and day are the same regardless of time zone.
	assertDatePart := func(t *testing.T, t1, t2 time.Time) {
		if t1.Year() != t2.Year() || t1.Month() != t2.Month() || t1.Day() != t2.Day() {
			t.Errorf("Date parts differs: %v vs %v", t1, t2)
		}
	}

	// Checks that the time instant is the same, ignores time zone
	assertEqual := func(t *testing.T, t1, t2 time.Time) {
		if !t1.Equal(t2) {
			t.Errorf("Times not equal: %v vs %v", t1, t2)
		}
	}

	assertDurationEqual := func(t *testing.T, d1, d2 neo4j.Duration) {
		if !d1.Equal(d2) {
			t.Errorf("Durations not equal: %v vs %v", d1, d2)
		}
	}

	// Checks that the Go time uses the Go Local location.
	assertLocal := func(t *testing.T, t1 time.Time) {
		if t1.Location() != time.Local {
			t.Errorf("time.Local is not used: %v", t1.Location())
		}
	}

	// Checks that zone/location offset matches the expectation
	assertOffset := func(t *testing.T, t1 time.Time, expected int) {
		_, offset := t1.Zone()
		if offset != expected {
			t.Errorf("Wrong offset, expected %d but was %d (%v)", expected, offset, t1)
		}
	}

	assertZone := func(t *testing.T, t1 time.Time, expected string) {
		zone, _ := t1.Zone()
		if zone != expected {
			t.Errorf("Wrong zone name, expected %s but was %s (%v)", expected, zone, t1)
		}
	}

	tt.Run("Receive", func(rt *testing.T) {
		rt.Run("neo4j.Duration", func(t *testing.T) {
			d1 := single(t, driver, "RETURN duration({ months: 16, days: 45, seconds: 120, nanoseconds: 187309812 })", nil).(neo4j.Duration)
			d2 := neo4j.Duration{Months: 16, Days: 45, Seconds: 120, Nanos: 187309812}
			assertDurationEqual(t, d1, d2)
			// Verify that utility function used to build duration works
			assertDurationEqual(t, d1, neo4j.DurationOf(16, 45, 120, 187309812))
		})

		rt.Run("neo4j.Date", func(t *testing.T) {
			t1 := time.Time(single(t, driver, "RETURN date({ year: 1994, month: 11, day: 15 })", nil).(neo4j.Date))
			t2 := time.Date(1994, 11, 15, 0, 0, 0, 0, time.Local)
			assertDatePart(t, t1, t2)
			// Verify that utility function used to build LocalTime out of Go time works
			t3 := time.Time(neo4j.DateOf(t2))
			assertEqual(t, t1, t3)
		})

		rt.Run("neo4j.LocalTime", func(t *testing.T) {
			t1 := time.Time(single(t, driver, "RETURN localtime({ hour: 23, minute: 49, second: 59, nanosecond: 999999999 })", nil).(neo4j.LocalTime))
			t2 := time.Date(0, 0, 0, 23, 49, 59, 999999999, time.Local)
			assertTimePart(t, t1, t2)
			// The received time should be represented with the local time zone for convenience.
			assertLocal(t, t1)
			// Verify that utility function used to build LocalTime out of Go time works
			t3 := time.Time(neo4j.LocalTimeOf(t2))
			assertEqual(t, t1, t3)
			assertLocal(t, t3)
		})

		rt.Run("neo4j.Time+offset", func(t *testing.T) {
			t1 := time.Time(single(t, driver, "RETURN time({ hour: 23, minute: 49, second: 59, nanosecond: 999999999, timezone:'+03:00' })", nil).(neo4j.Time))
			t2 := time.Date(0, 0, 0, 23, 49, 59, 999999999, time.FixedZone("Offset", 3*60*60))
			assertTimePart(t, t1, t2)
			assertOffset(t, t1, 3*60*60)
			// Verify that utility function used to build Time out of Go time works
			t3 := time.Time(neo4j.OffsetTimeOf(t2))
			assertEqual(t, t1, t3)
		})

		rt.Run("neo4j.LocalDateTime", func(t *testing.T) {
			t1 := time.Time(single(t, driver,
				"RETURN localdatetime({ year: 1859, month: 5, day: 31, hour: 23, minute: 49, second: 59, nanosecond: 999999999 })", nil).(neo4j.LocalDateTime))
			t2 := time.Date(1859, 5, 31, 23, 49, 59, 999999999, time.Local)
			assertTimePart(t, t1, t2)
			assertDatePart(t, t1, t2)
			// The received time should be represented with the local time zone for convenience.
			assertLocal(t, t1)
			// Verify that utility function used to build LocalDateTime out of Go time works
			t3 := time.Time(neo4j.LocalDateTimeOf(t2))
			assertEqual(t, t1, t3)
			// Verify that utility function can build a local time of a Go time of another time zone than local
			t4 := time.Time(neo4j.LocalDateTimeOf(time.Date(1859, 5, 31, 23, 49, 59, 999999999, time.UTC)))
			assertTimePart(t, t1, t4)
			assertDatePart(t, t1, t4)
		})

		rt.Run("time.Time+offset", func(t *testing.T) {
			t1 := single(t, driver,
				"RETURN datetime({ year: 1859, month: 5, day: 31, hour: 23, minute: 49, second: 59, nanosecond: 999999999, timezone:'+02:30' })", nil).(time.Time)
			offset := 150 * 60
			t2 := time.Date(1859, 5, 31, 23, 49, 59, 999999999, time.FixedZone("Offset", offset))
			assertTimePart(t, t1, t2)
			assertDatePart(t, t1, t2)
			assertOffset(t, t1, offset)
		})

		rt.Run("time.Time+zone", func(t *testing.T) {
			t1 := single(t, driver,
				"RETURN datetime({ year: 1959, month: 5, day: 31, hour: 23, minute: 49, second: 59, nanosecond: 999999999, timezone:'Europe/London'})", nil).(time.Time)
			offset := 60 * 60
			location, err := time.LoadLocation("Europe/London")
			if err != nil {
				t.Fatal(err)
			}
			t2 := time.Date(1959, 5, 31, 23, 49, 59, 999999999, location)
			assertTimePart(t, t1, t2)
			assertDatePart(t, t1, t2)
			assertOffset(t, t1, offset)
			assertZone(t, t1, "BST")
		})
	})

	tt.Run("Random", func(rt *testing.T) {
		const numRand = 100

		randomDate := func() neo4j.Date {
			sign := 1
			if rand.Intn(2) == 0 {
				sign = -sign
			}

			return neo4j.Date(time.Date(
				sign*rand.Intn(9999),
				time.Month(rand.Intn(12)+1),
				rand.Intn(28)+1,
				0, 0, 0, 0, time.Local))
		}

		randomDuration := func() neo4j.Duration {
			sign := int64(1)
			if rand.Intn(2) == 0 {
				sign = -sign
			}

			return neo4j.DurationOf(
				sign*rand.Int63n(math.MaxInt32),
				sign*rand.Int63n(math.MaxInt32),
				sign*rand.Int63n(math.MaxInt32),
				rand.Intn(1000000000))
		}

		randomLocalTime := func() neo4j.LocalTime {
			return neo4j.LocalTimeOf(
				time.Date(
					0, 0, 0,
					rand.Intn(24),
					rand.Intn(60),
					rand.Intn(60),
					rand.Intn(1000000000),
					time.Local))
		}

		randomLocalDateTime := func() neo4j.LocalDateTime {
			sign := 1
			if rand.Intn(2) == 0 {
				sign = -sign
			}

			return neo4j.LocalDateTimeOf(
				time.Date(
					sign*rand.Intn(9999),
					time.Month(rand.Intn(12)+1),
					rand.Intn(28)+1,
					rand.Intn(24),
					rand.Intn(60),
					rand.Intn(60),
					rand.Intn(1000000000),
					time.Local))
		}

		randomOffsetTime := func() neo4j.OffsetTime {
			sign := 1
			if rand.Intn(2) == 0 {
				sign = -sign
			}

			return neo4j.OffsetTimeOf(
				time.Date(
					0, 0, 0,
					rand.Intn(24),
					rand.Intn(60),
					rand.Intn(60),
					rand.Intn(1000000000),
					time.FixedZone("Offset", sign*rand.Intn(64800))))
		}
		randomOffsetDateTime := func() time.Time {
			sign := 1
			if rand.Intn(2) == 0 {
				sign = -sign
			}

			return time.Date(
				rand.Intn(300)+1900,
				time.Month(rand.Intn(12)+1),
				rand.Intn(28)+1,
				rand.Intn(24),
				rand.Intn(60),
				rand.Intn(60),
				rand.Intn(1000000000),
				time.FixedZone("Offset", sign*rand.Intn(64800)))
		}

		randomZonedDateTime := func() time.Time {
			var zones = []string{
				"Africa/Harare", "America/Aruba", "Africa/Nairobi", "America/Dawson", "Asia/Beirut", "Asia/Tashkent",
				"Canada/Eastern", "Europe/Malta", "Europe/Volgograd", "Indian/Kerguelen", "Etc/GMT+3",
			}

			location, err := time.LoadLocation(zones[rand.Intn(len(zones))])
			if err != nil {
				panic(err)
			}

			return time.Date(
				rand.Intn(300)+1900,
				time.Month(rand.Intn(12)+1),
				rand.Intn(28)+1,
				rand.Intn(17)+6, // to be safe from DST changes
				rand.Intn(60),
				rand.Intn(60),
				rand.Intn(1000000000),
				location)
		}

		rt.Run("Date", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomDate()
				d2 := sendAndReceive(t, d1).(neo4j.Date)
				assertDatePart(t, time.Time(d1), time.Time(d2))
			}
		})

		rt.Run("Duration", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomDuration()
				d2 := sendAndReceive(t, d1).(neo4j.Duration)
				assertDurationEqual(t, d1, d2)
			}
		})

		rt.Run("LocalTime", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomLocalTime()
				d2 := sendAndReceive(t, d1).(neo4j.LocalTime)
				assertTimePart(t, time.Time(d1), time.Time(d2))
				assertLocal(t, time.Time(d2))
			}
		})

		rt.Run("OffsetTime", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomOffsetTime()
				d2 := sendAndReceive(t, d1).(neo4j.OffsetTime)
				assertTimePart(t, time.Time(d1), time.Time(d2))
				assertZone(t, time.Time(d2), "Offset")
			}
		})

		rt.Run("LocalDateTime", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomLocalDateTime()
				d2 := sendAndReceive(t, d1).(neo4j.LocalDateTime)
				assertDatePart(t, time.Time(d1), time.Time(d2))
				assertTimePart(t, time.Time(d1), time.Time(d2))
				assertLocal(t, time.Time(d2))
			}
		})

		rt.Run("Offset DateTime", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomOffsetDateTime()
				d2 := sendAndReceive(t, d1).(time.Time)
				assertDatePart(t, d1, d2)
				assertTimePart(t, d1, d2)
				assertZone(t, d2, "Offset")
			}
		})

		rt.Run("Zoned DateTime", func(t *testing.T) {
			for i := 0; i < numRand; i++ {
				d1 := randomZonedDateTime()
				d2 := sendAndReceive(t, d1).(time.Time)
				assertDatePart(t, d1, d2)
				assertTimePart(t, d1, d2)
				zone, _ := d1.Zone()
				assertZone(t, d2, zone)
			}
		})
	})
}
