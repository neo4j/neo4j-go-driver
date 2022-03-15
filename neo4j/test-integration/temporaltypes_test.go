/*
 * Copyright (c) "Neo4j"
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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestTemporalTypes(outer *testing.T) {
	const (
		numberOfRandomValues = 200
	)

	server := dbserver.GetDbServer()
	var driver neo4j.Driver
	var session neo4j.Session
	var result neo4j.Result
	var err error

	rand.Seed(time.Now().UnixNano())

	driver = server.Driver()

	if server.Version.LessThan(V340) {
		outer.Skip("temporal types are only available after neo4j 3.4.0 release")
	}

	session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	assertNotNil(outer, session)

	defer func() {
		if session != nil {
			session.Close()
		}

		if driver != nil {
			driver.Close()
		}
	}()

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

	randomLocalDate := func() neo4j.Date {
		sign := 1
		if rand.Intn(2) == 0 {
			sign = -sign
		}

		return neo4j.DateOf(
			time.Date(
				sign*rand.Intn(9999),
				time.Month(rand.Intn(12)+1),
				rand.Intn(28)+1,
				0, 0, 0, 0, time.Local))
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

	randomZonedDateTime := func(t *testing.T) time.Time {
		var zones = []string{
			"Africa/Harare", "America/Aruba", "Africa/Nairobi", "America/Dawson", "Asia/Beirut", "Asia/Tashkent",
			"Canada/Eastern", "Europe/Malta", "Europe/Volgograd", "Indian/Kerguelen", "Etc/GMT+3",
		}

		location, err := time.LoadLocation(zones[rand.Intn(len(zones))])
		assertNil(t, err)

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

	testSendAndReceive := func(t *testing.T, query string, data interface{}, expected []interface{}) {
		result, err = session.Run(query, map[string]interface{}{"x": data})
		assertNil(t, err)

		if result.Next() {
			var received = result.Record().Values

			assertEquals(t, received, expected)
		}
		assertNil(t, result.Err())
		assertFalse(t, result.Next())
	}

	testSendAndReceiveValue := func(t *testing.T, value interface{}) {
		result, err = session.Run("CREATE (n:Node {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		assertNil(t, err)

		if result.Next() {
			var received = result.Record().Values[0]

			assertEquals(t, received, value)
		}
		assertNil(t, result.Err())
		assertFalse(t, result.Next())
	}

	testSendAndReceiveValueComp := func(t *testing.T, value interface{}, comp func(x, y interface{}) bool) {
		result, err = session.Run("CREATE (n:Node {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		assertNil(t, err)

		if result.Next() {
			var received = result.Record().Values[0]

			assertEquals(t, comp(value, received), true)
		}
		assertNil(t, result.Err())
		assertFalse(t, result.Next())
	}

	outer.Run("Send and Receive", func(inner *testing.T) {
		inner.Run("duration", func(t *testing.T) {
			data := neo4j.DurationOf(14, 35, 75, 789012587)

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.months, x.days, x.seconds, x.millisecondsOfSecond, x.microsecondsOfSecond, x.nanosecondsOfSecond",
				data,
				[]interface{}{
					data,
					int64(14),
					int64(35),
					int64(75),
					int64(789),
					int64(789012),
					int64(789012587),
				})
		})

		inner.Run("date", func(t *testing.T) {
			data := neo4j.DateOf(time.Date(1976, 6, 13, 0, 0, 0, 0, time.Local))

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.year, x.month, x.day",
				data,
				[]interface{}{
					data,
					int64(1976),
					int64(6),
					int64(13),
				})
		})

		inner.Run("local time", func(t *testing.T) {
			data := neo4j.LocalTimeOf(time.Date(0, 0, 0, 12, 34, 56, 789012587, time.Local))

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.hour, x.minute, x.second, x.millisecond, x.microsecond, x.nanosecond",
				data,
				[]interface{}{
					data,
					int64(12),
					int64(34),
					int64(56),
					int64(789),
					int64(789012),
					int64(789012587),
				})
		})

		inner.Run("offset time", func(t *testing.T) {
			data := neo4j.OffsetTimeOf(time.Date(0, 0, 0, 12, 34, 56, 789012587, time.FixedZone("Offset", 90*60)))

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.hour, x.minute, x.second, x.millisecond, x.microsecond, x.nanosecond, x.offset",
				data,
				[]interface{}{
					data,
					int64(12),
					int64(34),
					int64(56),
					int64(789),
					int64(789012),
					int64(789012587),
					"+01:30",
				})
		})

		inner.Run("local date time", func(t *testing.T) {
			data := neo4j.LocalDateTimeOf(time.Date(1976, 6, 13, 12, 34, 56, 789012587, time.Local))

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.year, x.month, x.day, x.hour, x.minute, x.second, x.millisecond, x.microsecond, x.nanosecond",
				data,
				[]interface{}{
					data,
					int64(1976),
					int64(6),
					int64(13),
					int64(12),
					int64(34),
					int64(56),
					int64(789),
					int64(789012),
					int64(789012587),
				})
		})

		inner.Run("offset date time", func(t *testing.T) {
			data := time.Date(1976, 6, 13, 12, 34, 56, 789012587, time.FixedZone("Offset", -90*60))

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.year, x.month, x.day, x.hour, x.minute, x.second, x.millisecond, x.microsecond, x.nanosecond, x.offset",
				data,
				[]interface{}{
					data,
					int64(1976),
					int64(6),
					int64(13),
					int64(12),
					int64(34),
					int64(56),
					int64(789),
					int64(789012),
					int64(789012587),
					"-01:30",
				})
		})

		inner.Run("zoned date time", func(t *testing.T) {
			location, err := time.LoadLocation("US/Pacific")
			assertNil(t, err)
			data := time.Date(1959, 5, 31, 23, 49, 59, 999999999, location)

			testSendAndReceive(t, "WITH $x AS x RETURN x, x.year, x.month, x.day, x.hour, x.minute, x.second, x.millisecond, x.microsecond, x.nanosecond, x.timezone",
				data,
				[]interface{}{
					data,
					int64(1959),
					int64(5),
					int64(31),
					int64(23),
					int64(49),
					int64(59),
					int64(999),
					int64(999999),
					int64(999999999),
					"US/Pacific",
				})
		})
	})

	outer.Run("Send and receive random arrays", func(inner *testing.T) {
		inner.Run("duration", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomDuration()
			}

			testSendAndReceiveValue(t, list)
		})

		inner.Run("date", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomLocalDate()
			}

			testSendAndReceiveValue(t, list)
		})

		inner.Run("local time", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomLocalTime()
			}

			testSendAndReceiveValue(t, list)
		})

		inner.Run("offset time", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomOffsetTime()
			}

			testSendAndReceiveValue(t, list)
		})

		inner.Run("local date time", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomLocalDateTime()
			}

			testSendAndReceiveValueComp(t, list, func(x, y interface{}) bool {
				l1 := x.([]interface{})
				l2 := y.([]interface{})

				equal := true
				for i, x := range l1 {
					y := l2[i]
					d1 := x.(neo4j.LocalDateTime)
					d2 := y.(neo4j.LocalDateTime)
					if !d1.Time().Equal(d2.Time()) {
						equal = false
					}
				}
				return equal
			})
		})

		inner.Run("offset date time", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomOffsetDateTime()
			}

			testSendAndReceiveValue(t, list)
		})

		inner.Run("zoned date time", func(t *testing.T) {
			listSize := rand.Intn(1000)
			list := make([]interface{}, listSize)
			for i := 0; i < listSize; i++ {
				list[i] = randomZonedDateTime(t)
			}

			testSendAndReceiveValue(t, list)
		})
	})

	outer.Run("should be able to send and receive nil pointer property", func(inner *testing.T) {
		nilPointers := map[string]interface{}{
			"Date":          (*neo4j.Date)(nil),
			"Duration":      (*neo4j.Duration)(nil),
			"LocalDateTime": (*neo4j.LocalDateTime)(nil),
			"LocalTime":     (*neo4j.LocalTime)(nil),
			"OffsetTime":    (*neo4j.OffsetTime)(nil),
			"Time":          (*time.Time)(nil),
		}

		for _, key := range sortedKeys(nilPointers) {
			inner.Run(fmt.Sprintf("with %s type", key), func(t *testing.T) {
				value := nilPointers[key]
				result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
				assertNil(t, err)

				if result.Next() {
					assertNil(t, result.Record().Values[0])
				}
				assertFalse(t, result.Next())
				assertNil(t, result.Err())
			})
		}
	})
}

func sortedKeys(m map[string]interface{}) []string {
	result := make([]string, len(m))
	i := 0
	for k := range m {
		result[i] = k
		i++
	}
	sort.Strings(result)
	return result
}
