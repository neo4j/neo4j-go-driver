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
	"testing"
	"time"
)

type stringTestCase[T interface{ String() string }] struct {
	input  T
	output string
}

func (c stringTestCase[T]) test(t *testing.T) {
	if c.input.String() != c.output {
		t.Errorf("Expected %s but was %s", c.output, c.input.String())
	}
	if fmt.Sprint(c.input) != c.output {
		t.Errorf("Expected %s but was %s", c.output, fmt.Sprintln(c.input))
	}
}

func TestTimeString(outer *testing.T) {
	outer.Parallel()
	testCases := []stringTestCase[Time]{
		{
			input:  Time(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)),
			output: "00:00:00Z",
		},
		{
			input:  Time(time.Date(0, 1, 1, 1, 2, 3, 4, time.UTC)),
			output: "01:02:03.000000004Z",
		},
		{
			input:  Time(time.Date(0, 1, 1, 15, 56, 34, 2_000_000, time.UTC)),
			output: "15:56:34.002Z",
		},
		{
			input:  Time(time.Date(0, 1, 1, 0, 0, 0, 2_000_000, time.FixedZone("Foo", 0))),
			output: "00:00:00.002Z",
		},
		{
			input:  Time(time.Date(0, 1, 1, 0, 0, 0, 2_000_000, time.FixedZone("Foo", -3600))),
			output: "00:00:00.002-01:00",
		},
		{
			input:  Time(time.Date(0, 1, 1, 0, 0, 0, 2_000_000, time.FixedZone("Foo", 3600))),
			output: "00:00:00.002+01:00",
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.output, func(inner *testing.T) {
			inner.Parallel()
			testCase.test(inner)
		})
	}
}

func TestDateString(outer *testing.T) {
	outer.Parallel()
	testCases := []stringTestCase[Date]{
		{input: Date(time.Date(-1, time.January, 1, 0, 0, 0, 0, time.UTC)), output: "-0001-01-01"},
		{input: Date(time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)), output: "0000-01-01"},
		{input: Date(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)), output: "0001-01-01"},
		{input: Date(time.Date(1991, time.August, 24, 0, 0, 0, 0, time.UTC)), output: "1991-08-24"},
		{input: Date(time.Date(-753, time.April, 21, 0, 0, 0, 0, time.UTC)), output: "-0753-04-21"},
		{input: Date(time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)), output: "10000-01-01"},
		{input: Date(time.Date(-10000, time.January, 1, 0, 0, 0, 0, time.UTC)), output: "-10000-01-01"},
	}
	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.output, func(inner *testing.T) {
			inner.Parallel()
			testCase.test(inner)
		})
	}
}

func TestLocalTimeString(outer *testing.T) {
	outer.Parallel()
	testCases := []stringTestCase[LocalTime]{
		{input: LocalTime(time.Date(0, 0, 0, 1, 2, 3, 4, time.Local)), output: "01:02:03.000000004"},
		{input: LocalTime(time.Date(0, 0, 0, 1, 2, 3, 4_000_000, time.Local)), output: "01:02:03.004"},
		{input: LocalTime(time.Date(0, 0, 0, 17, 56, 34, 0, time.Local)), output: "17:56:34"},
	}
	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.output, func(inner *testing.T) {
			inner.Parallel()
			testCase.test(inner)
		})
	}
}

func TestLocalDateTimeString(outer *testing.T) {
	outer.Parallel()
	testCases := []stringTestCase[LocalDateTime]{
		{
			input:  LocalDateTime(time.Date(-1, time.January, 1, 2, 3, 4, 5_000_000, time.Local)),
			output: "-0001-01-01T02:03:04.005",
		},
		{
			input:  LocalDateTime(time.Date(0, time.January, 1, 15, 16, 17, 18, time.Local)),
			output: "0000-01-01T15:16:17.000000018",
		},
		{
			input:  LocalDateTime(time.Date(1, time.January, 1, 0, 0, 0, 0, time.Local)),
			output: "0001-01-01T00:00:00",
		},
		{
			input:  LocalDateTime(time.Date(1991, time.August, 24, 0, 0, 0, 0, time.Local)),
			output: "1991-08-24T00:00:00",
		},
		{
			input:  LocalDateTime(time.Date(-753, time.April, 21, 0, 0, 0, 0, time.Local)),
			output: "-0753-04-21T00:00:00",
		},
		{
			input:  LocalDateTime(time.Date(10000, time.January, 1, 20, 0, 18, 1, time.Local)),
			output: "10000-01-01T20:00:18.000000001",
		},
		{
			input:  LocalDateTime(time.Date(-10000, time.January, 1, 0, 0, 0, 0, time.Local)),
			output: "-10000-01-01T00:00:00",
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.output, func(inner *testing.T) {
			inner.Parallel()
			testCase.test(inner)
		})
	}
}

func TestDurationSting(outer *testing.T) {
	outer.Parallel()
	testCases := []stringTestCase[Duration]{
		{input: Duration{Months: 15, Days: 32, Seconds: 785, Nanos: 789215800}, output: "P15M32DT785.789215800S"},
		{input: Duration{Months: 0, Days: 32, Seconds: 785, Nanos: 789215800}, output: "P0M32DT785.789215800S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 785, Nanos: 789215800}, output: "P0M0DT785.789215800S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 789215800}, output: "P0M0DT0.789215800S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -1, Nanos: 0}, output: "P0M0DT-1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 999999999}, output: "P0M0DT0.999999999S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -1, Nanos: 5}, output: "P0M0DT-0.999999995S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -1, Nanos: 999999999}, output: "P0M0DT-0.000000001S"},
		{input: Duration{Months: 500, Days: 0, Seconds: 0, Nanos: 0}, output: "P500M0DT0S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 5}, output: "P0M0DT0.000000005S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -500, Nanos: 1}, output: "P0M0DT-499.999999999S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -500, Nanos: 0}, output: "P0M0DT-500S"},
		{input: Duration{Months: -10, Days: 5, Seconds: -2, Nanos: 500}, output: "P-10M5DT-1.999999500S"},
		{input: Duration{Months: -10, Days: -5, Seconds: -2, Nanos: 500}, output: "P-10M-5DT-1.999999500S"},
	}

	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.output, func(inner *testing.T) {
			inner.Parallel()
			testCase.test(inner)
		})
	}
}
