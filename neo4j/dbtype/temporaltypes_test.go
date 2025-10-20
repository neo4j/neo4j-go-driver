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
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
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
	intIs64BitsWide := math.MaxInt64 == int64(math.MaxInt)

	var maxNanosCase, minNanosCase, maxCase, minCase stringTestCase[Duration]

	{
		var output string
		duration := Duration{Months: 0, Days: 0, Seconds: 0, Nanos: math.MaxInt}
		if intIs64BitsWide {
			output = "PT2562047H47M16.854775807S"
		} else {
			output = "PT2.147483647S"
		}
		maxNanosCase = stringTestCase[Duration]{input: duration, output: output}
	}

	{
		var output string
		duration := Duration{Months: 0, Days: 0, Seconds: 0, Nanos: math.MinInt}
		if intIs64BitsWide {
			output = "PT-2562047H-47M-16.854775808S"
		} else {
			output = "PT-2.147483648S"
		}
		minNanosCase = stringTestCase[Duration]{input: duration, output: output}
	}

	{
		var output string
		duration := Duration{Months: math.MaxInt64, Days: math.MaxInt64, Seconds: math.MaxInt64, Nanos: math.MaxInt}
		if intIs64BitsWide {
			output = "P768614336404564650Y7M9223372036854775807DT2562047790577263H17M23.854775807S"
		} else {
			output = "P768614336404564650Y7M9223372036854775807DT2562047788015215H30M9.147483647S"
		}
		maxCase = stringTestCase[Duration]{input: duration, output: output}
	}

	{
		var output string
		duration := Duration{Months: math.MinInt64, Days: math.MinInt64, Seconds: math.MinInt64, Nanos: math.MinInt}
		if intIs64BitsWide {
			output = "P-768614336404564650Y-8M-9223372036854775808DT-2562047790577263H-17M-24.854775808S"
		} else {
			output = "P-768614336404564650Y-8M-9223372036854775808DT-2562047788015215H-30M-10.147483648S"
		}
		minCase = stringTestCase[Duration]{input: duration, output: output}
	}

	outer.Parallel()
	testCases := []stringTestCase[Duration]{
		// all 0
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 0}, output: "PT0S"},
		// single positive component
		{input: Duration{Months: 1, Days: 0, Seconds: 0, Nanos: 0}, output: "P1M"},
		{input: Duration{Months: 0, Days: 1, Seconds: 0, Nanos: 0}, output: "P1D"},
		{input: Duration{Months: 0, Days: 0, Seconds: 1, Nanos: 0}, output: "PT1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 1}, output: "PT0.000000001S"},
		{input: Duration{Months: math.MaxInt64, Days: 0, Seconds: 0, Nanos: 0}, output: "P768614336404564650Y7M"},
		{input: Duration{Months: 0, Days: math.MaxInt64, Seconds: 0, Nanos: 0}, output: "P9223372036854775807D"},
		{input: Duration{Months: 0, Days: 0, Seconds: math.MaxInt64, Nanos: 0}, output: "PT2562047788015215H30M7S"},
		maxNanosCase,
		// single negative component
		{input: Duration{Months: -1, Days: 0, Seconds: 0, Nanos: 0}, output: "P-1M"},
		{input: Duration{Months: 0, Days: -1, Seconds: 0, Nanos: 0}, output: "P-1D"},
		{input: Duration{Months: 0, Days: 0, Seconds: -1, Nanos: 0}, output: "PT-1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: -1}, output: "PT-0.000000001S"},
		{input: Duration{Months: math.MinInt64, Days: 0, Seconds: 0, Nanos: 0}, output: "P-768614336404564650Y-8M"},
		{input: Duration{Months: 0, Days: math.MinInt64, Seconds: 0, Nanos: 0}, output: "P-9223372036854775808D"},
		{input: Duration{Months: 0, Days: 0, Seconds: math.MinInt64, Nanos: 0}, output: "PT-2562047788015215H-30M-8S"},
		minNanosCase,
		// only time components
		{input: Duration{Months: 0, Days: 0, Seconds: 1, Nanos: 1}, output: "PT1.000000001S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -1, Nanos: -1}, output: "PT-1.000000001S"},
		// only date components
		{input: Duration{Months: 1, Days: 1, Seconds: 0, Nanos: 0}, output: "P1M1D"},
		{input: Duration{Months: -1, Days: -1, Seconds: 0, Nanos: 0}, output: "P-1M-1D"},
		{input: Duration{Months: -1, Days: 1, Seconds: 0, Nanos: 0}, output: "P-1M1D"},
		{input: Duration{Months: 1, Days: -1, Seconds: 0, Nanos: 0}, output: "P1M-1D"},
		// all components
		{input: Duration{Months: 1, Days: 1, Seconds: 1, Nanos: 1}, output: "P1M1DT1.000000001S"},
		{input: Duration{Months: -1, Days: -1, Seconds: -1, Nanos: -1}, output: "P-1M-1DT-1.000000001S"},
		// nanos don't need trailing 0 decimals
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 1_000}, output: "PT0.000001S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: -1_000}, output: "PT-0.000001S"},
		// nanos wrap into seconds
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: 2_100_000_000}, output: "PT2.1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 0, Nanos: -2_100_000_000}, output: "PT-2.1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 3, Nanos: 2_100_000_000}, output: "PT5.1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -3, Nanos: -2_100_000_000}, output: "PT-5.1S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -3, Nanos: 2_100_000_000}, output: "PT-0.9S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 3, Nanos: -2_100_000_000}, output: "PT0.9S"},
		{input: Duration{Months: 0, Days: 0, Seconds: -3, Nanos: 2_100_000_000}, output: "PT-0.9S"},
		{input: Duration{Months: 0, Days: 0, Seconds: 3, Nanos: -2_100_000_000}, output: "PT0.9S"},
		// seconds wrap into minutes and hours
		{input: Duration{Months: 0, Days: 0, Seconds: 60, Nanos: 0}, output: "PT1M"},
		{input: Duration{Months: 0, Days: 0, Seconds: -60, Nanos: 0}, output: "PT-1M"},
		{input: Duration{Months: 0, Days: 0, Seconds: 3600, Nanos: 0}, output: "PT1H"},
		{input: Duration{Months: 0, Days: 0, Seconds: -3600, Nanos: 0}, output: "PT-1H"},
		// all max/min components
		maxCase,
		minCase,
	}

	if intIs64BitsWide {
		testCases = append(testCases, []stringTestCase[Duration]{
			// nanos wrap into seconds and hours
			{input: Duration{Months: 0, Days: 0, Seconds: 7_203, Nanos: 3_601_100_000_000}, output: "PT3H4.1S"},
			{input: Duration{Months: 0, Days: 0, Seconds: 7_263, Nanos: 3_721_100_000_000}, output: "PT3H3M4.1S"},
			{input: Duration{Months: 0, Days: 0, Seconds: 7_203, Nanos: -3_601_100_000_000}, output: "PT1H1.9S"},
			{input: Duration{Months: 0, Days: 0, Seconds: 7_383, Nanos: -3_721_100_000_000}, output: "PT1H1M1.9S"},
			{input: Duration{Months: 0, Days: 0, Seconds: -7_203, Nanos: 3_601_100_000_000}, output: "PT-1H-1.9S"},
			{input: Duration{Months: 0, Days: 0, Seconds: -7_323, Nanos: 3_661_100_000_000}, output: "PT-1H-1M-1.9S"},
		}...)
	}

	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.output, func(inner *testing.T) {
			inner.Parallel()
			testCase.test(inner)
		})
	}
}

func TestSign(outer *testing.T) {
	outer.Parallel()

	type testCaseType struct {
		name   string
		input  int64
		output int64
	}

	testCases := []testCaseType{
		{name: "0", input: 0, output: 1},
		{name: "1", input: 1, output: 1},
		{name: "2", input: 2, output: 1},
		{name: "math.MaxInt64 / 2", input: math.MaxInt64 / 2, output: 1},
		{name: "math.MaxInt64 - 1", input: math.MaxInt64 - 1, output: 1},
		{name: "math.MaxInt64", input: math.MaxInt64, output: 1},
		{name: "-1", input: -1, output: -1},
		{name: "-2", input: -2, output: -1},
		{name: "math.MinInt64 / 2", input: math.MinInt64 / 2, output: -1},
		{name: "math.MinInt64 + 1", input: math.MinInt64 + 1, output: -1},
		{name: "math.MinInt64", input: math.MinInt64, output: -1},
	}

	for _, testCase := range testCases {
		testCase := testCase
		outer.Run(testCase.name, func(inner *testing.T) {
			inner.Parallel()
			res := sign(testCase.input)
			testutil.AssertDeepEquals(inner, res, testCase.output)
		})

	}
}

func BenchmarkSign(b *testing.B) {
	numbers := make([]int64, 0, 1_000_000)
	for i := 0; i < 1_000_000; i++ {
		numbers = append(numbers, rand.Int64())
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sign(numbers[i%1_000_000])
	}
}
