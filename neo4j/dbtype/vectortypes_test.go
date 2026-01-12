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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

// TestVectorElementTypes verifies Vector compiles with all supported element types.
func TestVectorElementTypes(t *testing.T) {
	t.Parallel()

	var _ Vector[float64]
	var _ Vector[float32]
	var _ Vector[int8]
	var _ Vector[int16]
	var _ Vector[int32]
	var _ Vector[int64]
}

func TestVectorString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		vec      any
		expected string
	}{
		// Empty vectors
		{"empty int8", Vector[int8]{Elems: []int8{}}, "vector([], 0, INTEGER8 NOT NULL)"},
		{"empty int16", Vector[int16]{Elems: []int16{}}, "vector([], 0, INTEGER16 NOT NULL)"},
		{"empty int32", Vector[int32]{Elems: []int32{}}, "vector([], 0, INTEGER32 NOT NULL)"},
		{"empty int64", Vector[int64]{Elems: []int64{}}, "vector([], 0, INTEGER NOT NULL)"},
		{"empty float32", Vector[float32]{Elems: []float32{}}, "vector([], 0, FLOAT32 NOT NULL)"},
		{"empty float64", Vector[float64]{Elems: []float64{}}, "vector([], 0, FLOAT NOT NULL)"},

		// Single element vectors
		{"single int32", Vector[int32]{Elems: []int32{42}}, "vector([42], 1, INTEGER32 NOT NULL)"},
		{"single float64", Vector[float64]{Elems: []float64{3.14}}, "vector([3.14], 1, FLOAT NOT NULL)"},

		// Multiple element vectors
		{"int8 multiple", Vector[int8]{Elems: []int8{1, 2, 3}}, "vector([1, 2, 3], 3, INTEGER8 NOT NULL)"},
		{"int16 multiple", Vector[int16]{Elems: []int16{10, 20, 30}}, "vector([10, 20, 30], 3, INTEGER16 NOT NULL)"},
		{"int32 multiple", Vector[int32]{Elems: []int32{100, 200, 300}}, "vector([100, 200, 300], 3, INTEGER32 NOT NULL)"},
		{"int64 multiple", Vector[int64]{Elems: []int64{1000, 2000, 3000}}, "vector([1000, 2000, 3000], 3, INTEGER NOT NULL)"},
		{"float32 multiple", Vector[float32]{Elems: []float32{1.0, 2.0, 3.0}}, "vector([1.0, 2.0, 3.0], 3, FLOAT32 NOT NULL)"},
		{"float64 multiple", Vector[float64]{Elems: []float64{1.1, 2.2, 3.3}}, "vector([1.1, 2.2, 3.3], 3, FLOAT NOT NULL)"},

		// Zero values
		{"int32 zeros", Vector[int32]{Elems: []int32{0, 0, 0}}, "vector([0, 0, 0], 3, INTEGER32 NOT NULL)"},
		{"float64 zeros", Vector[float64]{Elems: []float64{0.0, 0.0, 0.0}}, "vector([0.0, 0.0, 0.0], 3, FLOAT NOT NULL)"},

		// Negative numbers
		{"int32 negative", Vector[int32]{Elems: []int32{-1, -2, -3}}, "vector([-1, -2, -3], 3, INTEGER32 NOT NULL)"},
		{"float64 negative", Vector[float64]{Elems: []float64{-1.5, -2.5, -3.5}}, "vector([-1.5, -2.5, -3.5], 3, FLOAT NOT NULL)"},

		// Special float values
		{"special floats", Vector[float64]{Elems: []float64{math.NaN(), math.Inf(1), math.Inf(-1)}}, "vector([NaN, Infinity, -Infinity], 3, FLOAT NOT NULL)"},
		{"mixed special floats", Vector[float64]{Elems: []float64{math.NaN(), 0.0, math.Inf(1), -1.0, math.Inf(-1)}}, "vector([NaN, 0.0, Infinity, -1.0, -Infinity], 5, FLOAT NOT NULL)"},

		// Very large numbers
		{"very large int64", Vector[int64]{Elems: []int64{math.MaxInt64, math.MinInt64, 0}}, fmt.Sprintf("vector([%d, %d, 0], 3, INTEGER NOT NULL)", math.MaxInt64, math.MinInt64)},

		// Scientific notation floats
		{"scientific floats", Vector[float64]{Elems: []float64{1e10, 2e-5, 3.14159e2}}, "vector([10000000000.0, 2e-05, 314.159], 3, FLOAT NOT NULL)"},

		// Precision test cases
		{"float64 precision", Vector[float64]{Elems: []float64{0.123}}, "vector([0.123], 1, FLOAT NOT NULL)"},
		{"float32 precision", Vector[float32]{Elems: []float32{0.123}}, "vector([0.123], 1, FLOAT32 NOT NULL)"},

		// Sub-normal floats
		{"subnormal float64", Vector[float64]{Elems: []float64{math.SmallestNonzeroFloat64}}, "vector([5e-324], 1, FLOAT NOT NULL)"},
		{"subnormal float32", Vector[float32]{Elems: []float32{math.SmallestNonzeroFloat32}}, "vector([1e-45], 1, FLOAT32 NOT NULL)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := fmt.Sprintf("%s", tc.vec)
			testutil.AssertDeepEquals(t, result, tc.expected)
		})
	}
}
