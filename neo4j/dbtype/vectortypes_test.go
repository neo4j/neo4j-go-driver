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

func TestVectorAPI(t *testing.T) {
	t.Parallel()
	float64Vec := NewVector(1.0, 2.0, 3.0, 4.0, 5.0)
	float32Vec := NewVector(float32(0.1), float32(0.2), float32(0.3), float32(0.4), float32(0.5))

	// Test type assertions - verify values implement Vector interface
	typeTests := []struct {
		name string
		vec  any
	}{
		{"float64", float64Vec},
		{"float32", float32Vec},
	}

	for _, tt := range typeTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Verify the value implements the Vector interface
			switch v := tt.vec.(type) {
			case Vector[float64]:
				testutil.AssertNotNil(t, v)
			case Vector[float32]:
				testutil.AssertNotNil(t, v)
			default:
				t.Errorf("Expected %s to implement Vector interface, but type assertion failed. Got type: %T", tt.name, tt.vec)
			}
		})
	}

	// Test vector operations
	t.Run("length", func(t *testing.T) {
		t.Parallel()
		testutil.AssertIntEqual(t, float64Vec.Len(), 5)
		testutil.AssertIntEqual(t, float32Vec.Len(), 5)
	})

	t.Run("access", func(t *testing.T) {
		t.Parallel()
		accessVec64 := NewVector(1.0, 2.0, 3.0, 4.0, 5.0)
		accessVec32 := NewVector(float32(0.1), float32(0.2), float32(0.3), float32(0.4), float32(0.5))
		testutil.AssertDeepEquals(t, accessVec64.At(0), 1.0)
		testutil.AssertDeepEquals(t, accessVec32.At(1), float32(0.2))
	})

	t.Run("slice", func(t *testing.T) {
		t.Parallel()
		vec := NewVector(1.0, 2.0, 3.0, 4.0, 5.0)
		slice := vec.Slice()
		testutil.AssertLen(t, slice, 5)
		testutil.AssertDeepEquals(t, slice[0], 1.0)
		testutil.AssertDeepEquals(t, slice[4], 5.0)

		// Modifying the slice shouldn't affect the vector
		slice[0] = 10.0
		testutil.AssertDeepEquals(t, vec.At(0), 1.0)
	})

	t.Run("make", func(t *testing.T) {
		t.Parallel()
		largeSlice := make([]float64, 100)
		largeVec := NewVectorFromSlice(largeSlice)
		testutil.AssertIntEqual(t, largeVec.Len(), 100)
	})

	t.Run("maps", func(t *testing.T) {
		t.Parallel()
		params := map[string]any{
			"float64_vec": float64Vec,
			"float32_vec": float32Vec,
		}

		vec64, ok := params["float64_vec"].(Vector[float64])
		testutil.AssertTrue(t, ok)
		testutil.AssertIntEqual(t, vec64.Len(), 5)

		vec32, ok := params["float32_vec"].(Vector[float32])
		testutil.AssertTrue(t, ok)
		testutil.AssertIntEqual(t, vec32.Len(), 5)
	})

	t.Run("slices", func(t *testing.T) {
		t.Parallel()
		vecSlice := []Vector[float64]{float64Vec, NewVector(6.0, 7.0, 8.0)}
		testutil.AssertLen(t, vecSlice, 2)
	})

	t.Run("comparison", func(t *testing.T) {
		t.Parallel()
		vec1 := NewVector(1.0, 2.0, 3.0)
		vec2 := NewVector(1.0, 2.0, 3.0)
		vec3 := NewVector(1.0, 2.0, 4.0)

		testutil.AssertDeepEquals(t, vec1, vec2)
		testutil.AssertNotDeepEquals(t, vec1, vec3)
	})
}

func TestVectorElementInterface(t *testing.T) {
	t.Parallel()
	// Test all supported element types
	type testCase struct {
		name string
		vec  any
		len  int
	}

	testCases := []testCase{
		{"float64", NewVector(1.0, 2.0, 3.0), 3},
		{"float32", NewVector(float32(1.0), float32(2.0), float32(3.0)), 3},
		{"int8", NewVector(int8(1), int8(2), int8(3)), 3},
		{"int16", NewVector(int16(1), int16(2), int16(3)), 3},
		{"int32", NewVector(int32(1), int32(2), int32(3)), 3},
		{"int64", NewVector(int64(1), int64(2), int64(3)), 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Test that the vector can be created (compilation test)
			testutil.AssertNotNil(t, tc.vec)

			vec := tc.vec.(interface{ Len() int })
			testutil.AssertIntEqual(t, vec.Len(), tc.len)
		})
	}
}

func TestVectorEmpty(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		emptyVec := NewVector[float64]()
		testutil.AssertIntEqual(t, emptyVec.Len(), 0)
	})
}

func TestVectorString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		vec      any
		expected string
	}{
		// Empty vectors
		{"empty int8", NewVector[int8](), "vector([], 0, INTEGER8 NOT NULL)"},
		{"empty int16", NewVector[int16](), "vector([], 0, INTEGER16 NOT NULL)"},
		{"empty int32", NewVector[int32](), "vector([], 0, INTEGER32 NOT NULL)"},
		{"empty int64", NewVector[int64](), "vector([], 0, INTEGER NOT NULL)"},
		{"empty float32", NewVector[float32](), "vector([], 0, FLOAT32 NOT NULL)"},
		{"empty float64", NewVector[float64](), "vector([], 0, FLOAT NOT NULL)"},

		// Single element vectors
		{"single int32", NewVector(int32(42)), "vector([42], 1, INTEGER32 NOT NULL)"},
		{"single float64", NewVector(3.14), "vector([3.14], 1, FLOAT NOT NULL)"},

		// Multiple element vectors
		{"int8 multiple", NewVector(int8(1), int8(2), int8(3)), "vector([1, 2, 3], 3, INTEGER8 NOT NULL)"},
		{"int16 multiple", NewVector(int16(10), int16(20), int16(30)), "vector([10, 20, 30], 3, INTEGER16 NOT NULL)"},
		{"int32 multiple", NewVector(int32(100), int32(200), int32(300)), "vector([100, 200, 300], 3, INTEGER32 NOT NULL)"},
		{"int64 multiple", NewVector(int64(1000), int64(2000), int64(3000)), "vector([1000, 2000, 3000], 3, INTEGER NOT NULL)"},
		{"float32 multiple", NewVector(float32(1.0), float32(2.0), float32(3.0)), "vector([1.0, 2.0, 3.0], 3, FLOAT32 NOT NULL)"},
		{"float64 multiple", NewVector(1.1, 2.2, 3.3), "vector([1.1, 2.2, 3.3], 3, FLOAT NOT NULL)"},

		// Zero values
		{"int32 zeros", NewVector(int32(0), int32(0), int32(0)), "vector([0, 0, 0], 3, INTEGER32 NOT NULL)"},
		{"float64 zeros", NewVector(0.0, 0.0, 0.0), "vector([0.0, 0.0, 0.0], 3, FLOAT NOT NULL)"},

		// Negative numbers
		{"int32 negative", NewVector(int32(-1), int32(-2), int32(-3)), "vector([-1, -2, -3], 3, INTEGER32 NOT NULL)"},
		{"float64 negative", NewVector(-1.5, -2.5, -3.5), "vector([-1.5, -2.5, -3.5], 3, FLOAT NOT NULL)"},

		// Special float values
		{"special floats", NewVector(math.NaN(), math.Inf(1), math.Inf(-1)), "vector([NaN, Infinity, -Infinity], 3, FLOAT NOT NULL)"},
		{"mixed special floats", NewVector(math.NaN(), 0.0, math.Inf(1), -1.0, math.Inf(-1)), "vector([NaN, 0.0, Infinity, -1.0, -Infinity], 5, FLOAT NOT NULL)"},

		// Very large numbers
		{"very large int64", NewVector(int64(math.MaxInt64), int64(math.MinInt64), int64(0)), fmt.Sprintf("vector([%d, %d, 0], 3, INTEGER NOT NULL)", math.MaxInt64, math.MinInt64)},

		// Scientific notation floats
		{"scientific floats", NewVector(1e10, 2e-5, 3.14159e2), "vector([10000000000.0, 2e-05, 314.159], 3, FLOAT NOT NULL)"},

		// Precision test cases
		{"float64 precision", NewVector(0.123), "vector([0.123], 1, FLOAT NOT NULL)"},
		{"float32 precision", NewVector(float32(0.123)), "vector([0.123], 1, FLOAT32 NOT NULL)"},

		// Sub-normal floats
		{"subnormal float64", NewVector(math.SmallestNonzeroFloat64), "vector([5e-324], 1, FLOAT NOT NULL)"},
		{"subnormal float32", NewVector(float32(math.SmallestNonzeroFloat32)), "vector([1e-45], 1, FLOAT32 NOT NULL)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := fmt.Sprintf("%s", tc.vec)
			testutil.AssertDeepEquals(t, result, tc.expected)
		})
	}
}
