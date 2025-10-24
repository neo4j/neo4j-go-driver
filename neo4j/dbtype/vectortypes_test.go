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
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

func TestVectorAPI(t *testing.T) {
	t.Parallel()
	float64Vec := Vector[float64]{1.0, 2.0, 3.0, 4.0, 5.0}
	float32Vec := Vector[float32]{0.1, 0.2, 0.3, 0.4, 0.5}

	// Test type assertions
	typeTests := []struct {
		name     string
		vec      any
		expected reflect.Type
	}{
		{"float64", float64Vec, reflect.TypeOf(Vector[float64]{})},
		{"float32", float32Vec, reflect.TypeOf(Vector[float32]{})},
	}

	for _, tt := range typeTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if reflect.TypeOf(tt.vec) != tt.expected {
				t.Errorf("Expected %s to be of type %v", tt.name, tt.expected)
			}
		})
	}

	// Test vector operations
	t.Run("length", func(t *testing.T) {
		t.Parallel()
		testutil.AssertLen(t, float64Vec, 5)
		testutil.AssertLen(t, float32Vec, 5)
	})

	t.Run("access", func(t *testing.T) {
		t.Parallel()
		accessVec64 := Vector[float64]{1.0, 2.0, 3.0, 4.0, 5.0}
		accessVec32 := Vector[float32]{0.1, 0.2, 0.3, 0.4, 0.5}
		testutil.AssertDeepEquals(t, accessVec64[0], 1.0)
		testutil.AssertDeepEquals(t, accessVec32[1], float32(0.2))
	})

	t.Run("modification", func(t *testing.T) {
		t.Parallel()
		modVec := Vector[float64]{1.0, 2.0, 3.0, 4.0, 5.0}
		modVec[0] = 10.0
		testutil.AssertDeepEquals(t, modVec[0], 10.0)
	})

	t.Run("make", func(t *testing.T) {
		t.Parallel()
		largeVec := make(Vector[float64], 100)
		testutil.AssertLen(t, largeVec, 100)
	})

	t.Run("append", func(t *testing.T) {
		t.Parallel()
		vec := Vector[float64]{1.0, 2.0}
		vec = append(vec, 3.0)
		testutil.AssertLen(t, vec, 3)
		testutil.AssertDeepEquals(t, vec[2], 3.0)
	})

	t.Run("maps", func(t *testing.T) {
		t.Parallel()
		params := map[string]any{
			"float64_vec": float64Vec,
			"float32_vec": float32Vec,
		}

		vec64, ok := params["float64_vec"].(Vector[float64])
		testutil.AssertTrue(t, ok)
		testutil.AssertLen(t, vec64, 5)

		vec32, ok := params["float32_vec"].(Vector[float32])
		testutil.AssertTrue(t, ok)
		testutil.AssertLen(t, vec32, 5)
	})

	t.Run("slices", func(t *testing.T) {
		t.Parallel()
		vecSlice := []Vector[float64]{float64Vec, {6.0, 7.0, 8.0}}
		testutil.AssertLen(t, vecSlice, 2)
	})

	t.Run("comparison", func(t *testing.T) {
		t.Parallel()
		vec1 := Vector[float64]{1.0, 2.0, 3.0}
		vec2 := Vector[float64]{1.0, 2.0, 3.0}
		vec3 := Vector[float64]{1.0, 2.0, 4.0}

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
		{"float64", Vector[float64]{1.0, 2.0, 3.0}, 3},
		{"float32", Vector[float32]{1.0, 2.0, 3.0}, 3},
		{"int8", Vector[int8]{1, 2, 3}, 3},
		{"int16", Vector[int16]{1, 2, 3}, 3},
		{"int32", Vector[int32]{1, 2, 3}, 3},
		{"int64", Vector[int64]{1, 2, 3}, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Test that the vector can be created (compilation test)
			testutil.AssertNotNil(t, tc.vec)

			// Test length using reflection
			vecValue := reflect.ValueOf(tc.vec)
			testutil.AssertIntEqual(t, vecValue.Len(), tc.len)
		})
	}
}

func TestVectorEmptyAndNil(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		emptyVec := Vector[float64]{}
		testutil.AssertLen(t, emptyVec, 0)
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		var nilVec Vector[float64]
		testutil.AssertLen(t, nilVec, 0)

		// Test that we can append to nil vectors
		nilVec = append(nilVec, 1.0)
		testutil.AssertLen(t, nilVec, 1)
		testutil.AssertDeepEquals(t, nilVec[0], 1.0)
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
		{"empty int8", Vector[int8]{}, "vector([], 0, INTEGER8 NOT NULL)"},
		{"empty int16", Vector[int16]{}, "vector([], 0, INTEGER16 NOT NULL)"},
		{"empty int32", Vector[int32]{}, "vector([], 0, INTEGER32 NOT NULL)"},
		{"empty int64", Vector[int64]{}, "vector([], 0, INTEGER NOT NULL)"},
		{"empty float32", Vector[float32]{}, "vector([], 0, FLOAT32 NOT NULL)"},
		{"empty float64", Vector[float64]{}, "vector([], 0, FLOAT NOT NULL)"},

		// Single element vectors
		{"single int32", Vector[int32]{42}, "vector([42], 1, INTEGER32 NOT NULL)"},
		{"single float64", Vector[float64]{3.14}, "vector([3.14], 1, FLOAT NOT NULL)"},

		// Multiple element vectors
		{"int8 multiple", Vector[int8]{1, 2, 3}, "vector([1, 2, 3], 3, INTEGER8 NOT NULL)"},
		{"int16 multiple", Vector[int16]{10, 20, 30}, "vector([10, 20, 30], 3, INTEGER16 NOT NULL)"},
		{"int32 multiple", Vector[int32]{100, 200, 300}, "vector([100, 200, 300], 3, INTEGER32 NOT NULL)"},
		{"int64 multiple", Vector[int64]{1000, 2000, 3000}, "vector([1000, 2000, 3000], 3, INTEGER NOT NULL)"},
		{"float32 multiple", Vector[float32]{1.0, 2.0, 3.0}, "vector([1.0, 2.0, 3.0], 3, FLOAT32 NOT NULL)"},
		{"float64 multiple", Vector[float64]{1.1, 2.2, 3.3}, "vector([1.1, 2.2, 3.3], 3, FLOAT NOT NULL)"},

		// Zero values
		{"int32 zeros", Vector[int32]{0, 0, 0}, "vector([0, 0, 0], 3, INTEGER32 NOT NULL)"},
		{"float64 zeros", Vector[float64]{0.0, 0.0, 0.0}, "vector([0.0, 0.0, 0.0], 3, FLOAT NOT NULL)"},

		// Negative numbers
		{"int32 negative", Vector[int32]{-1, -2, -3}, "vector([-1, -2, -3], 3, INTEGER32 NOT NULL)"},
		{"float64 negative", Vector[float64]{-1.5, -2.5, -3.5}, "vector([-1.5, -2.5, -3.5], 3, FLOAT NOT NULL)"},

		// Special float values
		{"special floats", Vector[float64]{math.NaN(), math.Inf(1), math.Inf(-1)}, "vector([NaN, Infinity, -Infinity], 3, FLOAT NOT NULL)"},
		{"mixed special floats", Vector[float64]{math.NaN(), 0.0, math.Inf(1), -1.0, math.Inf(-1)}, "vector([NaN, 0.0, Infinity, -1.0, -Infinity], 5, FLOAT NOT NULL)"},

		// Very large numbers
		{"very large int64", Vector[int64]{math.MaxInt64, math.MinInt64, 0}, fmt.Sprintf("vector([%d, %d, 0], 3, INTEGER NOT NULL)", math.MaxInt64, math.MinInt64)},

		// Scientific notation floats
		{"scientific floats", Vector[float64]{1e10, 2e-5, 3.14159e2}, "vector([10000000000.0, 2e-05, 314.159], 3, FLOAT NOT NULL)"},

		// Precision test cases
		{"float64 precision", Vector[float64]{0.123}, "vector([0.123], 1, FLOAT NOT NULL)"},
		{"float32 precision", Vector[float32]{0.123}, "vector([0.123], 1, FLOAT32 NOT NULL)"},

		// Sub-normal floats
		{"subnormal float64", Vector[float64]{math.SmallestNonzeroFloat64}, "vector([5e-324], 1, FLOAT NOT NULL)"},
		{"subnormal float32", Vector[float32]{math.SmallestNonzeroFloat32}, "vector([1e-45], 1, FLOAT32 NOT NULL)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := fmt.Sprintf("%s", tc.vec)
			testutil.AssertDeepEquals(t, result, tc.expected)
		})
	}
}
