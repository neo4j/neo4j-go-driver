/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dbtype

import (
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
