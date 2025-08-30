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
		if len(float64Vec) != 5 {
			t.Errorf("Expected float64Vec to have length 5, got %d", len(float64Vec))
		}
		if len(float32Vec) != 5 {
			t.Errorf("Expected float32Vec to have length 5, got %d", len(float32Vec))
		}
	})

	t.Run("access", func(t *testing.T) {
		t.Parallel()
		if float64Vec[0] != 1.0 {
			t.Errorf("Expected float64Vec[0] to be 1.0, got %f", float64Vec[0])
		}
		if float32Vec[1] != 0.2 {
			t.Errorf("Expected float32Vec[1] to be 0.2, got %f", float32Vec[1])
		}
	})

	t.Run("modification", func(t *testing.T) {
		t.Parallel()
		float64Vec[0] = 10.0
		if float64Vec[0] != 10.0 {
			t.Errorf("Expected float64Vec[0] to be 10.0 after modification, got %f", float64Vec[0])
		}
	})

	t.Run("make", func(t *testing.T) {
		t.Parallel()
		largeVec := make(Vector[float64], 100)
		if len(largeVec) != 100 {
			t.Errorf("Expected largeVec to have length 100, got %d", len(largeVec))
		}
	})

	t.Run("append", func(t *testing.T) {
		t.Parallel()
		vec := Vector[float64]{1.0, 2.0}
		vec = append(vec, 3.0)
		if len(vec) != 3 {
			t.Errorf("Expected vec to have length 3 after append, got %d", len(vec))
		}
		if vec[2] != 3.0 {
			t.Errorf("Expected vec[2] to be 3.0, got %f", vec[2])
		}
	})

	t.Run("maps", func(t *testing.T) {
		t.Parallel()
		params := map[string]any{
			"float64_vec": float64Vec,
			"float32_vec": float32Vec,
		}

		if vec, ok := params["float64_vec"].(Vector[float64]); !ok {
			t.Errorf("Expected float64_vec to be of type Vector[float64]")
		} else if len(vec) != 5 {
			t.Errorf("Expected float64_vec to have length 5, got %d", len(vec))
		}

		if vec, ok := params["float32_vec"].(Vector[float32]); !ok {
			t.Errorf("Expected float32_vec to be of type Vector[float32]")
		} else if len(vec) != 5 {
			t.Errorf("Expected float32_vec to have length 5, got %d", len(vec))
		}
	})

	t.Run("slices", func(t *testing.T) {
		t.Parallel()
		vecSlice := []Vector[float64]{float64Vec, {6.0, 7.0, 8.0}}
		if len(vecSlice) != 2 {
			t.Errorf("Expected vecSlice to have length 2, got %d", len(vecSlice))
		}
	})

	t.Run("comparison", func(t *testing.T) {
		t.Parallel()
		vec1 := Vector[float64]{1.0, 2.0, 3.0}
		vec2 := Vector[float64]{1.0, 2.0, 3.0}
		vec3 := Vector[float64]{1.0, 2.0, 4.0}

		if !reflect.DeepEqual(vec1, vec2) {
			t.Errorf("Expected vec1 and vec2 to be equal")
		}

		if reflect.DeepEqual(vec1, vec3) {
			t.Errorf("Expected vec1 and vec3 to be different")
		}
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
			if tc.vec == nil {
				t.Errorf("Vector creation failed for %s", tc.name)
			}

			// Test length using reflection
			vecValue := reflect.ValueOf(tc.vec)
			if vecValue.Len() != tc.len {
				t.Errorf("Expected %s vector to have length %d, got %d", tc.name, tc.len, vecValue.Len())
			}
		})
	}
}

func TestVectorEmptyAndNil(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		emptyVec := Vector[float64]{}
		if len(emptyVec) != 0 {
			t.Errorf("Expected emptyVec to have length 0, got %d", len(emptyVec))
		}
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		var nilVec Vector[float64]
		if len(nilVec) != 0 {
			t.Errorf("Expected nilVec to have length 0, got %d", len(nilVec))
		}

		// Test that we can append to nil vectors
		nilVec = append(nilVec, 1.0)
		if len(nilVec) != 1 {
			t.Errorf("Expected nilVec to have length 1 after append, got %d", len(nilVec))
		}
		if nilVec[0] != 1.0 {
			t.Errorf("Expected nilVec[0] to be 1.0, got %f", nilVec[0])
		}
	})
}
