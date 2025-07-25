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
	// Test that Vector types can be created and used
	float64Vec := Vector[float64]{1.0, 2.0, 3.0, 4.0, 5.0}
	float32Vec := Vector[float32]{0.1, 0.2, 0.3, 0.4, 0.5}
	int8Vec := Vector[int8]{1, 2, 3, 4, 5}
	int16Vec := Vector[int16]{10, 20, 30, 40, 50}
	int32Vec := Vector[int32]{100, 200, 300, 400, 500}
	int64Vec := Vector[int64]{1000, 2000, 3000, 4000, 5000}

	// Test that vectors have the correct type
	if reflect.TypeOf(float64Vec) != reflect.TypeOf(Vector[float64]{}) {
		t.Errorf("Expected float64Vec to be of type Vector[float64]")
	}

	if reflect.TypeOf(float32Vec) != reflect.TypeOf(Vector[float32]{}) {
		t.Errorf("Expected float32Vec to be of type Vector[float32]")
	}

	if reflect.TypeOf(int8Vec) != reflect.TypeOf(Vector[int8]{}) {
		t.Errorf("Expected int8Vec to be of type Vector[int8]")
	}

	// Test that vectors have the correct length
	if len(float64Vec) != 5 {
		t.Errorf("Expected float64Vec to have length 5, got %d", len(float64Vec))
	}

	if len(float32Vec) != 5 {
		t.Errorf("Expected float32Vec to have length 5, got %d", len(float32Vec))
	}

	// Test that vectors can be accessed
	if float64Vec[0] != 1.0 {
		t.Errorf("Expected float64Vec[0] to be 1.0, got %f", float64Vec[0])
	}

	if float32Vec[1] != 0.2 {
		t.Errorf("Expected float32Vec[1] to be 0.2, got %f", float32Vec[1])
	}

	// Test that vectors can be modified
	float64Vec[0] = 10.0
	if float64Vec[0] != 10.0 {
		t.Errorf("Expected float64Vec[0] to be 10.0 after modification, got %f", float64Vec[0])
	}

	// Test that vectors can be created with make
	largeVec := make(Vector[float64], 100)
	if len(largeVec) != 100 {
		t.Errorf("Expected largeVec to have length 100, got %d", len(largeVec))
	}

	// Test that vectors can be appended to
	vec := Vector[float64]{1.0, 2.0}
	vec = append(vec, 3.0)
	if len(vec) != 3 {
		t.Errorf("Expected vec to have length 3 after append, got %d", len(vec))
	}
	if vec[2] != 3.0 {
		t.Errorf("Expected vec[2] to be 3.0, got %f", vec[2])
	}

	// Test that vectors can be used in maps (for query parameters)
	params := map[string]any{
		"float64_vec": float64Vec,
		"float32_vec": float32Vec,
		"int8_vec":    int8Vec,
		"int16_vec":   int16Vec,
		"int32_vec":   int32Vec,
		"int64_vec":   int64Vec,
	}

	// Verify that the vectors are stored correctly in the map
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

	// Test that vectors can be used in slices
	vecSlice := []Vector[float64]{float64Vec, {6.0, 7.0, 8.0}}
	if len(vecSlice) != 2 {
		t.Errorf("Expected vecSlice to have length 2, got %d", len(vecSlice))
	}

	// Test that vectors can be compared
	vec1 := Vector[float64]{1.0, 2.0, 3.0}
	vec2 := Vector[float64]{1.0, 2.0, 3.0}
	vec3 := Vector[float64]{1.0, 2.0, 4.0}

	if !reflect.DeepEqual(vec1, vec2) {
		t.Errorf("Expected vec1 and vec2 to be equal")
	}

	if reflect.DeepEqual(vec1, vec3) {
		t.Errorf("Expected vec1 and vec3 to be different")
	}
}

func TestVectorNumericInterface(t *testing.T) {
	// Test that Vector types can be created with the supported numeric types
	_ = Vector[float64]{1.0, 2.0, 3.0}
	_ = Vector[float32]{1.0, 2.0, 3.0}
	_ = Vector[int8]{1, 2, 3}
	_ = Vector[int16]{1, 2, 3}
	_ = Vector[int32]{1, 2, 3}
	_ = Vector[int64]{1, 2, 3}

	// Test that the supported types work with Vector
	float64Vec := Vector[float64]{1.0, 2.0, 3.0}
	float32Vec := Vector[float32]{1.0, 2.0, 3.0}
	int8Vec := Vector[int8]{1, 2, 3}
	int16Vec := Vector[int16]{1, 2, 3}
	int32Vec := Vector[int32]{1, 2, 3}
	int64Vec := Vector[int64]{1, 2, 3}

	// Verify that all vectors have the expected length
	if len(float64Vec) != 3 {
		t.Errorf("Expected float64Vec to have length 3, got %d", len(float64Vec))
	}
	if len(float32Vec) != 3 {
		t.Errorf("Expected float32Vec to have length 3, got %d", len(float32Vec))
	}
	if len(int8Vec) != 3 {
		t.Errorf("Expected int8Vec to have length 3, got %d", len(int8Vec))
	}
	if len(int16Vec) != 3 {
		t.Errorf("Expected int16Vec to have length 3, got %d", len(int16Vec))
	}
	if len(int32Vec) != 3 {
		t.Errorf("Expected int32Vec to have length 3, got %d", len(int32Vec))
	}
	if len(int64Vec) != 3 {
		t.Errorf("Expected int64Vec to have length 3, got %d", len(int64Vec))
	}
}

func TestVectorEmptyAndNil(t *testing.T) {
	// Test empty vectors
	emptyVec := Vector[float64]{}
	if len(emptyVec) != 0 {
		t.Errorf("Expected emptyVec to have length 0, got %d", len(emptyVec))
	}

	// Test nil vectors
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
}
