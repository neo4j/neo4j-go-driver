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
	"strconv"
	"strings"
)

// VectorElement represents the supported element types for Vector.
type VectorElement interface {
	float64 | float32 | int8 | int16 | int32 | int64
}

// Vector represents a fixed-length array of numeric values.
type Vector[T VectorElement] interface {
	// Len returns the number of elements in the vector.
	Len() int

	// At returns the element at index i. It panics if i is out of range.
	At(i int) T

	// Slice returns the elements as a new slice. The returned slice is a copy.
	Slice() []T

	// String returns the string representation of this Vector in the format:
	// vector([data], length, type NOT NULL).
	String() string
}

type vector[T VectorElement] struct {
	elems []T
}

// NewVector constructs a Vector from the provided elements.
func NewVector[T VectorElement](elems ...T) Vector[T] {
	cp := make([]T, len(elems))
	copy(cp, elems)
	return &vector[T]{elems: cp}
}

// NewVectorFromSlice constructs a Vector from a slice.
func NewVectorFromSlice[T VectorElement](elems []T) Vector[T] {
	return NewVector(elems...)
}

func (v *vector[T]) Len() int {
	return len(v.elems)
}

func (v *vector[T]) At(i int) T {
	return v.elems[i]
}

func (v *vector[T]) Slice() []T {
	cp := make([]T, len(v.elems))
	copy(cp, v.elems)
	return cp
}

func (v *vector[T]) String() string {
	dataStr := formatVectorData(v.elems)
	length := len(v.elems)
	typeStr := getVectorTypeString[T]()

	return fmt.Sprintf("vector([%s], %d, %s)", dataStr, length, typeStr)
}

func getVectorTypeString[T VectorElement]() string {
	var typeDiscriminator T
	switch any(typeDiscriminator).(type) {
	case int8:
		return "INTEGER8 NOT NULL"
	case int16:
		return "INTEGER16 NOT NULL"
	case int32:
		return "INTEGER32 NOT NULL"
	case int64:
		return "INTEGER NOT NULL"
	case float32:
		return "FLOAT32 NOT NULL"
	case float64:
		return "FLOAT NOT NULL"
	default:
		return "UNKNOWN NOT NULL"
	}
}

func formatVectorData[T VectorElement](v []T) string {
	if len(v) == 0 {
		return ""
	}

	parts := make([]string, len(v))
	for i, element := range v {
		parts[i] = formatElement(element)
	}
	return strings.Join(parts, ", ")
}

func formatElement[T VectorElement](element T) string {
	switch e := any(element).(type) {
	case float32:
		return formatFloat(float64(e), 32)
	case float64:
		return formatFloat(e, 64)
	case int8, int16, int32, int64:
		return fmt.Sprintf("%d", e)
	default:
		return fmt.Sprintf("%v", e)
	}
}

func formatFloat(f float64, bitSize int) string {
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	if isWholeNumber(f) {
		// Ensure we show at least one decimal place
		return strconv.FormatFloat(f, 'f', 1, bitSize)
	}
	return strconv.FormatFloat(f, 'g', -1, bitSize)
}

func isWholeNumber(f float64) bool {
	return f == math.Trunc(f)
}
