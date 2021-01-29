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

package neo4j

import (
	"fmt"
	"math"
)

// Point represents a single two or three dimensional point in a particular coordinate reference system.
type Point struct {
	dimension int
	srId      int
	x         float64
	y         float64
	z         float64
}

// NewPoint2D creates a two dimensional Point with provided coordinates and coordinate reference system.
func NewPoint2D(srId int, x float64, y float64) *Point {
	return &Point{
		dimension: 2,
		srId:      srId,
		x:         x,
		y:         y,
		z:         math.NaN(),
	}
}

// NewPoint3D creates a three dimensional Point with provided coordinates and coordinate reference system.
func NewPoint3D(srId int, x float64, y float64, z float64) *Point {
	return &Point{
		dimension: 3,
		srId:      srId,
		x:         x,
		y:         y,
		z:         z,
	}
}

// SrId returns the Point's coordinate reference system.
func (point *Point) SrId() int {
	return point.srId
}

// X returns the Point's X coordinate.
func (point *Point) X() float64 {
	return point.x
}

// Y returns the Point's Y coordinate.
func (point *Point) Y() float64 {
	return point.y
}

// Z returns the Point's Z coordinate.
// math.NaN is returned when the Point is two dimensional.
func (point *Point) Z() float64 {
	return point.z
}

// String returns the string representation of this Point.
func (point *Point) String() string {
	if point.dimension == 2 {
		return fmt.Sprintf("Point{srId=%d, x=%f, y=%f}", point.srId, point.x, point.y)
	}

	return fmt.Sprintf("Point{srId=%d, x=%f, y=%f, z=%f}", point.srId, point.x, point.y, point.z)
}
