/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

type Point struct {
	dimension int
	srId      int
	x         float64
	y         float64
	z         float64
}

func NewPoint2D(srId int, x float64, y float64) *Point {
	return &Point{
		dimension: 2,
		srId:      srId,
		x:         x,
		y:         y,
		z:         math.NaN(),
	}
}

func NewPoint3D(srId int, x float64, y float64, z float64) *Point {
	return &Point{
		dimension: 3,
		srId:      srId,
		x:         x,
		y:         y,
		z:         z,
	}
}

func (point *Point) SrId() int {
	return point.srId
}

func (point *Point) X() float64 {
	return point.x
}

func (point *Point) Y() float64 {
	return point.y
}

func (point *Point) Z() float64 {
	return point.z
}

func (point *Point) String() string {
	if point.dimension == 2 {
		return fmt.Sprintf("Point{srId=%d, x=%f, y=%f}", point.srId, point.x, point.y)
	} else {
		return fmt.Sprintf("Point{srId=%d, x=%f, y=%f, z=%f}", point.srId, point.x, point.y, point.z)
	}
}