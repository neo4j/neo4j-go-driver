/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package dbtype

import (
	"testing"
)

func TestSpatialTypes(t *testing.T) {
	t.Run("String representation of Point2D", func(t *testing.T) {
		point := Point2D{SpatialRefId: 1, X: 1.0, Y: 2.0}
		actual := point.String()
		expect := "Point{srId=1, x=1.000000, y=2.000000}"
		if actual != expect {
			t.Errorf("Expected %s but was %s", expect, actual)
		}
	})

	t.Run("String representation of Point3D", func(t *testing.T) {
		point := Point3D{SpatialRefId: 1, X: 1.0, Y: 2.0, Z: 3.0}
		actual := point.String()
		expect := "Point{srId=1, x=1.000000, y=2.000000, z=3.000000}"
		if actual != expect {
			t.Errorf("Expected %s but was %s", expect, actual)
		}
	})
}
