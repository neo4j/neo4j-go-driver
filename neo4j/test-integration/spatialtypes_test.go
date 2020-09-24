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

package test_integration

import (
	"math/rand"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/dbserver"
)

func TestSpatialTypes(st *testing.T) {
	const (
		WGS84SrID       uint32 = 4326
		WGS843DSrID     uint32 = 4979
		CartesianSrID   uint32 = 7203
		Cartesian3DSrID uint32 = 9157
	)

	server := dbserver.GetDbServer()
	driver := server.Driver()
	if server.Version.LessThan(V340) {
		st.Skip("Spatial types are only available after neo4j 3.4.0 release")
	}

	// Returns a random 2D/3D point (depending on seq) with random values for X, Y and Z when applicable.
	randomPoint := func(seq int) interface{} {
		random := func() float64 {
			return float64(rand.Intn(360)-180) + rand.Float64()
		}

		randomY := func() float64 {
			return float64(rand.Intn(180)-90) + rand.Float64()
		}

		switch seq % 4 {
		case 0:
			return neo4j.Point2D{SpatialRefId: WGS84SrID, X: random(), Y: randomY()}
		case 1:
			return neo4j.Point3D{SpatialRefId: WGS843DSrID, X: random(), Y: randomY(), Z: random()}
		case 2:
			return neo4j.Point2D{SpatialRefId: CartesianSrID, X: random(), Y: random()}
		case 3:
			return neo4j.Point3D{SpatialRefId: Cartesian3DSrID, X: random(), Y: random(), Z: random()}
		default:
			panic("Not expected")
		}
	}

	randomPoints := func(seq, num int) []interface{} {
		points := make([]interface{}, num)
		for i, _ := range points {
			points[i] = randomPoint(seq)
		}
		return points
	}

	sendAndReceive := func(t *testing.T, p interface{}) interface{} {
		return single(t, driver, "CREATE (n:POI { x: $p}) RETURN n.x", map[string]interface{}{"p": p})
	}

	assertPoint2D := func(t *testing.T, p1, p2 neo4j.Point2D) {
		t.Helper()
		if p1.X != p2.X || p1.Y != p2.Y || p1.SpatialRefId != p2.SpatialRefId {
			t.Errorf("Point2D points differ: '%s' vs '%s'", p1.String(), p2.String())
		}
	}

	assertPoint3D := func(t *testing.T, p1, p2 neo4j.Point3D) {
		t.Helper()
		if p1.X != p2.X || p1.Y != p2.Y || p1.Z != p2.Z || p1.SpatialRefId != p2.SpatialRefId {
			t.Errorf("Point3D points differ: '%s' vs '%s'", p1.String(), p2.String())
		}
	}

	assertPoint := func(t *testing.T, p1, p2 interface{}) {
		_, ok := p1.(neo4j.Point2D)
		if ok {
			// Let it panic if p2 is not 2D
			assertPoint2D(t, p1.(neo4j.Point2D), p2.(neo4j.Point2D))
			return
		}
		// Let it panic if not 3D
		assertPoint3D(t, p1.(neo4j.Point3D), p2.(neo4j.Point3D))
	}

	// Verifies that a single 2D point can be received.
	st.Run("Receive", func(rt *testing.T) {
		rt.Run("Point2D", func(t *testing.T) {
			p1 := single(t, driver, "RETURN point({x: 39.111748, y:-76.775635})", nil).(neo4j.Point2D)
			p2 := neo4j.Point2D{X: 39.111748, Y: -76.775635, SpatialRefId: CartesianSrID}
			assertPoint2D(t, p1, p2)
		})

		// Verifies that a single 3D point can be received.
		rt.Run("Point3D", func(t *testing.T) {
			p1 := single(t, driver, "RETURN point({x: 39.111748, y:-76.775635, z:35.120})", nil).(neo4j.Point3D)
			p2 := neo4j.Point3D{X: 39.111748, Y: -76.775635, Z: 35.120, SpatialRefId: Cartesian3DSrID}
			assertPoint3D(t, p1, p2)
		})
	})

	st.Run("Send", func(tt *testing.T) {
		// Verifies that a single 2D point can be sent (and received)
		tt.Run("Point2D", func(t *testing.T) {
			p1 := neo4j.Point2D{SpatialRefId: WGS84SrID, X: 51.5044585, Y: -0.105658}
			p2 := sendAndReceive(t, p1).(neo4j.Point2D)
			assertPoint2D(t, p1, p2)
		})

		// Verifies that a single 3D point can be sent (and received)
		tt.Run("Point3D", func(t *testing.T) {
			p1 := neo4j.Point3D{SpatialRefId: WGS843DSrID, X: 51.5044585, Y: -0.105658, Z: 35.120}
			p2 := sendAndReceive(t, p1).(neo4j.Point3D)
			assertPoint3D(t, p1, p2)
		})

		// Verifies that a single 2D point can be sent by value
		tt.Run("Point2D by value", func(t *testing.T) {
			p1 := neo4j.Point2D{SpatialRefId: WGS84SrID, X: 51.5044585, Y: -0.105658}
			p2 := sendAndReceive(t, p1).(neo4j.Point2D)
			assertPoint2D(t, p1, p2)
		})

		// Verifies that a single 3D point can be sent by value
		tt.Run("Point3D by value", func(t *testing.T) {
			p1 := neo4j.Point3D{SpatialRefId: WGS843DSrID, X: 51.5044585, Y: -0.105658, Z: 35.120}
			p2 := sendAndReceive(t, p1).(neo4j.Point3D)
			assertPoint3D(t, p1, p2)
		})

		// Verifies that lists of points can be sent (and received)
		tt.Run("Random lists", func(t *testing.T) {
			num := 100
			for i := 0; i < 10; i++ {
				points1 := randomPoints(i, num)
				points2 := sendAndReceive(t, points1).([]interface{})
				if len(points2) != num {
					t.Fatalf("Too few points in list, expected %d but was %d", len(points1), len(points2))
				}
				for i, p1 := range points1 {
					assertPoint(t, p1, points2[i])
				}
			}
		})
	})

	driver.Close()
}
