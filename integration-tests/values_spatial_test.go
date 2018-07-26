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

package integration_tests

import (
	"math"
	"math/rand"
	"time"

	. "github.com/neo4j/neo4j-go-driver"
	. "github.com/neo4j/neo4j-go-driver/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Spatial Types", func() {
	const (
		WGS84SrId int = 4326
		WGS843DSrId int = 4979
		CartesianSrId int = 7203
		Cartesian3DSrId int = 9157
	)

	var (
		err     error
		driver  Driver
		session *Session
		result  *Result
	)

	rand.Seed(time.Now().UnixNano())

	BeforeEach(func() {
		driver, err = NewDriver(singleInstanceUri, BasicAuth(username, password, ""))
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())

		if VersionOfDriver(driver).LessThan(V3_4_0) {
			Skip("spatial types are only available after neo4j 3.4.0 release")
		}

		session, err = driver.Session(AccessModeRead)
		Expect(err).To(BeNil())
		Expect(session).NotTo(BeNil())
	})

	AfterEach(func() {
		if session != nil {
			session.Close()
		}

		if driver != nil {
			driver.Close()
		}
	})

	testSendAndReceive := func(point *Point) {
		result, err = session.Run("CREATE (n:Node { point: $point }) RETURN n.point", &map[string]interface{}{
			"point": point,
		})
		Expect(err).To(BeNil())

		if result.Next() {
			var pointReceived = result.Record().GetByIndex(0).(*Point)

			Expect(pointReceived).NotTo(BeNil())
			Expect(pointReceived.SrId()).To(Equal(point.SrId()))
			Expect(pointReceived.X()).To(Equal(point.X()))
			Expect(pointReceived.Y()).To(Equal(point.Y()))
			if math.IsNaN(point.Z()) {
				Expect(pointReceived.Z()).To(BeNaN())
			} else {
				Expect(pointReceived.Z()).To(Equal(point.Z()))
			}
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())
	}

	testSendAndReceiveList := func(points []*Point) {
		result, err = session.Run("CREATE (n:Node { points: $points }) RETURN n.points", &map[string]interface{}{
			"points": points,
		})
		Expect(err).To(BeNil())

		if result.Next() {
			var pointsList = result.Record().GetByIndex(0).([]interface{})

			Expect(pointsList).To(HaveLen(len(points)))
			for index, point := range pointsList {
				pointSent := points[index]
				pointReceived := point.(*Point)

				Expect(pointReceived).NotTo(BeNil())
				Expect(pointReceived.SrId()).To(Equal(pointSent.SrId()))
				Expect(pointReceived.X()).To(Equal(pointSent.X()))
				Expect(pointReceived.Y()).To(Equal(pointSent.Y()))
				if math.IsNaN(pointSent.Z()) {
					Expect(pointReceived.Z()).To(BeNaN())
				} else {
					Expect(pointReceived.Z()).To(Equal(pointSent.Z()))
				}
			}
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())
	}

	randomPoint := func(sequence int) *Point {
		randomDouble := func() float64  {
			return float64(rand.Intn(360) - 180) + rand.Float64()
		}

		switch sequence % 4 {
		case 0:
			return NewPoint(WGS84SrId, randomDouble(), randomDouble())
		case 1:
			return NewPoint3D(WGS843DSrId, randomDouble(), randomDouble(), randomDouble())
		case 2:
			return NewPoint(CartesianSrId, randomDouble(), randomDouble())
		case 3:
			return NewPoint3D(Cartesian3DSrId, randomDouble(), randomDouble(), randomDouble())
		default:
			panic("not expected")
		}
	}

	randomPointList := func(sequence int, count int) []*Point {
		result := make([]*Point, count)
		for i := 0; i < count; i++ {
			result[i] = randomPoint(sequence)
		}
		return result
	}

	It("should be able to receive points", func() {
		result, err = session.Run("RETURN point({x: 39.111748, y:-76.775635}), point({x: 39.111748, y:-76.775635, z:35.120})", nil)
		Expect(err).To(BeNil())

		if result.Next() {
			var point1 = result.Record().GetByIndex(0).(*Point)
			var point2 = result.Record().GetByIndex(1).(*Point)

			Expect(point1).NotTo(BeNil())
			Expect(point1.SrId()).To(Equal(CartesianSrId))
			Expect(point1.X()).To(Equal(39.111748))
			Expect(point1.Y()).To(Equal(-76.775635))
			Expect(point1.Z()).To(BeNaN())

			Expect(point2).NotTo(BeNil())
			Expect(point2.SrId()).To(Equal(Cartesian3DSrId))
			Expect(point2.X()).To(Equal(39.111748))
			Expect(point2.Y()).To(Equal(-76.775635))
			Expect(point2.Z()).To(Equal(35.120))
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())
	})

	It("should be able to send points", func() {
		point1 := NewPoint(WGS84SrId, 51.5044585, -0.105658)
		point2 := NewPoint3D(WGS843DSrId, 51.5044585, -0.105658, 35.120)

		result, err = session.Run("CREATE (n:POI { location1: $point1, location2: $point2 }) RETURN n", &map[string]interface{}{
			"point1": point1,
			"point2": point2,
		})
		Expect(err).To(BeNil())

		if result.Next() {
			var node = result.Record().GetByIndex(0).(Node)
			var point1Received = node.Props()["location1"].(*Point)
			var point2Received = node.Props()["location2"].(*Point)

			Expect(point1Received).NotTo(BeNil())
			Expect(point1Received.SrId()).To(Equal(point1.SrId()))
			Expect(point1Received.X()).To(Equal(point1.X()))
			Expect(point1Received.Y()).To(Equal(point1.Y()))
			Expect(point1Received.Z()).To(BeNaN())

			Expect(point2Received).NotTo(BeNil())
			Expect(point2Received.SrId()).To(Equal(point2.SrId()))
			Expect(point2Received.X()).To(Equal(point2.X()))
			Expect(point2Received.Y()).To(Equal(point2.Y()))
			Expect(point2Received.Z()).To(Equal(point2.Z()))
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())
	})

	It("should be able to send points - pass by value", func() {
		point1 := NewPoint(WGS84SrId, 51.5044585, -0.105658)
		point2 := NewPoint3D(WGS843DSrId, 51.5044585, -0.105658, 35.120)

		result, err = session.Run("CREATE (n:POI { location1: $point1, location2: $point2 }) RETURN n", &map[string]interface{}{
			"point1": *point1,
			"point2": *point2,
		})
		Expect(err).To(BeNil())

		if result.Next() {
			var node = result.Record().GetByIndex(0).(Node)
			var point1Received = node.Props()["location1"].(*Point)
			var point2Received = node.Props()["location2"].(*Point)

			Expect(point1Received).NotTo(BeNil())
			Expect(point1Received.SrId()).To(Equal(point1.SrId()))
			Expect(point1Received.X()).To(Equal(point1.X()))
			Expect(point1Received.Y()).To(Equal(point1.Y()))
			Expect(point1Received.Z()).To(BeNaN())

			Expect(point2Received).NotTo(BeNil())
			Expect(point2Received.SrId()).To(Equal(point2.SrId()))
			Expect(point2Received.X()).To(Equal(point2.X()))
			Expect(point2Received.Y()).To(Equal(point2.Y()))
			Expect(point2Received.Z()).To(Equal(point2.Z()))
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())
	})

	It("should send and receive point", func() {
		testSendAndReceive(NewPoint(WGS84SrId, 51.24923585, 0.92723724))
		testSendAndReceive(NewPoint3D(WGS843DSrId, 22.86211019, 171.61820439, 0.1230987))
		testSendAndReceive(NewPoint(CartesianSrId, 39.111748, -76.775635))
		testSendAndReceive(NewPoint3D(Cartesian3DSrId, 39.111748, -76.775635, 19.2937302840))
	})

	It("should send and receive points - randomised", func() {
		for i := 0; i < 1000; i++ {
			testSendAndReceive(randomPoint(i))
		}
	})

	It("should send and receive point list - randomised", func() {
		for i := 0; i < 1000; i++ {
			testSendAndReceiveList(randomPointList(i, 100))
		}
	})
})