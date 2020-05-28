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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func TestHydrator(ot *testing.T) {
	cases := []struct {
		name       string
		tag        packstream.StructTag
		fields     []interface{}
		hydrateErr error
		hydrated   interface{}
	}{
		{
			name:       "Illegal tag",
			tag:        0x00,
			hydrateErr: errors.New("Something"),
		},
		{
			name: "Node",
			tag:  'N',
			fields: []interface{}{
				int64(112), []interface{}{"lbl1", "lbl2"}, map[string]interface{}{"prop1": 2}},
			hydrated: &types.Node{Id: 112, Labels: []string{"lbl1", "lbl2"}, Props: map[string]interface{}{"prop1": 2}},
		},
		{
			name:       "Node, no fields",
			tag:        'N',
			fields:     []interface{}{},
			hydrateErr: errors.New("something"),
		},
		{
			name: "Relationship",
			tag:  'R',
			fields: []interface{}{
				int64(3), int64(1), int64(2), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &types.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
		},
		{
			name:       "Relationship, no fields",
			tag:        'R',
			fields:     []interface{}{},
			hydrateErr: errors.New("something"),
		},
		{
			name: "Path",
			tag:  'P',
			fields: []interface{}{
				[]interface{}{&types.Node{}, &types.Node{}},
				[]interface{}{&types.RelNode{}},
				[]interface{}{int64(1), int64(1)},
			},
			hydrated: &types.Path{
				Nodes:    []*types.Node{&types.Node{}, &types.Node{}},
				RelNodes: []*types.RelNode{&types.RelNode{}},
				Indexes:  []int{1, 1},
			},
		},
		{
			name: "RelNode",
			tag:  'r',
			fields: []interface{}{
				int64(3), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &types.RelNode{Id: 3, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
		},
		{
			name:   "Point2D",
			tag:    'X',
			fields: []interface{}{int64(1), float64(2.0), float64(3.0)},
			hydrated: &types.Point2D{
				SpatialRefId: 1,
				X:            2.0,
				Y:            3.0,
			},
		},
		{
			name:   "Point3D",
			tag:    'Y',
			fields: []interface{}{int64(1), float64(2.0), float64(3.0), float64(4.0)},
			hydrated: &types.Point3D{
				SpatialRefId: 1,
				X:            2.0,
				Y:            3.0,
				Z:            4.0,
			},
		},
		{
			name:     "Success response",
			tag:      msgSuccess,
			fields:   []interface{}{map[string]interface{}{"x": 1}},
			hydrated: &successResponse{meta: map[string]interface{}{"x": 1}},
		},
		{
			name:     "Ignored response",
			tag:      msgIgnored,
			fields:   []interface{}{},
			hydrated: &ignoredResponse{},
		},
		{
			name:     "Failure response",
			tag:      msgFailure,
			fields:   []interface{}{map[string]interface{}{"code": "Code", "message": "Msg"}},
			hydrated: &db.DatabaseError{Code: "Code", Msg: "Msg"},
		},
		{
			name:     "Record response",
			tag:      msgRecord,
			fields:   []interface{}{[]interface{}{1, "a"}},
			hydrated: &recordResponse{values: []interface{}{1, "a"}},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			hydrated, err := hydrate(c.tag, c.fields)
			if (err != nil) != (c.hydrateErr != nil) {
				t.Fatalf("Expected error '%+v' but was '%+v'", c.hydrateErr, err)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(hydrated, c.hydrated) {
				t.Fatalf("Hydrated differs, expected: %+v but was: %+v", c.hydrated, hydrated)
			}
		})
	}
}
func TestDehydrator(ot *testing.T) {
	cases := []struct {
		name string
		x    interface{}
		s    *packstream.Struct
	}{
		{
			name: "Point2D",
			x:    &types.Point2D{SpatialRefId: 7, X: 8.0, Y: 9.0},
			s:    &packstream.Struct{Tag: 'X', Fields: []interface{}{uint32(7), 8.0, 9.0}},
		},
		{
			name: "Point3D",
			x:    &types.Point3D{SpatialRefId: 7, X: 8.0, Y: 9.0, Z: 10},
			s:    &packstream.Struct{Tag: 'Y', Fields: []interface{}{uint32(7), 8.0, 9.0, 10.0}},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			s, err := dehydrate(c.x)
			assertNoError(t, err)
			// Compare Structs
			if s.Tag != c.s.Tag {
				t.Errorf("Wrong tags, expected %d but was %d", c.s.Tag, s.Tag)
			}
			if len(c.s.Fields) != len(s.Fields) {
				t.Errorf("Wrong number of fields, expected %d but was %d", len(c.s.Fields), len(s.Fields))
			}
			for i, v := range c.s.Fields {
				if s.Fields[i] != v {
					t.Errorf("Field %d differs, expected %+v but was %+v", i, v, s.Fields[i])
				}
			}
		})
	}
}

func TestDehydrateHydrate(ot *testing.T) {
	dehydrateAndHydrate := func(t *testing.T, xi interface{}) interface{} {
		t.Helper()
		// Run through packstream to be certain of type assumptions
		buf := bytes.NewBuffer([]byte{})
		packer := packstream.NewPacker(buf, dehydrate)
		err := packer.Pack(xi)
		assertNoError(t, err)
		unpacker := packstream.NewUnpacker(buf)
		xo, err := unpacker.UnpackStruct(hydrate)
		assertNoError(t, err)
		return xo
	}

	ot.Run("time.Time", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, ni).(time.Time)
		assertDateTimeSame(t, ni, no)
		assertTimeLocationSame(t, ni, no)
	})

	ot.Run("time.Time offset", func(t *testing.T) {
		ni := time.Now()
		l := time.FixedZone("Offset", 60*60)
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, ni).(time.Time)
		assertDateTimeSame(t, ni, no)
		assertTimeLocationSame(t, ni, no)
	})

	ot.Run("LocalDateTime", func(t *testing.T) {
		ni := time.Now().Round(0 * time.Nanosecond)
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l).Round(0 * time.Nanosecond)
		no := dehydrateAndHydrate(t, types.LocalDateTime(ni)).(types.LocalDateTime)
		assertTimeSame(t, ni, time.Time(no))
		assertDateSame(t, ni, time.Time(no))
		// Received time should be in Local time even if sent as something else
		if time.Time(no).Location().String() != "Local" {
			t.Errorf("Should be local")
		}
	})

	ot.Run("LocalDateTime way back", func(t *testing.T) {
		l, _ := time.LoadLocation("Asia/Anadyr")
		ni := time.Date(311, 7, 2, 23, 59, 3, 1, l)
		no := dehydrateAndHydrate(t, types.LocalDateTime(ni)).(types.LocalDateTime)
		assertTimeSame(t, ni, time.Time(no))
		assertDateSame(t, ni, time.Time(no))
		// Received time should be in Local time even if sent as something else
		if time.Time(no).Location().String() != "Local" {
			t.Errorf("Should be local")
		}
	})

	ot.Run("Date", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.Date(ni)).(types.Date)
		assertDateSame(t, ni, time.Time(no))
	})

	ot.Run("Time", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.Time(ni)).(types.Time)
		assertZoneOffsetSame(t, ni, time.Time(no))
		assertTimeSame(t, ni, time.Time(no))
	})

	ot.Run("LocalTime", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.LocalTime(ni)).(types.LocalTime)
		assertTimeSame(t, ni, time.Time(no))
	})

	ot.Run("Duration", func(t *testing.T) {
		di := types.Duration{Months: 3, Days: 3, Seconds: 9000, Nanos: 13}
		do := dehydrateAndHydrate(t, di).(types.Duration)
		assertDurationSame(t, di, do)
	})
}
