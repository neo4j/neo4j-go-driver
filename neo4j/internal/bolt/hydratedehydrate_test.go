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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
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
			hydrated: dbtype.Node{Id: 112, Labels: []string{"lbl1", "lbl2"}, Props: map[string]interface{}{"prop1": 2}},
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
			hydrated: dbtype.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
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
				[]interface{}{dbtype.Node{Id: 1}, dbtype.Node{Id: 2}},
				[]interface{}{&relNode{id: 3, name: "x"}},
				[]interface{}{int64(1), int64(1)},
			},
			hydrated: dbtype.Path{
				Nodes:         []dbtype.Node{dbtype.Node{Id: 1}, dbtype.Node{Id: 2}},
				Relationships: []dbtype.Relationship{dbtype.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "x"}},
			},
		},
		{
			name: "relNode",
			tag:  'r',
			fields: []interface{}{
				int64(3), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &relNode{id: 3, name: "lbl1", props: map[string]interface{}{"propx": 3}},
		},
		{
			name:   "Point2D",
			tag:    'X',
			fields: []interface{}{int64(1), float64(2.0), float64(3.0)},
			hydrated: dbtype.Point2D{
				SpatialRefId: 1,
				X:            2.0,
				Y:            3.0,
			},
		},
		{
			name:   "Point3D",
			tag:    'Y',
			fields: []interface{}{int64(1), float64(2.0), float64(3.0), float64(4.0)},
			hydrated: dbtype.Point3D{
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
			hydrated: &db.Neo4jError{Code: "Code", Msg: "Msg"},
		},
		{
			name:     "Record response",
			tag:      msgRecord,
			fields:   []interface{}{[]interface{}{1, "a"}},
			hydrated: &db.Record{Values: []interface{}{1, "a"}},
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
			x:    &dbtype.Point2D{SpatialRefId: 7, X: 8.0, Y: 9.0},
			s:    &packstream.Struct{Tag: 'X', Fields: []interface{}{uint32(7), 8.0, 9.0}},
		},
		{
			name: "Point3D",
			x:    &dbtype.Point3D{SpatialRefId: 7, X: 8.0, Y: 9.0, Z: 10},
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
		buf := []byte{}
		packer := packstream.Packer{}
		var err error
		buf, err = packer.Pack(buf, dehydrate, xi)
		assertNoError(t, err)
		unpacker := &packstream.Unpacker{}
		xo, err := unpacker.UnpackStruct(buf, hydrate)
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
		no := dehydrateAndHydrate(t, dbtype.LocalDateTime(ni)).(dbtype.LocalDateTime)
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
		no := dehydrateAndHydrate(t, dbtype.LocalDateTime(ni)).(dbtype.LocalDateTime)
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
		no := dehydrateAndHydrate(t, dbtype.Date(ni)).(dbtype.Date)
		assertDateSame(t, ni, time.Time(no))
	})

	ot.Run("Time", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, dbtype.Time(ni)).(dbtype.Time)
		assertZoneOffsetSame(t, ni, time.Time(no))
		assertTimeSame(t, ni, time.Time(no))
	})

	ot.Run("LocalTime", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, dbtype.LocalTime(ni)).(dbtype.LocalTime)
		assertTimeSame(t, ni, time.Time(no))
	})

	ot.Run("Duration", func(t *testing.T) {
		di := dbtype.Duration{Months: 3, Days: 3, Seconds: 9000, Nanos: 13}
		do := dehydrateAndHydrate(t, di).(dbtype.Duration)
		assertDurationSame(t, di, do)
	})
}
