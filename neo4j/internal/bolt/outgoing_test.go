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
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

// Utility to dehydrate/unpack
func unpack(u *packstream.Unpacker) interface{} {
	u.Next()
	switch u.Curr {
	case packstream.PackedInt:
		return u.Int()
	case packstream.PackedFloat:
		return u.Float()
	case packstream.PackedStr:
		return u.String()
	case packstream.PackedByteArray:
		return u.ByteArray()
	case packstream.PackedNil:
		return nil
	case packstream.PackedTrue:
		return true
	case packstream.PackedFalse:
		return false
	case packstream.PackedArray:
		l := u.Len()
		a := make([]interface{}, l)
		for i := range a {
			a[i] = unpack(u)
		}
		return a
	case packstream.PackedMap:
		l := u.Len()
		m := make(map[string]interface{}, l)
		for i := uint32(0); i < l; i++ {
			u.Next()
			k := u.String()
			m[k] = unpack(u)
		}
		return m
	case packstream.PackedStruct:
		t := u.StructTag()
		l := u.Len()
		s := testStruct{tag: t}
		if l == 0 {
			return &s
		}
		s.fields = make([]interface{}, l)
		for i := range s.fields {
			s.fields[i] = unpack(u)
		}
		return &s
	default:
		panic(".")
	}
}

func TestOutgoing(ot *testing.T) {
	var err error
	out := &outgoing{
		chunker: newChunker(),
		packer:  &packstream.Packer{},
		onErr:   func(e error) { err = e },
	}
	// Utility to unpack through dechunking and a custom build func
	dechunkAndUnpack := func(t *testing.T, build func()) interface{} {
		buf := &bytes.Buffer{}
		err = nil
		build()
		if err != nil {
			t.Fatal(err)
		}
		out.send(buf)

		// Dechunk it
		byts, err := dechunkMessage(buf, []byte{})
		if err != nil {
			t.Fatal(err)
		}
		// Hydrate it
		unpacker := &packstream.Unpacker{}
		unpacker.Reset(byts)
		x := unpack(unpacker)
		if unpacker.Err != nil {
			t.Fatal(err)
		}
		return x
	}

	// tests for top level appending and sending outgoing messages
	cases := []struct {
		name   string
		build  func()
		expect interface{}
	}{
		{
			name: "hello",
			build: func() {
				out.appendHello(nil)
			},
			expect: &testStruct{
				tag:    byte(msgHello),
				fields: []interface{}{map[string]interface{}{}},
			},
		},
		{
			name: "begin",
			build: func() {
				out.appendBegin(map[string]interface{}{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgBegin),
				fields: []interface{}{map[string]interface{}{"mode": "r"}},
			},
		},
		{
			name: "commit",
			build: func() {
				out.appendCommit()
			},
			expect: &testStruct{
				tag: byte(msgCommit),
			},
		},
		{
			name: "rollback",
			build: func() {
				out.appendRollback()
			},
			expect: &testStruct{
				tag: byte(msgRollback),
			},
		},
		{
			name: "goodbye",
			build: func() {
				out.appendGoodbye()
			},
			expect: &testStruct{
				tag: byte(msgGoodbye),
			},
		},
		{
			name: "reset",
			build: func() {
				out.appendReset()
			},
			expect: &testStruct{
				tag: byte(msgReset),
			},
		},
		{
			name: "pull all",
			build: func() {
				out.appendPullAll()
			},
			expect: &testStruct{
				tag: byte(msgPullAll),
			},
		},
		{
			name: "pull n",
			build: func() {
				out.appendPullN(7)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []interface{}{map[string]interface{}{"n": int64(7)}},
			},
		},
		{
			name: "pull n+qid",
			build: func() {
				out.appendPullNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []interface{}{map[string]interface{}{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "discard n+qid",
			build: func() {
				out.appendDiscardNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgDiscardN),
				fields: []interface{}{map[string]interface{}{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "run, no params, no meta",
			build: func() {
				out.appendRun("cypher", nil, nil)
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []interface{}{"cypher", map[string]interface{}{}, map[string]interface{}{}},
			},
		},
		{
			name: "run, no params, meta",
			build: func() {
				out.appendRun("cypher", nil, map[string]interface{}{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []interface{}{"cypher", map[string]interface{}{}, map[string]interface{}{"mode": "r"}},
			},
		},
		{
			name: "run, params, meta",
			build: func() {
				out.appendRun("cypher", map[string]interface{}{"x": 1, "y": "2"}, map[string]interface{}{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []interface{}{"cypher", map[string]interface{}{"x": int64(1), "y": "2"}, map[string]interface{}{"mode": "r"}},
			},
		},
		{
			name: "run, params, meta",
			build: func() {
				out.appendRun("cypher", map[string]interface{}{"x": 1, "y": "2"}, map[string]interface{}{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []interface{}{"cypher", map[string]interface{}{"x": int64(1), "y": "2"}, map[string]interface{}{"mode": "r"}},
			},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			x := dechunkAndUnpack(t, c.build)
			if !reflect.DeepEqual(x, c.expect) {
				t.Errorf("Unpacked differs, expected %+v (%T) but was %+v (%T)", c.expect, c.expect, x, x)
			}
		})
	}

	offsetZone := time.FixedZone("Offset", 100)

	type (
		customBool        bool
		customFloat       float64
		customInt         int64
		customString      string
		customByteSlice   []byte
		customStringSlice []string
		customMapOfInts   map[string]int
	)
	// Test packing of maps in more detail, essentially tests allowed parameters to Run command
	// tests for top level appending and sending outgoing messages
	paramCases := []struct {
		name   string
		inp    map[string]interface{}
		expect map[string]interface{}
	}{
		{
			name: "map of maps",
			inp: map[string]interface{}{
				"map":    map[string]interface{}{"k1": "v1"},
				"int":    map[string]int{"k2": 1},
				"[]int":  map[string][]int{"k3": {1, 2, 3}},
				"[]bool": map[string]bool{"t": true, "f": false},
			},
			expect: map[string]interface{}{
				"map":    map[string]interface{}{"k1": "v1"},
				"int":    map[string]interface{}{"k2": int64(1)},
				"[]int":  map[string]interface{}{"k3": []interface{}{int64(1), int64(2), int64(3)}},
				"[]bool": map[string]interface{}{"t": true, "f": false},
			},
		},
		{
			name: "map of spatial",
			inp: map[string]interface{}{
				"p1": dbtype.Point2D{SpatialRefId: 1, X: 2, Y: 3},
				"p2": dbtype.Point3D{SpatialRefId: 4, X: 5, Y: 6, Z: 7},
				"ps": []dbtype.Point3D{{SpatialRefId: 4, X: 5, Y: 6, Z: 7}},
			},
			expect: map[string]interface{}{
				"p1": &testStruct{tag: 'X', fields: []interface{}{int64(1), float64(2), float64(3)}},
				"p2": &testStruct{tag: 'Y', fields: []interface{}{int64(4), float64(5), float64(6), float64(7)}},
				"ps": []interface{}{&testStruct{tag: 'Y', fields: []interface{}{int64(4), float64(5), float64(6), float64(7)}}},
			},
		},
		{
			name: "map of spatial pointers",
			inp: map[string]interface{}{
				"p1": &dbtype.Point2D{SpatialRefId: 1, X: 2, Y: 3},
				"p2": &dbtype.Point3D{SpatialRefId: 4, X: 5, Y: 6, Z: 7},
			},
			expect: map[string]interface{}{
				"p1": &testStruct{tag: 'X', fields: []interface{}{int64(1), float64(2), float64(3)}},
				"p2": &testStruct{tag: 'Y', fields: []interface{}{int64(4), float64(5), float64(6), float64(7)}},
			},
		},
		{
			name: "map of temporals",
			inp: map[string]interface{}{
				"time.Time UTC":    time.Unix(1, 2).UTC(),
				"time.Time offset": time.Unix(1, 2).In(offsetZone),
				"LocalDateTime":    dbtype.LocalDateTime(time.Unix(1, 2).UTC()),
				"Date":             dbtype.Date(time.Date(1993, 11, 31, 7, 59, 1, 100, time.UTC)),
				"Time":             dbtype.Time(time.Unix(1, 2).In(offsetZone)),
				"LocalTime":        dbtype.LocalTime(time.Unix(1, 2).UTC()),
				"Duration":         dbtype.Duration{Months: 1, Days: 2, Seconds: 3, Nanos: 4},
			},
			expect: map[string]interface{}{
				"time.Time UTC":    &testStruct{tag: 'f', fields: []interface{}{int64(1), int64(2), "UTC"}},
				"time.Time offset": &testStruct{tag: 'F', fields: []interface{}{int64(101), int64(2), int64(100)}},
				"LocalDateTime":    &testStruct{tag: 'd', fields: []interface{}{int64(1), int64(2)}},
				"Date":             &testStruct{tag: 'D', fields: []interface{}{int64(8735)}},
				"Time":             &testStruct{tag: 'T', fields: []interface{}{int64(101*time.Second + 2), int64(100)}},
				"LocalTime":        &testStruct{tag: 't', fields: []interface{}{int64(1*time.Second + 2)}},
				"Duration":         &testStruct{tag: 'E', fields: []interface{}{int64(1), int64(2), int64(3), int64(4)}},
			},
		},
		{
			name: "map of custom native types",
			inp: map[string]interface{}{
				"custom bool":         customBool(true),
				"custom float":        customFloat(3.14),
				"custom int":          customInt(12345),
				"custom string":       customString("Hello"),
				"custom byte slice":   customByteSlice([]byte{1, 2, 3}),
				"custom string slice": customStringSlice([]string{"hello", "again"}),
				"custom map of ints":  customMapOfInts(map[string]int{"l": 1}),
			},
			expect: map[string]interface{}{
				"custom bool":   true,
				"custom float":  3.14,
				"custom int":    int64(12345),
				"custom string": "Hello",
				// Custom will cause []byte to come back as []interface{}, could be handled but maybe not worth it
				"custom byte slice":   []interface{}{int64(1), int64(2), int64(3)},
				"custom string slice": []interface{}{"hello", "again"},
				"custom map of ints":  map[string]interface{}{"l": int64(1)},
			},
		},
		{
			name: "map of pointer types",
			inp: map[string]interface{}{
				"*[]int":             &([]int{3}),
				"*map[string]string": &(map[string]string{"x": "y"}),
			},
			expect: map[string]interface{}{
				"*[]int":             []interface{}{int64(3)},
				"*map[string]string": map[string]interface{}{"x": "y"},
			},
		},
	}

	for _, c := range paramCases {
		ot.Run(c.name, func(t *testing.T) {
			x := dechunkAndUnpack(t, func() {
				out.begin()
				out.packMap(c.inp)
				out.end()
			})
			if !reflect.DeepEqual(x, c.expect) {
				t.Errorf("Unpacked differs, expected\n %#v but was\n %#v", c.expect, x)
			}
		})
	}

	type aStruct struct{}

	// Test packing of stuff that is expected to give an error
	paramErrorCases := []struct {
		name string
		inp  map[string]interface{}
		err  error
	}{
		{
			name: "map with non string key",
			inp: map[string]interface{}{
				"m": map[int]string{1: "y"},
			},
			err: &db.UnsupportedTypeError{},
		},
		{
			name: "a random struct",
			inp: map[string]interface{}{
				"m": aStruct{},
			},
			err: &db.UnsupportedTypeError{},
		},
		{
			name: "a random *struct",
			inp: map[string]interface{}{
				"m": &aStruct{},
			},
			err: &db.UnsupportedTypeError{},
		},
	}
	for _, c := range paramErrorCases {
		var err error
		out := &outgoing{
			chunker: newChunker(),
			packer:  &packstream.Packer{},
			onErr:   func(e error) { err = e },
		}
		ot.Run(c.name, func(t *testing.T) {
			out.begin()
			out.packMap(c.inp)
			out.end()
			if reflect.TypeOf(err) != reflect.TypeOf(c.err) {
				t.Error(err)
			}
		})
	}
}
