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

package bolt

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

// Utility to dehydrate/unpack
func unpack(u *packstream.Unpacker) any {
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
		a := make([]any, l)
		for i := range a {
			a[i] = unpack(u)
		}
		return a
	case packstream.PackedMap:
		l := u.Len()
		m := make(map[string]any, l)
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
		s.fields = make([]any, l)
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
	// Utility to unpack through dechunking and a custom build func
	dechunkAndUnpack := func(t *testing.T, build func(*testing.T, *outgoing)) any {
		out := &outgoing{
			chunker:   newChunker(),
			packer:    packstream.Packer{},
			onPackErr: func(e error) { err = e },
			onIoErr: func(_ context.Context, err error) {
				ot.Fatalf("Should be no io errors in this test: %s", err)
			},
		}
		serv, cli := net.Pipe()
		defer func() {
			if err := cli.Close(); err != nil {
				ot.Errorf("failed to close client connection %v", err)
			}
			if err := serv.Close(); err != nil {
				ot.Errorf("failed to close server connection %v", err)
			}
		}()
		err = nil
		build(t, out)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			out.send(context.Background(), cli)
		}()

		// Dechunk it
		_, byts, err := dechunkMessage(context.Background(), serv, []byte{}, -1)
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
		build  func(t *testing.T, outgoing *outgoing)
		expect any
	}{
		{
			name: "hello",
			build: func(t *testing.T, out *outgoing) {
				out.appendHello(nil)
			},
			expect: &testStruct{
				tag:    byte(msgHello),
				fields: []any{map[string]any{}},
			},
		},
		{
			name: "begin",
			build: func(t *testing.T, out *outgoing) {
				out.appendBegin(map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgBegin),
				fields: []any{map[string]any{"mode": "r"}},
			},
		},
		{
			name: "commit",
			build: func(t *testing.T, out *outgoing) {
				out.appendCommit()
			},
			expect: &testStruct{
				tag: byte(msgCommit),
			},
		},
		{
			name: "rollback",
			build: func(t *testing.T, out *outgoing) {
				out.appendRollback()
			},
			expect: &testStruct{
				tag: byte(msgRollback),
			},
		},
		{
			name: "goodbye",
			build: func(t *testing.T, out *outgoing) {
				out.appendGoodbye()
			},
			expect: &testStruct{
				tag: byte(msgGoodbye),
			},
		},
		{
			name: "reset",
			build: func(t *testing.T, out *outgoing) {
				out.appendReset()
			},
			expect: &testStruct{
				tag: byte(msgReset),
			},
		},
		{
			name: "pull all",
			build: func(t *testing.T, out *outgoing) {
				out.appendPullAll()
			},
			expect: &testStruct{
				tag: byte(msgPullAll),
			},
		},
		{
			name: "pull n",
			build: func(t *testing.T, out *outgoing) {
				out.appendPullN(7)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []any{map[string]any{"n": int64(7)}},
			},
		},
		{
			name: "pull n+qid",
			build: func(t *testing.T, out *outgoing) {
				out.appendPullNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []any{map[string]any{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "discard n+qid",
			build: func(t *testing.T, out *outgoing) {
				out.appendDiscardNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgDiscardN),
				fields: []any{map[string]any{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "run, no params, no meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", nil, nil)
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{}, map[string]any{}},
			},
		},
		{
			name: "run, no params, meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", nil, map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{}, map[string]any{"mode": "r"}},
			},
		},
		{
			name: "run, params, meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", map[string]any{"x": 1, "y": "2"}, map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{"x": int64(1), "y": "2"}, map[string]any{"mode": "r"}},
			},
		},
		{
			name: "run, params, meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", map[string]any{"x": 1, "y": "2"}, map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{"x": int64(1), "y": "2"}, map[string]any{"mode": "r"}},
			},
		},
		{
			name: "route v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1", "key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, "adb")
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{"deutsch-mark", "mark-twain"}, "adb"},
			},
		},
		{
			name: "route, default database v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1",
					"key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, idb.DefaultDatabase)
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{"deutsch-mark", "mark-twain"}, nil},
			},
		},
		{
			name: "route, default bookmarks v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1", "key2": "val2"}, nil, "adb")
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{}, "adb"},
			},
		},
		{
			name: "route, default bookmarks and database v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1",
					"key2": "val2"}, nil, idb.DefaultDatabase)
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{}, nil},
			},
		},
		{
			name: "route",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, map[string]any{"db": "adb"})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{"deutsch-mark", "mark-twain"},
					map[string]any{"db": "adb"}},
			},
		},
		{
			name: "route, default database",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, map[string]any{})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{"deutsch-mark", "mark-twain"},
					map[string]any{}},
			},
		},
		{
			name: "route, default bookmarks",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, nil, map[string]any{"db": "adb"})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{},
					map[string]any{"db": "adb"}},
			},
		},
		{
			name: "route, default bookmarks and database",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, nil, map[string]any{})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{},
					map[string]any{}},
			},
		},
		{
			name: "UTC datetime struct, with timezone offset",
			build: func(t *testing.T, out *outgoing) {
				defer func() {
					out.useUtc = false
				}()
				out.useUtc = true
				minusTwoHours := -2 * 60 * 60
				tz := time.FixedZone("Offset", minusTwoHours)
				out.begin()
				// June 15, 2020 12:30:00 in "Offset"
				// aka June 15, 2020 14:30:00 UTC
				// aka 1592231400 seconds since Unix epoch
				out.packStruct(time.Date(2020, 6, 15, 12, 30, 0, 42, tz))
				out.end()
			},
			expect: &testStruct{
				tag: 'I',
				fields: []any{
					int64(1592231400),
					int64(42),
					int64(-2 * 60 * 60),
				},
			},
		},
		{
			name: "UTC datetime struct, with timezone name",
			build: func(t *testing.T, out *outgoing) {
				defer func() {
					out.useUtc = false
				}()
				out.useUtc = true
				tz, err := time.LoadLocation("Pacific/Honolulu")
				if err != nil {
					t.Fatal(err)
				}
				out.begin()
				// June 15, 2020 04:30:00 in "Pacific/Honolulu"
				// aka June 15, 2020 14:30:00 UTC
				// aka 1592231400 seconds since Unix epoch
				out.packStruct(time.Date(2020, 6, 15, 4, 30, 0, 42, tz))
				out.end()
			},
			expect: &testStruct{
				tag: 'i',
				fields: []any{
					int64(1592231400),
					int64(42),
					"Pacific/Honolulu",
				},
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
		customByte        byte
		customStringSlice []string
		customMapOfInts   map[string]int
	)

	someBool := true
	var someBoolPtr = &someBool
	var nilBool *bool = nil
	someInt := 1
	var someIntPtr = &someInt
	var nilInt *int = nil
	someInt8 := int8(2)
	var someInt8Ptr = &someInt8
	var nilInt8 *int8 = nil
	someInt16 := int16(3)
	var someInt16Ptr = &someInt16
	var nilInt16 *int16 = nil
	someInt32 := int32(4)
	var someInt32Ptr = &someInt32
	var nilInt32 *int32 = nil
	someInt64 := int64(5)
	var someInt64Ptr = &someInt64
	var nilInt64 *int64 = nil
	someUint := uint(6)
	var someUintPtr = &someUint
	var nilUint *uint = nil
	someUint8 := uint8(7)
	var someUint8Ptr = &someUint8
	var nilUint8 *uint8 = nil
	someByte := byte(8)
	var someBytePtr = &someByte
	var nilByte *byte = nil
	someUint16 := uint16(9)
	var someUint16Ptr = &someUint16
	var nilUint16 *uint16 = nil
	someUint32 := uint32(10)
	var someUint32Ptr = &someUint32
	var nilUint32 *uint32 = nil
	someUint64 := uint64(11)
	var someUint64Ptr = &someUint64
	var nilUint64 *uint64 = nil
	someFloat32 := float32(3.25)
	var someFloat32Ptr = &someFloat32
	var nilFloat32 *float32 = nil
	someFloat64 := 1.5
	var someFloat64Ptr = &someFloat64
	var nilFloat64 *float64 = nil
	someString := "hello"
	var someStringPtr = &someString
	var nilString *string = nil

	someCustomInt := customInt(someInt)

	// Test packing of maps in more detail, essentially tests allowed parameters to Run command
	// tests for top level appending and sending outgoing messages
	paramCases := []struct {
		name   string
		inp    map[string]any
		expect map[string]any
	}{
		{
			name: "simple types",
			inp: map[string]any{
				"nil":     nil,
				"bool":    true,
				"int":     1,
				"int8":    int8(2),
				"int16":   int16(3),
				"int32":   int32(4),
				"int64":   int64(5),
				"uint":    uint(6),
				"uint8":   uint8(7),
				"byte":    byte(8),
				"[]uint8": []uint8{9},
				"[]byte":  []byte{10},
				"uint16":  uint16(11),
				"uint32":  uint32(12),
				"uint64":  uint64(13),
				"float32": float32(3.25),
				"float64": 1.5,
				"string":  "hello",
			},
			expect: map[string]any{
				"nil":     nil,
				"bool":    true,
				"int":     int64(1),
				"int8":    int64(2),
				"int16":   int64(3),
				"int32":   int64(4),
				"int64":   int64(5),
				"uint":    int64(6),
				"uint8":   int64(7),
				"byte":    int64(8),
				"[]uint8": []byte{9},
				"[]byte":  []byte{10},
				"uint16":  int64(11),
				"uint32":  int64(12),
				"uint64":  int64(13),
				"float32": 3.25,
				"float64": 1.5,
				"string":  "hello",
			},
		},
		{
			name: "simple type slices",
			inp: map[string]any{
				"[]nil any":  []any{nil},
				"[]nil *int": []*int{nil},
				"[]bool":     []bool{true},
				"[]int":      []int{1},
				"[]int8":     []int8{2},
				"[]int16":    []int16{3},
				"[]int32":    []int32{4},
				"[]int64":    []int64{5},
				"[]uint":     []uint{6},
				"[]uint8":    []uint8{7},
				"[]byte":     []byte{8},
				"[][]uint8":  [][]uint8{{9}},
				"[][]byte":   [][]byte{{10}},
				"[]uint16":   []uint16{11},
				"[]uint32":   []uint32{12},
				"[]uint64":   []uint64{13},
				"[]float32":  []float32{3.25},
				"[]float64":  []float64{1.5},
				"[]float32 long": []float32{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				},
				"[]float64 long": []float64{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				},
				"[][]float32 long": [][]float32{{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				}},
				"[][]float64 long": [][]float64{{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				}},
				"[]string": []string{"hello"},
			},
			expect: map[string]any{
				"[]nil any":  []any{nil},
				"[]nil *int": []any{nil},
				"[]bool":     []any{true},
				"[]int":      []any{int64(1)},
				"[]int8":     []any{int64(2)},
				"[]int16":    []any{int64(3)},
				"[]int32":    []any{int64(4)},
				"[]int64":    []any{int64(5)},
				"[]uint":     []any{int64(6)},
				"[]uint8":    []byte{7},
				"[]byte":     []byte{8},
				"[][]uint8":  []any{[]byte{9}},
				"[][]byte":   []any{[]byte{10}},
				"[]uint16":   []any{int64(11)},
				"[]uint32":   []any{int64(12)},
				"[]uint64":   []any{int64(13)},
				"[]float32":  []any{3.25},
				"[]float64":  []any{1.5},
				"[]float32 long": []any{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				},
				"[]float64 long": []any{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				},
				"[][]float32 long": []any{[]any{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				}},
				"[][]float64 long": []any{[]any{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
					11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
				}},
				"[]string": []any{"hello"},
			},
		},
		{
			name: "empty simple type slices",
			inp: map[string]any{
				"[]any":     []any{},
				"[]bool":    []bool{},
				"[]int":     []int{},
				"[]int8":    []int8{},
				"[]int16":   []int16{},
				"[]int32":   []int32{},
				"[]int64":   []int64{},
				"[]uint":    []uint{},
				"[]uint8":   []uint8{},
				"[]byte":    []byte{},
				"[]uint16":  []uint16{},
				"[]uint32":  []uint32{},
				"[]uint64":  []uint64{},
				"[]float32": []float32{},
				"[]float64": []float64{},
				"[]string":  []string{},
			},
			expect: map[string]any{
				"[]any":     []any{},
				"[]bool":    []any{},
				"[]int":     []any{},
				"[]int8":    []any{},
				"[]int16":   []any{},
				"[]int32":   []any{},
				"[]int64":   []any{},
				"[]uint":    []any{},
				"[]uint8":   []byte{},
				"[]byte":    []byte{},
				"[]uint16":  []any{},
				"[]uint32":  []any{},
				"[]uint64":  []any{},
				"[]float32": []any{},
				"[]float64": []any{},
				"[]string":  []any{},
			},
		},
		{
			name: "simple type maps",
			inp: map[string]any{
				"map[string]nil any":  map[string]any{"key": nil},
				"map[string]nil *int": map[string]*int{"key": nil},
				"map[string]bool":     map[string]bool{"key": true},
				"map[string]int":      map[string]int{"key": 1},
				"map[string]int8":     map[string]int8{"key": 2},
				"map[string]int16":    map[string]int16{"key": 3},
				"map[string]int32":    map[string]int32{"key": 4},
				"map[string]int64":    map[string]int64{"key": 5},
				"map[string]uint":     map[string]uint{"key": 6},
				"map[string]uint8":    map[string]uint8{"key": 7},
				"map[string]byte":     map[string]byte{"key": 8},
				"map[string]uint16":   map[string]uint16{"key": 9},
				"map[string]uint32":   map[string]uint32{"key": 10},
				"map[string]uint64":   map[string]uint64{"key": 11},
				"map[string]float32":  map[string]float32{"key": 3.25},
				"map[string]float64":  map[string]float64{"key": 1.5},
				"map[string]string":   map[string]string{"key": "hello"},
			},
			expect: map[string]any{
				"map[string]nil any":  map[string]any{"key": nil},
				"map[string]nil *int": map[string]any{"key": nil},
				"map[string]bool":     map[string]any{"key": true},
				"map[string]int":      map[string]any{"key": int64(1)},
				"map[string]int8":     map[string]any{"key": int64(2)},
				"map[string]int16":    map[string]any{"key": int64(3)},
				"map[string]int32":    map[string]any{"key": int64(4)},
				"map[string]int64":    map[string]any{"key": int64(5)},
				"map[string]uint":     map[string]any{"key": int64(6)},
				"map[string]uint8":    map[string]any{"key": int64(7)},
				"map[string]byte":     map[string]any{"key": int64(8)},
				"map[string]uint16":   map[string]any{"key": int64(9)},
				"map[string]uint32":   map[string]any{"key": int64(10)},
				"map[string]uint64":   map[string]any{"key": int64(11)},
				"map[string]float32":  map[string]any{"key": 3.25},
				"map[string]float64":  map[string]any{"key": 1.5},
				"map[string]string":   map[string]any{"key": "hello"},
			},
		},
		{
			name: "empty simple type maps",
			inp: map[string]any{
				"map[string]any":     map[string]any{},
				"map[string]*int":    map[string]*int{},
				"map[string]bool":    map[string]bool{},
				"map[string]int":     map[string]int{},
				"map[string]int8":    map[string]int8{},
				"map[string]int16":   map[string]int16{},
				"map[string]int32":   map[string]int32{},
				"map[string]int64":   map[string]int64{},
				"map[string]uint":    map[string]uint{},
				"map[string]uint8":   map[string]uint8{},
				"map[string]byte":    map[string]byte{},
				"map[string]uint16":  map[string]uint16{},
				"map[string]uint32":  map[string]uint32{},
				"map[string]uint64":  map[string]uint64{},
				"map[string]float32": map[string]float32{},
				"map[string]float64": map[string]float64{},
				"map[string]string":  map[string]string{},
			},
			expect: map[string]any{
				"map[string]any":     map[string]any{},
				"map[string]*int":    map[string]any{},
				"map[string]bool":    map[string]any{},
				"map[string]int":     map[string]any{},
				"map[string]int8":    map[string]any{},
				"map[string]int16":   map[string]any{},
				"map[string]int32":   map[string]any{},
				"map[string]int64":   map[string]any{},
				"map[string]uint":    map[string]any{},
				"map[string]uint8":   map[string]any{},
				"map[string]byte":    map[string]any{},
				"map[string]uint16":  map[string]any{},
				"map[string]uint32":  map[string]any{},
				"map[string]uint64":  map[string]any{},
				"map[string]float32": map[string]any{},
				"map[string]float64": map[string]any{},
				"map[string]string":  map[string]any{},
			},
		},
		{
			name: "simple type slice in maps",
			inp: map[string]any{
				"map[string][]nil any":  map[string][]any{"key": {nil}},
				"map[string][]nil *int": map[string][]*int{"key": {nil}},
				"map[string][]bool":     map[string][]bool{"key": {true}},
				"map[string][]int":      map[string][]int{"key": {1}},
				"map[string][]int8":     map[string][]int8{"key": {2}},
				"map[string][]int16":    map[string][]int16{"key": {3}},
				"map[string][]int32":    map[string][]int32{"key": {4}},
				"map[string][]int64":    map[string][]int64{"key": {5}},
				"map[string][]uint":     map[string][]uint{"key": {6}},
				"map[string][]uint8":    map[string][]uint8{"key": {7}},
				"map[string][]byte":     map[string][]byte{"key": {8}},
				"map[string][]uint16":   map[string][]uint16{"key": {9}},
				"map[string][]uint32":   map[string][]uint32{"key": {10}},
				"map[string][]uint64":   map[string][]uint64{"key": {11}},
				"map[string][]float32":  map[string][]float32{"key": {3.25}},
				"map[string][]float64":  map[string][]float64{"key": {1.5}},
				"map[string][]string":   map[string][]string{"key": {"hello"}},
			},
			expect: map[string]any{
				"map[string][]nil any":  map[string]any{"key": []any{nil}},
				"map[string][]nil *int": map[string]any{"key": []any{nil}},
				"map[string][]bool":     map[string]any{"key": []any{true}},
				"map[string][]int":      map[string]any{"key": []any{int64(1)}},
				"map[string][]int8":     map[string]any{"key": []any{int64(2)}},
				"map[string][]int16":    map[string]any{"key": []any{int64(3)}},
				"map[string][]int32":    map[string]any{"key": []any{int64(4)}},
				"map[string][]int64":    map[string]any{"key": []any{int64(5)}},
				"map[string][]uint":     map[string]any{"key": []any{int64(6)}},
				"map[string][]uint8":    map[string]any{"key": []byte{7}},
				"map[string][]byte":     map[string]any{"key": []byte{8}},
				"map[string][]uint16":   map[string]any{"key": []any{int64(9)}},
				"map[string][]uint32":   map[string]any{"key": []any{int64(10)}},
				"map[string][]uint64":   map[string]any{"key": []any{int64(11)}},
				"map[string][]float32":  map[string]any{"key": []any{3.25}},
				"map[string][]float64":  map[string]any{"key": []any{1.5}},
				"map[string][]string":   map[string]any{"key": []any{"hello"}},
			},
		},

		{
			name: "simple type pointers",
			inp: map[string]any{
				"*bool":        someBoolPtr,
				"*int":         someIntPtr,
				"*int8":        someInt8Ptr,
				"*int16":       someInt16Ptr,
				"*int32":       someInt32Ptr,
				"*int64":       someInt64Ptr,
				"*uint":        someUintPtr,
				"*uint8":       someUint8Ptr,
				"*byte":        someBytePtr,
				"*uint16":      someUint16Ptr,
				"*uint32":      someUint32Ptr,
				"*uint64":      someUint64Ptr,
				"*float32":     someFloat32Ptr,
				"*float64":     someFloat64Ptr,
				"*string":      someStringPtr,
				"*bool nil":    nilBool,
				"*int nil":     nilInt,
				"*int8 nil":    nilInt8,
				"*int16 nil":   nilInt16,
				"*int32 nil":   nilInt32,
				"*int64 nil":   nilInt64,
				"*uint nil":    nilUint,
				"*uint8 nil":   nilUint8,
				"*byte nil":    nilByte,
				"*uint16 nil":  nilUint16,
				"*uint32 nil":  nilUint32,
				"*uint64 nil":  nilUint64,
				"*float32 nil": nilFloat32,
				"*float64 nil": nilFloat64,
				"*string nil":  nilString,
				"**bool":       &someBoolPtr,
				"**int":        &someIntPtr,
				"**int8":       &someInt8Ptr,
				"**int16":      &someInt16Ptr,
				"**int32":      &someInt32Ptr,
				"**int64":      &someInt64Ptr,
				"**uint":       &someUintPtr,
				"**uint8":      &someUint8Ptr,
				"**byte":       &someBytePtr,
				"**uint16":     &someUint16Ptr,
				"**uint32":     &someUint32Ptr,
				"**uint64":     &someUint64Ptr,
				"**float32":    &someFloat32Ptr,
				"**float64":    &someFloat64Ptr,
				"**string":     &someStringPtr,
			},
			expect: map[string]any{
				"*bool":        someBool,
				"*int":         int64(someInt),
				"*int8":        int64(someInt8),
				"*int16":       int64(someInt16),
				"*int32":       int64(someInt32),
				"*int64":       someInt64,
				"*uint":        int64(someUint),
				"*uint8":       int64(someUint8),
				"*byte":        int64(someByte),
				"*uint16":      int64(someUint16),
				"*uint32":      int64(someUint32),
				"*uint64":      int64(someUint64),
				"*float32":     float64(someFloat32),
				"*float64":     someFloat64,
				"*string":      "hello",
				"*bool nil":    nil,
				"*int nil":     nil,
				"*int8 nil":    nil,
				"*int16 nil":   nil,
				"*int32 nil":   nil,
				"*int64 nil":   nil,
				"*uint nil":    nil,
				"*uint8 nil":   nil,
				"*byte nil":    nil,
				"*uint16 nil":  nil,
				"*uint32 nil":  nil,
				"*uint64 nil":  nil,
				"*float32 nil": nil,
				"*float64 nil": nil,
				"*string nil":  nil,
				"**bool":       someBool,
				"**int":        int64(someInt),
				"**int8":       int64(someInt8),
				"**int16":      int64(someInt16),
				"**int32":      int64(someInt32),
				"**int64":      someInt64,
				"**uint":       int64(someUint),
				"**uint8":      int64(someUint8),
				"**byte":       int64(someByte),
				"**uint16":     int64(someUint16),
				"**uint32":     int64(someUint32),
				"**uint64":     int64(someUint64),
				"**float32":    float64(someFloat32),
				"**float64":    someFloat64,
				"**string":     "hello",
			},
		},
		{
			name: "simple type pointer slices",
			inp: map[string]any{
				"[]*bool":        []*bool{someBoolPtr},
				"[]*int":         []*int{someIntPtr},
				"[]*int8":        []*int8{someInt8Ptr},
				"[]*int16":       []*int16{someInt16Ptr},
				"[]*int32":       []*int32{someInt32Ptr},
				"[]*int64":       []*int64{someInt64Ptr},
				"[]*uint":        []*uint{someUintPtr},
				"[]*uint8":       []*uint8{someUint8Ptr},
				"[]*byte":        []*byte{someBytePtr},
				"[]*uint16":      []*uint16{someUint16Ptr},
				"[]*uint32":      []*uint32{someUint32Ptr},
				"[]*uint64":      []*uint64{someUint64Ptr},
				"[]*float32":     []*float32{someFloat32Ptr},
				"[]*float64":     []*float64{someFloat64Ptr},
				"[]*string":      []*string{someStringPtr},
				"[]*bool nil":    []*bool{nilBool},
				"[]*int nil":     []*int{nilInt},
				"[]*int8 nil":    []*int8{nilInt8},
				"[]*int16 nil":   []*int16{nilInt16},
				"[]*int32 nil":   []*int32{nilInt32},
				"[]*int64 nil":   []*int64{nilInt64},
				"[]*uint nil":    []*uint{nilUint},
				"[]*uint8 nil":   []*uint8{nilUint8},
				"[]*byte nil":    []*byte{nilByte},
				"[]*uint16 nil":  []*uint16{nilUint16},
				"[]*uint32 nil":  []*uint32{nilUint32},
				"[]*uint64 nil":  []*uint64{nilUint64},
				"[]*float32 nil": []*float32{nilFloat32},
				"[]*float64 nil": []*float64{nilFloat64},
				"[]*string nil":  []*string{nilString},
				"[]**bool":       []**bool{&someBoolPtr},
				"[]**int":        []**int{&someIntPtr},
				"[]**int8":       []**int8{&someInt8Ptr},
				"[]**int16":      []**int16{&someInt16Ptr},
				"[]**int32":      []**int32{&someInt32Ptr},
				"[]**int64":      []**int64{&someInt64Ptr},
				"[]**uint":       []**uint{&someUintPtr},
				"[]**uint8":      []**uint8{&someUint8Ptr},
				"[]**byte":       []**byte{&someBytePtr},
				"[]**uint16":     []**uint16{&someUint16Ptr},
				"[]**uint32":     []**uint32{&someUint32Ptr},
				"[]**uint64":     []**uint64{&someUint64Ptr},
				"[]**float32":    []**float32{&someFloat32Ptr},
				"[]**float64":    []**float64{&someFloat64Ptr},
				"[]**string":     []**string{&someStringPtr},
			},
			expect: map[string]any{
				"[]*bool":        []any{someBool},
				"[]*int":         []any{int64(someInt)},
				"[]*int8":        []any{int64(someInt8)},
				"[]*int16":       []any{int64(someInt16)},
				"[]*int32":       []any{int64(someInt32)},
				"[]*int64":       []any{someInt64},
				"[]*uint":        []any{int64(someUint)},
				"[]*uint8":       []any{int64(someUint8)},
				"[]*byte":        []any{int64(someByte)},
				"[]*uint16":      []any{int64(someUint16)},
				"[]*uint32":      []any{int64(someUint32)},
				"[]*uint64":      []any{int64(someUint64)},
				"[]*float32":     []any{float64(someFloat32)},
				"[]*float64":     []any{someFloat64},
				"[]*string":      []any{"hello"},
				"[]*bool nil":    []any{nil},
				"[]*int nil":     []any{nil},
				"[]*int8 nil":    []any{nil},
				"[]*int16 nil":   []any{nil},
				"[]*int32 nil":   []any{nil},
				"[]*int64 nil":   []any{nil},
				"[]*uint nil":    []any{nil},
				"[]*uint8 nil":   []any{nil},
				"[]*byte nil":    []any{nil},
				"[]*uint16 nil":  []any{nil},
				"[]*uint32 nil":  []any{nil},
				"[]*uint64 nil":  []any{nil},
				"[]*float32 nil": []any{nil},
				"[]*float64 nil": []any{nil},
				"[]*string nil":  []any{nil},
				"[]**bool":       []any{someBool},
				"[]**int":        []any{int64(someInt)},
				"[]**int8":       []any{int64(someInt8)},
				"[]**int16":      []any{int64(someInt16)},
				"[]**int32":      []any{int64(someInt32)},
				"[]**int64":      []any{someInt64},
				"[]**uint":       []any{int64(someUint)},
				"[]**uint8":      []any{int64(someUint8)},
				"[]**byte":       []any{int64(someByte)},
				"[]**uint16":     []any{int64(someUint16)},
				"[]**uint32":     []any{int64(someUint32)},
				"[]**uint64":     []any{int64(someUint64)},
				"[]**float32":    []any{float64(someFloat32)},
				"[]**float64":    []any{someFloat64},
				"[]**string":     []any{"hello"},
			},
		},

		{
			name: "map of maps",
			inp: map[string]any{
				"map":    map[string]any{"k1": "v1"},
				"int":    map[string]int{"k2": 1},
				"[]int":  map[string][]int{"k3": {1, 2, 3}},
				"[]bool": map[string]bool{"t": true, "f": false},
			},
			expect: map[string]any{
				"map":    map[string]any{"k1": "v1"},
				"int":    map[string]any{"k2": int64(1)},
				"[]int":  map[string]any{"k3": []any{int64(1), int64(2), int64(3)}},
				"[]bool": map[string]any{"t": true, "f": false},
			},
		},
		{
			name: "map of spatial",
			inp: map[string]any{
				"p1":     dbtype.Point2D{SpatialRefId: 1, X: 2, Y: 3},
				"p2":     dbtype.Point3D{SpatialRefId: 4, X: 5, Y: 6, Z: 7},
				"ps":     []dbtype.Point3D{{SpatialRefId: 4, X: 5, Y: 6, Z: 7}},
				"ps_ptr": []*dbtype.Point3D{{SpatialRefId: 4, X: 5, Y: 6, Z: 7}},
			},
			expect: map[string]any{
				"p1":     &testStruct{tag: 'X', fields: []any{int64(1), float64(2), float64(3)}},
				"p2":     &testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}},
				"ps":     []any{&testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}}},
				"ps_ptr": []any{&testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}}},
			},
		},
		{
			name: "map of spatial pointers",
			inp: map[string]any{
				"p1": &dbtype.Point2D{SpatialRefId: 1, X: 2, Y: 3},
				"p2": &dbtype.Point3D{SpatialRefId: 4, X: 5, Y: 6, Z: 7},
			},
			expect: map[string]any{
				"p1": &testStruct{tag: 'X', fields: []any{int64(1), float64(2), float64(3)}},
				"p2": &testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}},
			},
		},
		{
			name: "map of temporals",
			inp: map[string]any{
				"time.Time UTC":    time.Unix(1, 2).UTC(),
				"time.Time offset": time.Unix(1, 2).In(offsetZone),
				"LocalDateTime":    dbtype.LocalDateTime(time.Unix(1, 2).UTC()),
				"Date":             dbtype.Date(time.Date(1993, 11, 31, 7, 59, 1, 100, time.UTC)),
				"Time":             dbtype.Time(time.Unix(1, 2).In(offsetZone)),
				"LocalTime":        dbtype.LocalTime(time.Unix(1, 2).UTC()),
				"Duration":         dbtype.Duration{Months: 1, Days: 2, Seconds: 3, Nanos: 4},
			},
			expect: map[string]any{
				"time.Time UTC":    &testStruct{tag: 'f', fields: []any{int64(1), int64(2), "UTC"}},
				"time.Time offset": &testStruct{tag: 'F', fields: []any{int64(101), int64(2), int64(100)}},
				"LocalDateTime":    &testStruct{tag: 'd', fields: []any{int64(1), int64(2)}},
				"Date":             &testStruct{tag: 'D', fields: []any{int64(8735)}},
				"Time":             &testStruct{tag: 'T', fields: []any{int64(101*time.Second + 2), int64(100)}},
				"LocalTime":        &testStruct{tag: 't', fields: []any{int64(1*time.Second + 2)}},
				"Duration":         &testStruct{tag: 'E', fields: []any{int64(1), int64(2), int64(3), int64(4)}},
			},
		},
		{
			name: "map of custom native types",
			inp: map[string]any{
				"custom bool":           customBool(true),
				"custom float":          customFloat(3.14),
				"custom int":            customInt(12345),
				"custom string":         customString("Hello"),
				"custom byte slice":     customByteSlice([]byte{1, 2, 3}),
				"slice of custom bytes": []customByte{1, 2, 3},
				"custom string slice":   customStringSlice([]string{"hello", "again"}),
				"custom map of ints":    customMapOfInts(map[string]int{"l": 1}),
			},
			expect: map[string]any{
				"custom bool":   true,
				"custom float":  3.14,
				"custom int":    int64(12345),
				"custom string": "Hello",
				// Custom will cause []byte to come back as []any, could be handled but maybe not worth it
				"custom byte slice":     []any{int64(1), int64(2), int64(3)},
				"slice of custom bytes": []any{int64(1), int64(2), int64(3)},
				"custom string slice":   []any{"hello", "again"},
				"custom map of ints":    map[string]any{"l": int64(1)},
			},
		},
		{
			name: "map of pointer types",
			inp: map[string]any{
				"*int":               someIntPtr,
				"*[]int":             &([]int{3}),
				"[]*int (nil)":       []*int{nil},
				"[]*any (nil)":       []*any{nil},
				"*map[string]string": &(map[string]string{"x": "y"}),
			},
			expect: map[string]any{
				"*int":               int64(someInt),
				"*[]int":             []any{int64(3)},
				"[]*int (nil)":       []any{nil},
				"[]*any (nil)":       []any{nil},
				"*map[string]string": map[string]any{"x": "y"},
			},
		},
		{
			name: "map of custom pointer types",
			inp: map[string]any{
				"*customInt":                     &someCustomInt,
				"*[]customInt":                   &([]customInt{3}),
				"*[]any (CustomInt)":             &([]any{customInt(4)}),
				"*map[customString]customString": &(map[customString]customString{"x": "y"}),
			},
			expect: map[string]any{
				"*customInt":                     int64(someCustomInt),
				"*[]customInt":                   []any{int64(3)},
				"*[]any (CustomInt)":             []any{int64(4)},
				"*map[customString]customString": map[string]any{"x": "y"},
			},
		},
	}

	paramWrappings := []struct {
		name string
		wrap func(map[string]any) map[string]any
	}{

		{
			name: "",
			wrap: func(m map[string]any) map[string]any { return m },
		},
		{
			name: " (root in map)",
			wrap: func(m map[string]any) map[string]any {
				map_ := make(map[string]any)
				map_["map"] = m
				return map_
			},
		},
		{
			name: " (root in slice in map)",
			wrap: func(m map[string]any) map[string]any {
				map_ := make(map[string]any)
				map_["map"] = []any{m}
				return map_
			},
		},
		{
			name: " (root in map in map)",
			wrap: func(m map[string]any) map[string]any {
				map_ := make(map[string]any)
				map_["map"] = map[string]any{"mapp": m}
				return map_
			},
		},
		{
			name: " (elements in map)",
			wrap: func(m map[string]any) map[string]any {
				map_ := make(map[string]any)
				for key, value := range m {
					map_[key] = map[string]any{"elem": value}
				}
				return map_
			},
		},
		{
			name: " (elements in slice)",
			wrap: func(m map[string]any) map[string]any {
				map_ := make(map[string]any)
				for key, value := range m {
					map_[key] = []any{value}
				}
				return map_
			},
		},
		{
			name: " (all elements as slice)",
			wrap: func(m map[string]any) map[string]any {
				slice := make([]any, 0, len(m))
				keys := make([]string, 0, len(m))
				for key := range m {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					slice = append(slice, m[key])
				}
				return map[string]any{"slice": slice}
			},
		},
	}

	for _, c := range paramCases {
		for _, w := range paramWrappings {
			ot.Run(c.name+w.name, func(t *testing.T) {
				x := dechunkAndUnpack(t, func(t *testing.T, out *outgoing) {
					out.begin()
					out.packMap(w.wrap(c.inp))
					out.end()
				})
				if !reflect.DeepEqual(x, w.wrap(c.expect)) {
					t.Errorf("Unpacked differs, expected\n %#v but was\n %#v", c.expect, x)
				}
			})
		}
	}

	type aStruct struct{}

	// Test packing of stuff that is expected to give an error
	paramErrorCases := []struct {
		name string
		inp  map[string]any
		err  error
	}{
		{
			name: "map with non string key",
			inp: map[string]any{
				"m": map[int]string{1: "y"},
			},
			err: &db.UnsupportedTypeError{},
		},
		{
			name: "a random struct",
			inp: map[string]any{
				"m": aStruct{},
			},
			err: &db.UnsupportedTypeError{},
		},
		{
			name: "a random *struct",
			inp: map[string]any{
				"m": &aStruct{},
			},
			err: &db.UnsupportedTypeError{},
		},
	}
	for _, c := range paramErrorCases {
		var err error
		out := &outgoing{
			chunker:   newChunker(),
			packer:    packstream.Packer{},
			onPackErr: func(e error) { err = e },
			onIoErr: func(_ context.Context, err error) {
				ot.Fatalf("Should be no io errors in this test: %s", err)
			},
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

func TestCredentialsRedaction(outer *testing.T) {
	outer.Parallel()

	type testCase struct {
		description        string
		produceBoltMessage func(*outgoing)
		credentials        string
	}

	testCases := []testCase{
		{
			description: "HELLO msg",
			produceBoltMessage: func(o *outgoing) {
				o.appendHello(map[string]any{
					"credentials": "sup3rs3cr3t",
				})
			},
			credentials: "sup3rs3cr3t",
		},
		{
			description: "LOGON msg",
			produceBoltMessage: func(o *outgoing) {
				o.appendLogon(map[string]any{
					"credentials": "letmein!",
				})
			},
			credentials: "letmein!",
		},
	}

	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			logger := &inMemoryBoltLogger{}
			outWriter := &outgoing{
				chunker:    newChunker(),
				packer:     packstream.Packer{},
				boltLogger: logger,
			}

			testCase.produceBoltMessage(outWriter)

			AssertFalse(t, logger.AnyClientMessageContains(testCase.credentials))
		})
	}
}

type inMemoryBoltLogger struct {
	clientMessages []string
	serverMessages []string
}

func (log *inMemoryBoltLogger) LogClientMessage(context string, msg string, args ...any) {
	log.clientMessages = append(log.clientMessages, log.format(context, msg, args))
}

func (log *inMemoryBoltLogger) LogServerMessage(context string, msg string, args ...any) {
	log.serverMessages = append(log.serverMessages, log.format(context, msg, args))
}

func (log *inMemoryBoltLogger) AnyClientMessageContains(substring string) bool {
	for _, clientMessage := range log.clientMessages {
		if strings.Contains(clientMessage, substring) {
			return true
		}
	}
	return false
}

func (log *inMemoryBoltLogger) format(context string, msg string, args []any) string {
	return fmt.Sprintf("[%s] %s", context, fmt.Sprintf(msg, args...))
}
