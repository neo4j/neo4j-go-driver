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

package packstream

import (
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
)

type testStruct struct {
	tag    byte
	fields []interface{}
}

// Utility to simplify testcase writing
// Also an example of how a "generic" unpack function looks like.
func unpack(u *Unpacker) interface{} {
	u.Next()
	switch u.Curr {
	case PackedInt:
		return u.Int()
	case PackedFloat:
		return u.Float()
	case PackedStr:
		return u.String()
	case PackedByteArray:
		return u.ByteArray()
	case PackedNil:
		return nil
	case PackedTrue:
		return true
	case PackedFalse:
		return false
	case PackedArray:
		l := u.Len()
		a := make([]interface{}, l)
		for i := range a {
			a[i] = unpack(u)
		}
		return a
	case PackedMap:
		l := u.Len()
		m := make(map[string]interface{}, l)
		for i := uint32(0); i < l; i++ {
			u.Next()
			k := u.String()
			m[k] = unpack(u)
		}
		return m
	case PackedStruct:
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
		panic(fmt.Sprintf("Unhandled Curr: %d", u.Curr))
	}
}

// Utility to simplify testcase writing
// Also an example of how a "generic" pack function looks like.
func pack(p *Packer, x interface{}) {
	if x == nil {
		p.Nil()
		return
	}
	switch v := x.(type) {
	case int64:
		p.Int64(v)
	case int32:
		p.Int32(v)
	case int16:
		p.Int16(v)
	case int8:
		p.Int8(v)
	case int:
		p.Int(v)
	case uint64:
		p.Uint64(v)
	case uint32:
		p.Uint32(v)
	case uint16:
		p.Uint16(v)
	case uint8:
		p.Uint8(v)
	case bool:
		p.Bool(v)
	case string:
		p.String(v)
	case float64:
		p.Float64(v)
	case float32:
		p.Float32(v)
	case []byte:
		p.Bytes(v)
	case []interface{}:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []string:
		p.Strings(v)
	case []int64:
		p.Int64s(v)
	case []int16:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []int:
		p.Ints(v)
	case []int8:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []uint16:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []uint32:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []int32:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []uint64:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case []float64:
		p.Float64s(v)
	case []float32:
		p.ArrayHeader(len(v))
		for _, y := range v {
			pack(p, y)
		}
	case map[string]interface{}:
		p.MapHeader(len(v))
		for s, y := range v {
			p.String(s)
			pack(p, y)
		}
	case map[string]string:
		p.StringMap(v)
	case map[string]int:
		p.IntMap(v)
	case *testStruct:
		p.StructHeader(byte(v.tag), len(v.fields))
		for _, y := range v.fields {
			pack(p, y)
		}
	default:
		panic(fmt.Sprintf("Unhandled pack type: %T", v))
	}
}

func TestPackStream(ot *testing.T) {
	dumper := hex.Dumper(os.Stdout)
	defer dumper.Close()

	const (
		// Floats, 64 and 32 bits supported
		zeroFloat64 float64 = 0
		piFloat64   float64 = 3.14
		piFloat32   float32 = 3.14
		// int64, limits of bit representations
		negInt8To16  int64 = -0x10
		negInt16To24 int64 = -0x80
		negInt24To40 int64 = -0x8000
		negInt40To72 int64 = -0x80000000
		posInt8To24  int64 = 127
		posInt24To40 int64 = 0x8000 - 1
		posInt40To72 int64 = 0x80000000 - 1
		// All kinds of int types supported
		auint8  uint8  = math.MaxUint8
		aint8   int8   = math.MinInt8
		auint16 uint16 = math.MaxUint16
		aint16  int16  = math.MinInt16
		auint32 uint32 = math.MaxUint32
		aint32  int32  = math.MinInt32
	)

	genStr := func(l int) (string, []byte) {
		b := make([]byte, l)
		for i := range b {
			b[i] = byte('a')
		}
		return string(b), b
	}

	str15, str15Bytes := genStr(15)
	str16, str16Bytes := genStr(16)
	str255, str255Bytes := genStr(255)
	str256, str256Bytes := genStr(256)
	str65535, str65535Bytes := genStr(65535)
	str65536, str65536Bytes := genStr(65536)

	genByt := func(l int) []byte {
		b := make([]byte, l)
		for i := range b {
			b[i] = 0x3f
		}
		return b
	}

	byt255 := genByt(255)
	byt256 := genByt(256)
	byt65535 := genByt(65535)
	byt65536 := genByt(65536)

	genArr := func(l int) []int16 {
		b := make([]int16, l)
		for i := range b {
			b[i] = 1
		}
		return b
	}

	arr15 := genArr(15)
	arr16 := genArr(16)
	arr255 := genArr(255)
	arr256 := genArr(256)
	arr65535 := genArr(65535)
	arr65536 := genArr(65536)

	arr16toBytes := func(a []int16) []byte {
		b := make([]byte, len(a))
		for i, v := range a {
			b[i] = byte(v)
		}
		return b
	}

	arr16toUnpackedIntSlice := func(a []int16) []interface{} {
		b := make([]interface{}, len(a))
		for i, v := range a {
			b[i] = int64(v)
		}
		return b
	}

	emptyStruct := &testStruct{tag: 0x66}
	maxStruct := &testStruct{
		tag:    0x67,
		fields: []interface{}{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}}
	structOfStruct := &testStruct{
		tag: 0x66,
		fields: []interface{}{
			&testStruct{tag: 0x67, fields: []interface{}{"1", "2"}},
			&testStruct{tag: 0x68, fields: []interface{}{"3", "4"}}}}

	cases := []struct {
		name           string
		value          interface{}
		expectPacked   []byte
		expectUnpacked interface{}
		testUnpacked   bool
	}{
		// Nil
		{name: "nil", value: nil, testUnpacked: true,
			expectUnpacked: nil,
			expectPacked:   []byte{0xc0}},

		// Bools
		{name: "true", value: true, testUnpacked: true,
			expectUnpacked: true,
			expectPacked:   []byte{0xc3}},
		{name: "false", value: false, testUnpacked: true,
			expectUnpacked: false,
			expectPacked:   []byte{0xc2}},

		// Floats
		{name: "zero float64", value: zeroFloat64, testUnpacked: true,
			expectUnpacked: float64(zeroFloat64),
			expectPacked:   []byte{0xc1, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "pi float64", value: piFloat64, testUnpacked: true,
			expectUnpacked: float64(piFloat64),
			expectPacked:   []byte{0xc1, 0x40, 0x09, 0x1e, 0xb8, 0x51, 0xeb, 0x85, 0x1f}},
		{name: "pi float32", value: piFloat32, testUnpacked: true,
			expectUnpacked: float64(piFloat32),
			expectPacked:   []byte{0xc1, 0x40, 0x09, 0x1e, 0xb8, 0x60, 0x00, 0x00, 0x00}},

		// Ints
		{name: "zero int 8", value: 0, testUnpacked: true,
			expectUnpacked: int64(0),
			expectPacked:   []byte{0x00}},
		{name: "neg int 8", value: -7, testUnpacked: true,
			expectUnpacked: int64(-7),
			expectPacked:   []byte{0xf9}},
		{name: "neg int 8, 8->16", value: negInt8To16, testUnpacked: true,
			expectUnpacked: int64(negInt8To16),
			expectPacked:   []byte{0xf0}},
		{name: "neg int 16, 8->16", value: negInt8To16 - 1, testUnpacked: true,
			expectUnpacked: int64(negInt8To16 - 1),
			expectPacked:   []byte{0xc8, 0xef}},
		{name: "neg int 16, 16->24", value: negInt16To24, testUnpacked: true,
			expectUnpacked: int64(negInt16To24),
			expectPacked:   []byte{0xc8, 0x80}},
		{name: "neg int 24, 16->24", value: negInt16To24 - 1, testUnpacked: true,
			expectUnpacked: int64(negInt16To24 - 1),
			expectPacked:   []byte{0xc9, 0xff, 0x7f}},
		{name: "neg int 24, 24->40", value: negInt24To40, testUnpacked: true,
			expectUnpacked: int64(negInt24To40),
			expectPacked:   []byte{0xc9, 0x80, 0x00}},
		{name: "neg int 40, 24->40", value: negInt24To40 - 1, testUnpacked: true,
			expectUnpacked: int64(negInt24To40 - 1),
			expectPacked:   []byte{0xca, 0xff, 0xff, 0x7f, 0xff}},
		{name: "neg int 40, 40->72", value: negInt40To72, testUnpacked: true,
			expectUnpacked: int64(negInt40To72),
			expectPacked:   []byte{0xca, 0x80, 0x00, 0x00, 0x00}},
		{name: "neg int 72, 40->72", value: negInt40To72 - 1, testUnpacked: true,
			expectUnpacked: int64(negInt40To72 - 1),
			expectPacked:   []byte{0xcb, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff}},
		{name: "pos int 8", value: 7, testUnpacked: true,
			expectUnpacked: int64(7),
			expectPacked:   []byte{0x07}},
		{name: "pos int 8, 8->24", value: posInt8To24, testUnpacked: true,
			expectUnpacked: int64(posInt8To24),
			expectPacked:   []byte{0x7f}},
		{name: "pos int 24, 8->24", value: posInt8To24 + 1, testUnpacked: true,
			expectUnpacked: int64(posInt8To24 + 1),
			expectPacked:   []byte{0xc9, 0x00, 0x80}},
		{name: "pos int 24, 24->40", value: posInt24To40, testUnpacked: true,
			expectUnpacked: int64(posInt24To40),
			expectPacked:   []byte{0xc9, 0x7f, 0xff}},
		{name: "pos int 40, 24->40", value: posInt24To40 + 1, testUnpacked: true,
			expectUnpacked: int64(posInt24To40 + 1),
			expectPacked:   []byte{0xca, 0x00, 0x00, 0x80, 0x00}},
		{name: "pos int 40, 40->72", value: posInt40To72, testUnpacked: true,
			expectUnpacked: int64(posInt40To72),
			expectPacked:   []byte{0xca, 0x7f, 0xff, 0xff, 0xff}},
		{name: "pos int 72, 40->72", value: posInt40To72 + 1, testUnpacked: true,
			expectUnpacked: int64(posInt40To72 + 1),
			expectPacked:   []byte{0xcb, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00}},
		{name: "uint8", value: auint8, testUnpacked: true,
			expectUnpacked: int64(auint8),
			expectPacked:   []byte{0xc9, 0x00, 0xff}},
		{name: "int8", value: aint8, testUnpacked: true,
			expectUnpacked: int64(aint8),
			expectPacked:   []byte{0xc8, 0x80}},
		{name: "uint16", value: auint16, testUnpacked: true,
			expectUnpacked: int64(auint16),
			expectPacked:   []byte{0xca, 0x00, 0x00, 0xff, 0xff}},
		{name: "int16", value: aint16, testUnpacked: true,
			expectUnpacked: int64(aint16),
			expectPacked:   []byte{0xc9, 0x80, 0x00}},
		{name: "uint32", value: auint32, testUnpacked: true,
			expectUnpacked: int64(auint32),
			expectPacked:   []byte{0xcb, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff}},
		{name: "int32", value: aint32, testUnpacked: true,
			expectUnpacked: int64(aint32),
			expectPacked:   []byte{0xca, 0x80, 0x00, 0x00, 0x00}},
		// Strings
		{name: "String, empty", value: "", testUnpacked: true,
			expectUnpacked: "",
			expectPacked:   []byte{0x80}},
		{name: "string, 1", value: "1", testUnpacked: true,
			expectUnpacked: "1",
			expectPacked:   []byte{0x81, 0x31}},
		{name: "string, 12", value: "12", testUnpacked: true,
			expectUnpacked: "12",
			expectPacked:   []byte{0x82, 0x31, 0x32}},
		{name: "string, 123", value: "123", testUnpacked: true,
			expectUnpacked: "123",
			expectPacked:   []byte{0x83, 0x31, 0x32, 0x33}},
		{name: "string, 4, 4->8", value: str15, testUnpacked: true,
			expectUnpacked: str15,
			expectPacked:   append([]byte{0x8f}, str15Bytes...)},
		{name: "string, 8, 4->8", value: str16, testUnpacked: true,
			expectUnpacked: str16,
			expectPacked:   append([]byte{0xd0, 0x10}, str16Bytes...)},
		{name: "string, 8, 8->16", value: str255, testUnpacked: true,
			expectUnpacked: str255,
			expectPacked:   append([]byte{0xd0, 0xff}, str255Bytes...)},
		{name: "string, 16, 8->16", value: str256, testUnpacked: true,
			expectUnpacked: str256,
			expectPacked:   append([]byte{0xd1, 0x01, 0x00}, str256Bytes...)},
		{name: "string, 16, 16->32", value: str65535, testUnpacked: true,
			expectUnpacked: str65535,
			expectPacked:   append([]byte{0xd1, 0xff, 0xff}, str65535Bytes...)},
		{name: "string, 32, 16->32", value: str65536, testUnpacked: true,
			expectUnpacked: str65536,
			expectPacked:   append([]byte{0xd2, 0x00, 0x01, 0x00, 0x00}, str65536Bytes...)},

		// Slice of bytes
		{name: "[]byte, empty", value: []byte{}, testUnpacked: true,
			expectUnpacked: []byte{},
			expectPacked:   []byte{0xcc, 0x00}},
		{name: "[]byte, some", value: []byte{0x01, 0x02, 0x03}, testUnpacked: true,
			expectUnpacked: []byte{0x01, 0x02, 0x03},
			expectPacked:   []byte{0xcc, 0x03, 0x01, 0x02, 0x03}},
		{name: "[]byte, 8, 8->16", value: byt255, testUnpacked: true,
			expectUnpacked: byt255,
			expectPacked:   append([]byte{0xcc, 0xff}, byt255...)},
		{name: "[]byte, 16, 8->16", value: byt256, testUnpacked: true,
			expectUnpacked: byt256,
			expectPacked:   append([]byte{0xcd, 0x01, 0x00}, byt256...)},
		{name: "[]byte, 16, 16->32", value: byt65535, testUnpacked: true,
			expectUnpacked: byt65535,
			expectPacked:   append([]byte{0xcd, 0xff, 0xff}, byt65535...)},
		{name: "[]byte, 32, 16->32", value: byt65536, testUnpacked: true,
			expectUnpacked: byt65536,
			expectPacked:   append([]byte{0xce, 0x00, 0x01, 0x00, 0x00}, byt65536...)},

		// Slice of interface{}
		{name: "[]interface{}, empty", value: []interface{}{},
			expectPacked: []byte{0x90}},
		{name: "[]interface{}, sample", value: []interface{}{nil, "s", 1},
			expectPacked: []byte{0x93, 0xc0, 0x81, 0x73, 0x01}},

		// Slice of strings
		{name: "[]string, empty", value: []string{}, testUnpacked: true,
			expectUnpacked: []interface{}{},
			expectPacked:   []byte{0x90}},
		{name: "[]string, samples", value: []string{"short", str16}, testUnpacked: true,
			expectUnpacked: []interface{}{"short", str16},
			expectPacked: []byte{
				0x92, 0x85, 0x73, 0x68, 0x6f, 0x72, 0x74, 0xd0, 0x10, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61}},
		{name: "[]int64, samples", value: []int64{1, posInt24To40 + 1, 0}, testUnpacked: true,
			expectUnpacked: []interface{}{int64(1), int64(posInt24To40 + 1), int64(0)},
			expectPacked:   []byte{0x93, 0x01, 0xca, 0x00, 0x00, 0x80, 0x00, 0x00}},

		// Slice sizes
		{name: "array size, 4, 4->8", value: arr15, testUnpacked: true,
			expectUnpacked: arr16toUnpackedIntSlice(arr15),
			expectPacked:   append([]byte{0x9f}, arr16toBytes(arr15)...)},
		{name: "array size, 8, 4->8", value: arr16, testUnpacked: true,
			expectUnpacked: arr16toUnpackedIntSlice(arr16),
			expectPacked:   append([]byte{0xd4, 0x10}, arr16toBytes(arr16)...)},
		{name: "array size, 8, 8->16", value: arr255, testUnpacked: true,
			expectUnpacked: arr16toUnpackedIntSlice(arr255),
			expectPacked:   append([]byte{0xd4, 0xff}, arr16toBytes(arr255)...)},
		{name: "array size, 16, 8->16", value: arr256, testUnpacked: true,
			expectUnpacked: arr16toUnpackedIntSlice(arr256),
			expectPacked:   append([]byte{0xd5, 0x01, 0x00}, arr16toBytes(arr256)...)},
		{name: "array size, 16, 16->32", value: arr65535, testUnpacked: true,
			expectUnpacked: arr16toUnpackedIntSlice(arr65535),
			expectPacked:   append([]byte{0xd5, 0xff, 0xff}, arr16toBytes(arr65535)...)},
		{name: "array size, 32, 16->32", value: arr65536, testUnpacked: true,
			expectUnpacked: arr16toUnpackedIntSlice(arr65536),
			expectPacked:   append([]byte{0xd6, 0x00, 0x01, 0x00, 0x00}, arr16toBytes(arr65536)...)},

		// Slice of ints (bytes excluded)
		{name: "[]int, type", value: []int{1},
			expectPacked: []byte{0x91, 0x01}},
		{name: "[]int8, type", value: []int8{1},
			expectPacked: []byte{0x91, 0x01}},
		{name: "[]uint8, type", value: []uint8{1},
			expectPacked: []byte{0xcc, 0x01, 0x01}}, // byte is alias fo uint8
		{name: "[]uint16, type", value: []uint16{1},
			expectPacked: []byte{0x91, 0x01}},
		{name: "[]int16, type", value: []int16{1},
			expectPacked: []byte{0x91, 0x01}},
		{name: "[]uint32, type", value: []uint32{1},
			expectPacked: []byte{0x91, 0x01}},
		{name: "[]int32, type", value: []int32{1},
			expectPacked: []byte{0x91, 0x01}},
		{name: "[]uint64, type", value: []uint64{1},
			expectPacked: []byte{0x91, 0x01}},

		// Slice of floats
		{name: "[]float64, samples", value: []float64{piFloat64}, testUnpacked: true,
			expectUnpacked: []interface{}{float64(piFloat64)},
			expectPacked:   []byte{0x91, 0xc1, 0x40, 0x09, 0x1e, 0xb8, 0x51, 0xeb, 0x85, 0x1f}},
		{name: "[]float32, type", value: []float32{},
			expectPacked: []byte{0x90}},

		// Map[string] of interface{}, main entry point for sending queries.
		{name: "map[string]interface{}, empty", value: map[string]interface{}{}, testUnpacked: true,
			expectUnpacked: map[string]interface{}{},
			expectPacked:   []byte{0xa0}},
		{name: "map[string]interface{}, sample", value: map[string]interface{}{"nil": nil}, testUnpacked: true,
			expectUnpacked: map[string]interface{}{"nil": nil},
			expectPacked:   []byte{0xa1, 0x83, 0x6e, 0x69, 0x6c, 0xc0}},
		{name: "map[string]interface{}, sample", value: map[string]interface{}{"s": "str"}, testUnpacked: true,
			expectUnpacked: map[string]interface{}{"s": interface{}("str")},
			expectPacked:   []byte{0xa1, 0x81, 0x73, 0x83, 0x73, 0x74, 0x72}},
		{name: "map[string]string, empty", value: map[string]string{},
			expectPacked: []byte{0xa0}},
		{name: "map[string]string, sample", value: map[string]string{"key": "value"},
			expectPacked: []byte{
				0xa1, 0x83, 0x6b, 0x65, 0x79, 0x85, 0x76, 0x61, 0x6c, 0x75, 0x65}},
		{name: "map[string]int, type", value: map[string]int{"l": 1},
			expectPacked: []byte{0xa1, 0x81, 0x6c, 0x01}},

		// Structs
		{name: "struct, empty", value: emptyStruct, testUnpacked: true,
			expectUnpacked: emptyStruct,
			expectPacked:   []byte{0xb0, 0x66}},
		{name: "struct, one", value: &testStruct{tag: 0x01, fields: []interface{}{1}}, testUnpacked: false,
			expectUnpacked: &testStruct{tag: 0x01, fields: []interface{}{1}},
			expectPacked:   []byte{0xb1, 0x01, 0x01}},
		{name: "struct, max size", value: maxStruct, testUnpacked: true,
			expectUnpacked: maxStruct,
			expectPacked: []byte{
				0xbf, 0x67, 0x81, 0x31, 0x81, 0x32, 0x81, 0x33, 0x81, 0x34, 0x81, 0x35, 0x81,
				0x36, 0x81, 0x37, 0x81, 0x38, 0x81, 0x39, 0x82, 0x31, 0x30, 0x82, 0x31, 0x31,
				0x82, 0x31, 0x32, 0x82, 0x31, 0x33, 0x82, 0x31, 0x34, 0x82, 0x31, 0x35}},
		{name: "struct with structs", value: structOfStruct, testUnpacked: true,
			expectUnpacked: structOfStruct,
			expectPacked: []byte{
				0xb2, 0x66, 0xb2, 0x67, 0x81, 0x31, 0x81, 0x32, 0xb2, 0x68, 0x81, 0x33, 0x81,
				0x34}},
	}

	for _, c := range cases {
		ot.Run(fmt.Sprintf("Packing of %s", c.name), func(t *testing.T) {
			buf := []byte{}
			p := Packer{}
			p.Begin([]byte{})
			pack(&p, c.value)
			buf, err := p.End()

			if err != nil {
				t.Fatalf("Unable to pack: %s", p.err)
			}
			if len(c.expectPacked) != len(buf) {
				dumper.Write(buf)
				t.Fatalf("Packed buffer differs in size. Got %+v expected %+v",
					len(buf), len(c.expectPacked))
			}
			for i, x := range c.expectPacked {
				if buf[i] != x {
					dumper.Write(buf)
					t.Fatalf("Packed first diff at %d. Got %+v expected %+v",
						i, buf[i], c.expectPacked[i])
				}
			}
		})

		// Unpacking
		// Some test cases doesn't make sense here.
		if !c.testUnpacked {
			continue
		}

		ot.Run(fmt.Sprintf("Unpacking of %s", c.name), func(t *testing.T) {
			u := &Unpacker{}
			u.Reset(c.expectPacked)
			// Use the unpacker as intended to unpack something generic
			x := unpack(u)
			if u.Err != nil {
				t.Fatalf("Unable to unpack: %s", u.Err)
			}
			if !reflect.DeepEqual(x, c.expectUnpacked) {
				t.Errorf("Unpacked differs, expected %+v (%T) but was %+v (%T)", c.expectUnpacked, c.expectUnpacked, x, x)
			}
		})
	}

	// Map sizes and multiple entries in maps.
	// Hard to test with above testcase setup due to Go:s randomness when accessing maps.
	mapSizeCases := []struct {
		name         string
		size         int
		expectHeader []byte
	}{
		{name: "4, 4->8", size: 15,
			expectHeader: []byte{0xaf}},
		{name: "8, 4->8", size: 16,
			expectHeader: []byte{0xd8, 0x10}},
		{name: "8, 8->16", size: 255,
			expectHeader: []byte{0xd8, 0xff}},
		{name: "16, 8->16", size: 256,
			expectHeader: []byte{0xd9, 0x01, 0x00}},
		{name: "16, 16->32", size: 65535,
			expectHeader: []byte{0xd9, 0xff, 0xff}},
		{name: "32, 16->32", size: 65536,
			expectHeader: []byte{0xda, 0x00, 0x01, 0x00, 0x00}},
	}

	// Packing test cases for map sizes
	for _, c := range mapSizeCases {
		m := make(map[string]interface{}, c.size)
		for i := 0; i < c.size; i++ {
			m[fmt.Sprintf("%d", i)] = i
		}

		// Pack the map
		buf := []byte{}
		p := &Packer{}
		p.Begin(buf)
		pack(p, m)
		buf, err := p.End()
		if err != nil {
			ot.Fatalf("Unable to pack: %s", p.err)
		}

		ot.Run(fmt.Sprintf("Packing of map size %s", c.name), func(t *testing.T) {
			// Compare the header
			if len(buf) < len(c.expectHeader) {
				t.Fatalf("Packed map has less bytes(%d) than size of expected header (%d)",
					len(buf), len(c.expectHeader))
			}
			for i, e := range c.expectHeader {
				if buf[i] != e {
					t.Fatalf("Expected header and actual header differs at %d", i)
				}
			}
		})

		ot.Run(fmt.Sprintf("Unpacking of map size %s", c.name), func(t *testing.T) {
			u := &Unpacker{}
			u.Reset(buf)
			ux := unpack(u)

			um, ok := ux.(map[string]interface{})
			if !ok {
				t.Errorf("Unpacked is not a map")
			}
			if err != nil {
				t.Fatalf("Unable to unpack: %s", err)
			}
			// Make sure the unpacked corresponds to input
			if len(um) != len(m) {
				t.Errorf("Unpacked map differs in size from expected %d vs %d", len(um), len(m))
			}
			for k, v := range m {
				uvx, exists := um[k]
				if !exists {
					t.Fatalf("Key %s does not exist in unpacked map", k)
				}

				uv := uvx.(int64)
				if uv != int64(v.(int)) {
					t.Errorf("Value in unpacked map is wrong, expected %d but was %d", v, uv)
				}
			}
		})
	}

	// Packer error cases, things that packer is expected to fail on
	packerErrorCases := []struct {
		name        string
		value       interface{}
		expectedErr interface{}
	}{
		{name: "uin64 overflow", expectedErr: &OverflowError{},
			value: (uint64(math.MaxInt64) + 1)},
		{name: "too big struct", expectedErr: &OverflowError{},
			value: &testStruct{tag: 0x67, fields: []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}},
	}
	for _, c := range packerErrorCases {
		ot.Run(fmt.Sprintf("Packing error of %s", c.name), func(t *testing.T) {
			buf := []byte{}
			p := &Packer{}
			v := c.value
			p.Begin(buf)
			pack(p, v)
			_, err := p.End()
			if err == nil {
				t.Fatal("Should have gotten an error!")
			}
			if reflect.TypeOf(err) != reflect.TypeOf(c.expectedErr) {
				t.Errorf("Wrong type of error, expected %T but was %T", c.expectedErr, p.err)
			}
		})
	}
}
