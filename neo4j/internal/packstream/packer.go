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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

// Called by packer to let caller send custom object as structs not known by packstream.
// Used as a convenience to let the caller be lazy and just send in data and be called
// when packstream doesn't know what it is instead of checking all data up front.
type Dehydrate func(x interface{}) (*Struct, error)

type Packer struct {
	buf       []byte
	dehydrate Dehydrate
	err       error // Sticky error
}

func (p *Packer) PackStruct(buf []byte, dehydrate Dehydrate, tag StructTag, fields ...interface{}) ([]byte, error) {
	p.buf = buf
	p.dehydrate = dehydrate
	p.err = nil
	p.writeStruct(tag, fields)
	return p.buf, p.err
}

func (p *Packer) writeStruct(tag StructTag, fields []interface{}) {
	l := len(fields)
	if l > 0x0f {
		p.err = &OverflowError{msg: "Trying to pack struct with too many fields"}
		return
	}

	p.buf = append(p.buf, 0xb0+byte(l), byte(tag))
	for _, f := range fields {
		p.pack(f)
	}
}

func (p *Packer) writeInt(i int64) {
	switch {
	case int64(-0x10) <= i && i < int64(0x80):
		p.buf = append(p.buf, byte(i))
	case int64(-0x80) <= i && i < int64(-0x10):
		p.buf = append(p.buf, 0xc8, byte(i))
	case int64(-0x8000) <= i && i < int64(0x8000):
		buf := [3]byte{0xc9}
		binary.BigEndian.PutUint16(buf[1:], uint16(i))
		p.buf = append(p.buf, buf[:]...)
	case int64(-0x80000000) <= i && i < int64(0x80000000):
		buf := [5]byte{0xca}
		binary.BigEndian.PutUint32(buf[1:], uint32(i))
		p.buf = append(p.buf, buf[:]...)
	default:
		buf := [9]byte{0xcb}
		binary.BigEndian.PutUint64(buf[1:], uint64(i))
		p.buf = append(p.buf, buf[:]...)
	}
}

func (p *Packer) writeFloat(f float64) {
	buf := [9]byte{0xc1}
	binary.BigEndian.PutUint64(buf[1:], math.Float64bits(f))
	p.buf = append(p.buf, buf[:]...)
}

func (p *Packer) writeListHeader(ll int, shortOffset, longOffset byte) {
	l := int64(ll)
	hdr := make([]byte, 0, 1+4)
	if l < 0x10 {
		hdr = append(hdr, shortOffset+byte(l))
	} else {
		switch {
		case l < 0x100:
			hdr = append(hdr, []byte{longOffset, byte(l)}...)
		case l < 0x10000:
			hdr = hdr[:1+2]
			hdr[0] = longOffset + 1
			binary.BigEndian.PutUint16(hdr[1:], uint16(l))
		case l < math.MaxUint32:
			hdr = hdr[:1+4]
			hdr[0] = longOffset + 2
			binary.BigEndian.PutUint32(hdr[1:], uint32(l))
		default:
			p.err = &OverflowError{msg: fmt.Sprintf("Trying to pack too large list of size %d ", l)}
			return
		}
	}
	p.buf = append(p.buf, hdr...)
}

func (p *Packer) writeString(s string) {
	p.writeListHeader(len(s), 0x80, 0xd0)
	p.buf = append(p.buf, []byte(s)...)
}

func (p *Packer) writeArrayHeader(l int) {
	p.writeListHeader(l, 0x90, 0xd4)
}

func (p *Packer) writeMapHeader(l int) {
	p.writeListHeader(l, 0xa0, 0xd8)
}

func (p *Packer) writeBytes(b []byte) {
	hdr := make([]byte, 0, 1+4)
	l := int64(len(b))
	switch {
	case l < 0x100:
		hdr = append(hdr, 0xcc, byte(l))
	case l < 0x10000:
		hdr = hdr[:1+2]
		hdr[0] = 0xcd
		binary.BigEndian.PutUint16(hdr[1:], uint16(l))
	case l < 0x100000000:
		hdr = hdr[:1+4]
		hdr[0] = 0xce
		binary.BigEndian.PutUint32(hdr[1:], uint32(l))
	default:
		p.err = &OverflowError{msg: fmt.Sprintf("Trying to pack too large byte array of size %d", l)}
		return
	}
	p.buf = append(p.buf, hdr...)
	p.buf = append(p.buf, b...)
}

func (p *Packer) writeBool(b bool) {
	if b {
		p.buf = append(p.buf, 0xc3)
		return
	}
	p.buf = append(p.buf, 0xc2)
}

func (p *Packer) writeNil() {
	p.buf = append(p.buf, 0xc0)
}

func (p *Packer) tryDehydrate(x interface{}) {
	s, err := p.dehydrate(x)
	if err != nil {
		p.err = err
		return
	}
	if s == nil {
		p.writeNil()
		return
	}
	p.writeStruct(s.Tag, s.Fields)
}

func (p *Packer) writeSlice(x interface{}) {
	// Check for optimized cases, resort to slower reflection if not found
	switch v := x.(type) {
	case []byte:
		p.writeBytes(v)
	case []interface{}:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			// Recurse
			p.pack(s)
		}
	case []string:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeString(s)
		}
	case []int64:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(s)
		}
	case []uint64:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.checkOverflowInt(s)
			p.writeInt(int64(s))
		}
	case []int:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(int64(s))
		}
	case []int8:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(int64(s))
		}
	case []uint16:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(int64(s))
		}
	case []int16:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(int64(s))
		}
	case []uint32:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(int64(s))
		}
	case []int32:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeInt(int64(s))
		}
	case []float64:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeFloat(s)
		}
	case []float32:
		p.writeArrayHeader(len(v))
		for _, s := range v {
			p.writeFloat(float64(s))
		}
	default:
		// We know that this is some kind of slice
		rv := reflect.ValueOf(x)
		num := rv.Len()
		p.writeArrayHeader(num)
		for i := 0; i < num; i++ {
			rx := rv.Index(i)
			p.pack(rx.Interface())
		}
	}
}

func (p *Packer) writeMap(x interface{}) {
	switch v := x.(type) {
	case map[string]interface{}:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			// Recurse
			p.pack(v)
		}
	case map[string]string:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeString(v)
		}
	case map[string]float64:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeFloat(v)
		}
	case map[string]float32:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeFloat(float64(v))
		}
	case map[string]int64:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(v)
		}
	case map[string]uint64:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.checkOverflowInt(v)
			p.writeInt(int64(v))
		}
	case map[string]int:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]int8:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]uint8:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]uint16:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]int16:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]uint32:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]int32:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeInt(int64(v))
		}
	case map[string]bool:
		p.writeMapHeader(len(v))
		for k, v := range v {
			p.writeString(k)
			p.writeBool(v)
		}
	default:
		// We know that this is some kind of map
		rv := reflect.ValueOf(x)
		num := rv.Len()
		p.writeMapHeader(num)
		// We will not detect if key is something else but a string until checking first
		// element, this means that we will succeed in packaging map[int]string {} as an empty
		// map. TODO: When Go 1.12 is min version use MapRange instead.
		keys := rv.MapKeys()
		for _, ik := range keys {
			if ik.Kind() != reflect.String {
				p.err = &UnsupportedTypeError{t: reflect.TypeOf(x)}
				return
			}
			p.writeString(ik.String())
			iv := rv.MapIndex(ik)
			p.pack(iv.Interface())
		}
	}
}

func (p *Packer) checkOverflowInt(i uint64) {
	if i > math.MaxInt64 {
		p.err = &OverflowError{msg: "Trying to pack uint64 that doesn't fit into int64"}
	}
}

func (p *Packer) pack(x interface{}) {
	if x == nil {
		p.writeNil()
		return
	}

	// Use reflect to handle cases where users have done strange things like:
	//   type SpecialInt int64
	t := reflect.ValueOf(x)
	switch t.Kind() {
	case reflect.Bool:
		p.writeBool(t.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		p.writeInt(t.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := t.Uint()
		p.checkOverflowInt(u)
		p.writeInt(int64(u))
	case reflect.Float32, reflect.Float64:
		p.writeFloat(t.Float())
	case reflect.String:
		p.writeString(t.String())
	case reflect.Ptr:
		if t.IsNil() {
			p.writeNil()
			return
		}
		// Inspect what the pointer points to
		i := reflect.Indirect(t)
		switch i.Kind() {
		case reflect.Struct:
			s, isS := x.(*Struct)
			if isS {
				p.writeStruct(s.Tag, s.Fields)
			} else {
				// Unknown type, call dehydration hook to make it into a struct
				p.tryDehydrate(x)
			}
		default:
			p.pack(i.Interface())
		}
	case reflect.Struct:
		// Unknown type, call dehydration hook to make it into a struct
		p.tryDehydrate(x)
	case reflect.Slice:
		p.writeSlice(x)
	case reflect.Map:
		p.writeMap(x)
	default:
		p.err = &UnsupportedTypeError{t: reflect.TypeOf(x)}
	}
}
