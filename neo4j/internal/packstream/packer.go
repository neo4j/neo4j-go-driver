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
	"io"
	"math"
	"reflect"
)

// Called by packer to let caller send custom object as structs not known by packstream.
// Used as a convenience to let the caller be lazy and just send in data and be called
// when packstream doesn't know what it is instead of checking all data up front.
type Dehydrate func(x interface{}) (*Struct, error)

type Packer struct {
	wr        io.Writer
	dehydrate Dehydrate
}

func NewPacker(wr io.Writer, dehydrate Dehydrate) *Packer {
	if dehydrate == nil {
		dehydrate = func(x interface{}) (*Struct, error) {
			return nil, &UnsupportedTypeError{t: reflect.TypeOf(x)}
		}
	}

	return &Packer{
		wr:        wr,
		dehydrate: dehydrate,
	}
}

// Convenience function for caller that doesn't want to implement Struct
// interface.
func (p *Packer) PackStruct(tag StructTag, fields ...interface{}) error {
	// Convert to simple struct implementation and pass it on the generic pack.
	s := &Struct{Tag: tag, Fields: fields}
	return p.Pack(s)
}

func (p *Packer) write(buf []byte) error {
	// Wrap error in IO error type?
	_, err := p.wr.Write(buf)
	if err == nil {
		return nil
	}
	return &IoError{inner: err}
}

func (p *Packer) writeStruct(s *Struct) error {
	l := len(s.Fields)
	if l > 0x0f {
		return &OverflowError{msg: "Trying to pack struct with too many fields"}
	}

	buf := []byte{0xb0 + byte(l), byte(s.Tag)}
	err := p.write(buf)
	if err != nil {
		return err
	}

	for _, f := range s.Fields {
		err = p.Pack(f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Packer) writeInt(i int64) error {
	switch {
	case int64(-0x10) <= i && i < int64(0x80):
		return p.write([]byte{byte(i)})
	case int64(-0x80) <= i && i < int64(-0x10):
		return p.write([]byte{0xc8, byte(i)})
	case int64(-0x8000) <= i && i < int64(0x8000):
		buf := [3]byte{0xc9}
		binary.BigEndian.PutUint16(buf[1:], uint16(i))
		return p.write(buf[:])
	case int64(-0x80000000) <= i && i < int64(0x80000000):
		buf := [5]byte{0xca}
		binary.BigEndian.PutUint32(buf[1:], uint32(i))
		return p.write(buf[:])
	default:
		buf := [9]byte{0xcb}
		binary.BigEndian.PutUint64(buf[1:], uint64(i))
		return p.write(buf[:])
	}
}

func (p *Packer) writeFloat(f float64) error {
	buf := [9]byte{0xc1}
	binary.BigEndian.PutUint64(buf[1:], math.Float64bits(f))
	return p.write(buf[:])
}

func (p *Packer) writeListHeader(ll int, shortOffset, longOffset byte) error {
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
			return &OverflowError{msg: fmt.Sprintf("Trying to pack too large list of size %d ", l)}
		}
	}
	return p.write(hdr)
}

func (p *Packer) writeString(s string) error {
	err := p.writeListHeader(len(s), 0x80, 0xd0)
	if err != nil {
		return err
	}
	return p.write([]byte(s))
}

func (p *Packer) writeArrayHeader(l int) error {
	return p.writeListHeader(l, 0x90, 0xd4)
}

func (p *Packer) writeMapHeader(l int) error {
	return p.writeListHeader(l, 0xa0, 0xd8)
}

func (p *Packer) writeBytes(b []byte) error {
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
		return &OverflowError{msg: fmt.Sprintf("Trying to pack too large byte array of size %d", l)}
	}
	err := p.write(hdr)
	if err != nil {
		return err
	}
	return p.write(b)
}

func (p *Packer) writeBool(b bool) error {
	if b {
		return p.write([]byte{0xc3})
	}
	return p.write([]byte{0xc2})
}

func (p *Packer) writeNil() error {
	return p.write([]byte{0xc0})
}

func (p *Packer) tryDehydrate(x interface{}) error {
	s, err := p.dehydrate(x)
	if err != nil {
		return err
	}
	if s == nil {
		return p.writeNil()
	}
	return p.writeStruct(s)
}

func (p *Packer) writeSlice(x interface{}) error {
	// Check for optimized cases, resort to slower reflection if not found
	switch v := x.(type) {
	case []byte:
		return p.writeBytes(v)
	case []interface{}:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			// Recurse
			err = p.Pack(s)
			if err != nil {
				return err
			}
		}
		return nil
	case []string:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeString(s)
			if err != nil {
				return err
			}
		}
		return nil
	case []int64:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(s)
			if err != nil {
				return err
			}
		}
		return nil
	case []uint64:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = overflowInt(s)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []int:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []int8:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []uint16:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []int16:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []uint32:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []int32:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeInt(int64(s))
			if err != nil {
				return err
			}
		}
		return nil
	case []float64:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeFloat(s)
			if err != nil {
				return err
			}
		}
		return nil
	case []float32:
		err := p.writeArrayHeader(len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			err = p.writeFloat(float64(s))
			if err != nil {
				return err
			}
		}
		return nil
	default:
		// We know that this is some kind of slice
		rv := reflect.ValueOf(x)
		num := rv.Len()
		if err := p.writeArrayHeader(num); err != nil {
			return err
		}
		for i := 0; i < num; i++ {
			rx := rv.Index(i)
			if err := p.Pack(rx.Interface()); err != nil {
				return err
			}
		}
		return nil
	}
}

func (p *Packer) writeMap(x interface{}) error {
	switch v := x.(type) {
	case map[string]interface{}:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			// Recurse
			err = p.Pack(v)
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]string:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeString(v)
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]float64:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeFloat(v)
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]float32:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeFloat(float64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]int64:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(v)
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]uint64:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = overflowInt(v)
			if err != nil {
				return err
			}
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]int:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]int8:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]uint8:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]uint16:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]int16:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]uint32:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]int32:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeInt(int64(v))
			if err != nil {
				return err
			}
		}
		return nil
	case map[string]bool:
		err := p.writeMapHeader(len(v))
		if err != nil {
			return err
		}
		for k, v := range v {
			err = p.writeString(k)
			if err != nil {
				return err
			}
			err = p.writeBool(v)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		// We know that this is some kind of map
		rv := reflect.ValueOf(x)
		num := rv.Len()
		if err := p.writeMapHeader(num); err != nil {
			return err
		}
		// We will not detect if key is something else but a string until checking first
		// element, this means that we will succeed in packaging map[int]string {} as an empty
		// map. TODO: When Go 1.12 is min version use MapRange instead.
		keys := rv.MapKeys()
		for _, ik := range keys {
			if ik.Kind() != reflect.String {
				return &UnsupportedTypeError{t: reflect.TypeOf(x)}
			}
			if err := p.writeString(ik.String()); err != nil {
				return err
			}
			iv := rv.MapIndex(ik)
			if err := p.Pack(iv.Interface()); err != nil {
				return err
			}
		}

		return nil
	}
}

func overflowInt(i uint64) error {
	if i > math.MaxInt64 {
		return &OverflowError{msg: "Trying to pack uint64 that doesn't fit into int64"}
	}
	return nil
}

func (p *Packer) Pack(x interface{}) error {
	if x == nil {
		return p.writeNil()
	}

	t := reflect.ValueOf(x)
	switch t.Kind() {
	case reflect.Bool:
		return p.writeBool(t.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return p.writeInt(t.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := t.Uint()
		if err := overflowInt(u); err != nil {
			return err
		}
		return p.writeInt(int64(u))
	case reflect.Float32, reflect.Float64:
		return p.writeFloat(t.Float())
	case reflect.String:
		return p.writeString(t.String())
	case reflect.Ptr:
		if t.IsNil() {
			return p.writeNil()
		}
		// Inspect what the pointer points to
		i := reflect.Indirect(t)
		switch i.Kind() {
		case reflect.Struct:
			s, isS := x.(*Struct)
			if isS {
				return p.writeStruct(s)
			}
			// Unknown type, call dehydration hook to make it into a struct
			return p.tryDehydrate(x)
		default:
			return p.Pack(i.Interface())
		}
	case reflect.Struct:
		// Unknown type, call dehydration hook to make it into a struct
		return p.tryDehydrate(x)
	case reflect.Slice:
		return p.writeSlice(x)
	case reflect.Map:
		return p.writeMap(x)
	}
	return &UnsupportedTypeError{t: reflect.TypeOf(x)}
}
