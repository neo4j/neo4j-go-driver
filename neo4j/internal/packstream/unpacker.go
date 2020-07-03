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
)

// Called by unpacker to let receiver check all fields and let the receiver return an
// hydrated instance or an error if any of the fields or number of fields are different
// expected. Values from fields must be copied, the fields slice cannot be saved.
type Hydrate func(tag StructTag, fields []interface{}) (interface{}, error)

type Unpacker struct {
	buf    []byte
	offset uint32
	length uint32
	err    error
	fields []interface{} // Cached field list for unpacking structs faster
	foff   int           // Offset into fields when nesting structs
}

func (u *Unpacker) pop() byte {
	if u.offset < u.length {
		x := u.buf[u.offset]
		u.offset += 1
		return x
	}
	u.err = &IoError{}
	return 0
}

func (u *Unpacker) read(n uint32) []byte {
	start := u.offset
	end := u.offset + n
	if end > u.length {
		u.err = &IoError{}
		return nil
	}
	u.offset = end
	return u.buf[start:end]
}

func (u *Unpacker) readStruct(hydrate Hydrate, numFields int) (interface{}, error) {
	if numFields < 0 || numFields > 0x0f {
		return nil, &IllegalFormatError{msg: fmt.Sprintf("Invalid struct size: %d", numFields)}
	}

	// Read struct tag
	tag := StructTag(u.pop())

	// No need for allocating when no fields
	if numFields == 0 && u.err == nil {
		return hydrate(tag, nil)
	}

	// Use cached fields slice and grow it if needed
	if len(u.fields) < (u.foff + numFields) {
		newFields := make([]interface{}, len(u.fields)+numFields)
		copy(newFields, u.fields)
		u.fields = newFields
	}

	// Read fields and pass them to hydrator
	foff := u.foff
	u.foff += numFields
	for i := foff; i < numFields+foff; i++ {
		u.fields[i], _ = u.unpack(hydrate)
	}
	u.foff -= numFields
	if u.err != nil {
		return nil, u.err
	}
	fields := u.fields[u.foff : u.foff+numFields]
	return hydrate(tag, fields)
}

func (u *Unpacker) readStr(n uint32) (interface{}, error) {
	buf := u.read(n)
	if u.err != nil {
		return nil, u.err
	}
	return string(buf), nil
}

func (u *Unpacker) readArr(hydrate Hydrate, n uint32) ([]interface{}, error) {
	arr := make([]interface{}, n)
	for i := range arr {
		arr[i], _ = u.unpack(hydrate)
	}
	return arr, u.err
}

func (u *Unpacker) readMap(hydrate Hydrate, n uint32) (map[string]interface{}, error) {
	m := make(map[string]interface{}, n)
	for i := uint32(0); i < n; i++ {
		keyx, _ := u.unpack(hydrate)
		key, ok := keyx.(string)
		if !ok {
			if u.err == nil {
				u.err = &IllegalFormatError{msg: fmt.Sprintf("Map key is not string type: %T", keyx)}
			}
			return nil, u.err
		}
		valx, _ := u.unpack(hydrate)
		m[key] = valx
	}

	return m, u.err
}

func (u *Unpacker) unpack(hydrate Hydrate) (interface{}, error) {
	// Read field marker
	marker := u.pop()
	if u.err != nil {
		return nil, u.err
	}

	if marker < 0x80 {
		// Tiny positive int
		return int64(marker), nil
	}
	if marker > 0x80 && marker < 0x90 {
		// Tiny string
		return u.readStr(uint32(marker) - 0x80)
	}
	if marker > 0x90 && marker < 0xa0 {
		// Tiny array
		return u.readArr(hydrate, uint32(marker-0x90))
	}
	if marker >= 0xf0 {
		// Tiny negative int
		return int64(marker) - 0x100, nil
	}
	if marker > 0xa0 && marker < 0xb0 {
		// Tiny map
		return u.readMap(hydrate, uint32(marker-0xa0))
	}
	if marker >= 0xb0 && marker < 0xc0 {
		return u.readStruct(hydrate, int(marker-0xb0))
	}

	switch marker {
	case 0x80:
		// Empty string
		return "", nil
	case 0x90:
		// Empty array
		return []interface{}{}, nil
	case 0xa0:
		// Empty map
		return map[string]interface{}{}, nil
	case 0xc0:
		// Nil
		return nil, nil
	case 0xc1:
		// Float
		buf := u.read(8)
		if u.err != nil {
			return nil, u.err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(buf)), nil
	case 0xc2:
		// False
		return false, nil
	case 0xc3:
		// True
		return true, nil
	case 0xc8:
		// Int, 1 byte
		return int64(int8(u.pop())), u.err
	case 0xc9:
		// Int, 2 bytes
		buf := u.read(2)
		if u.err != nil {
			return nil, u.err
		}
		return int64(int16(binary.BigEndian.Uint16(buf))), nil
	case 0xca:
		// Int, 4 bytes
		buf := u.read(4)
		if u.err != nil {
			return nil, u.err
		}
		return int64(int32(binary.BigEndian.Uint32(buf))), nil
	case 0xcb:
		// Int, 8 bytes
		buf := u.read(8)
		if u.err != nil {
			return nil, u.err
		}
		return int64(binary.BigEndian.Uint64(buf)), nil
	case 0xcc:
		// byte[] of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return nil, u.err
		}
		return u.read(uint32(num)), u.err
	case 0xcd:
		// byte[] of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint16(buf)
		return u.read(uint32(num)), u.err
	case 0xce:
		// byte[] of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint32(buf)
		return u.read(num), u.err
	case 0xd0:
		// String of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return nil, u.err
		}
		return u.readStr(uint32(num))
	case 0xd1:
		// String of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint16(buf)
		return u.readStr(uint32(num))
	case 0xd2:
		// String of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint32(buf)
		return u.readStr(num)
	case 0xd4:
		// Array of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return nil, u.err
		}
		return u.readArr(hydrate, uint32(num))
	case 0xd5:
		// Array of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint16(buf)
		return u.readArr(hydrate, uint32(num))
	case 0xd6:
		// Array of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint32(buf)
		return u.readArr(hydrate, num)
	case 0xd8:
		// Map of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return nil, u.err
		}
		return u.readMap(hydrate, uint32(num))
	case 0xd9:
		// Map of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint16(buf)
		return u.readMap(hydrate, uint32(num))
	case 0xda:
		// Map of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return nil, u.err
		}
		num := binary.BigEndian.Uint32(buf)
		return u.readMap(hydrate, num)
	}

	return nil, &IllegalFormatError{msg: fmt.Sprintf("Unknown marker: %02x", marker)}
}

func (u *Unpacker) UnpackStruct(buf []byte, hydrate Hydrate) (interface{}, error) {
	u.buf = buf
	u.offset = 0
	u.length = uint32(len(buf))
	u.err = nil
	u.foff = 0

	numFields := u.pop() - 0xb0
	if u.err != nil {
		return nil, u.err
	}
	return u.readStruct(hydrate, int(numFields))
}
