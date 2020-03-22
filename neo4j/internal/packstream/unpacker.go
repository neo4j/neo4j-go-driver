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
)

type Unpacker struct {
	rd io.Reader
}

func NewUnpacker(rd io.Reader) *Unpacker {
	return &Unpacker{
		rd: rd,
	}
}

func (u *Unpacker) read(n uint32) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(u.rd, buf)
	if err == nil {
		return buf, nil
	}
	return nil, &IoError{inner: err}
}

func (u *Unpacker) readStruct(hf HydratorFactory, numFields int) (interface{}, error) {
	if numFields < 0 || numFields > 0x0f {
		return nil, &IllegalFormatError{msg: fmt.Sprintf("Invalid struct size: %d", numFields)}
	}

	// Read struct tag
	buf, err := u.read(1)
	if err != nil {
		return nil, err
	}
	tag := StructTag(buf[0])

	// Reach out to get actual object to hydrate into
	hydrator, err := hf.Hydrator(tag, numFields)
	if err != nil {
		return nil, err
	}

	// Read fields
	for numFields > 0 {
		field, err := u.Unpack(hf)
		if err != nil {
			return nil, err
		}
		err = hydrator.HydrateField(field)
		if err != nil {
			return nil, err
		}
		numFields--
	}

	// All fields hydrated
	err = hydrator.HydrationComplete()
	if err != nil {
		return nil, err
	}

	return hydrator, nil
}

func (u *Unpacker) readNum(x interface{}) error {
	err := binary.Read(u.rd, binary.BigEndian, x)
	if err == nil {
		return nil
	}
	return &IoError{inner: err}
}

func (u *Unpacker) readStr(n uint32) (interface{}, error) {
	buf, err := u.read(n)
	if err != nil {
		return nil, err
	}
	return string(buf), nil
}

func (u *Unpacker) readArr(hf HydratorFactory, n uint32) ([]interface{}, error) {
	var err error
	arr := make([]interface{}, n)
	for i := range arr {
		arr[i], err = u.Unpack(hf)
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func (u *Unpacker) readMap(hf HydratorFactory, n uint32) (map[string]interface{}, error) {
	m := make(map[string]interface{}, n)
	for i := uint32(0); i < n; i++ {
		keyx, err := u.Unpack(hf)
		if err != nil {
			return nil, err
		}
		key, ok := keyx.(string)
		if !ok {
			return nil, &IllegalFormatError{msg: fmt.Sprintf("Map key is not string type: %T", keyx)}
		}
		valx, err := u.Unpack(hf)
		if err != nil {
			return nil, err
		}
		m[key] = valx
	}
	return m, nil
}

func (u *Unpacker) Unpack(hf HydratorFactory) (interface{}, error) {
	// Read field marker
	buf, err := u.read(1)
	if err != nil {
		return nil, err
	}
	marker := buf[0]

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
		return u.readArr(hf, uint32(marker-0x90))
	}
	if marker >= 0xf0 {
		// Tiny negative int
		return int64(marker) - 0x100, nil
	}
	if marker > 0xa0 && marker < 0xb0 {
		// Tiny map
		return u.readMap(hf, uint32(marker-0xa0))
	}
	if marker >= 0xb0 && marker < 0xc0 {
		return u.readStruct(hf, int(marker-0xb0))
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
		var f float64
		if err = u.readNum(&f); err != nil {
			return nil, err
		}
		return f, nil
	case 0xc2:
		// False
		return false, nil
	case 0xc3:
		// True
		return true, nil
	case 0xc8:
		// Int, 1 byte
		var x int8
		if err = u.readNum(&x); err != nil {
			return nil, err
		}
		return int64(x), nil
	case 0xc9:
		// Int, 2 bytes
		var x int16
		if err = u.readNum(&x); err != nil {
			return nil, err
		}
		return int64(x), nil
	case 0xca:
		// Int, 4 bytes
		var x int32
		if err = u.readNum(&x); err != nil {
			return nil, err
		}
		return int64(x), nil
	case 0xcb:
		// Int, 8 bytes
		var x int64
		if err = u.readNum(&x); err != nil {
			return nil, err
		}
		return x, nil
	case 0xcc:
		// byte[] of length up to 0xff
		var num uint8
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.read(uint32(num))
	case 0xcd:
		// byte[] of length up to 0xffff
		var num uint16
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.read(uint32(num))
	case 0xce:
		// byte[] of length up to 0xffffffff
		var num uint32
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.read(num)
	case 0xd0:
		// String of length up to 0xff
		var num uint8
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readStr(uint32(num))
	case 0xd1:
		// String of length up to 0xffff
		var num uint16
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readStr(uint32(num))
	case 0xd2:
		// String of length up to 0xffffffff
		var num uint32
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readStr(num)
	case 0xd4:
		// Array of length up to 0xff
		var num uint8
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readArr(hf, uint32(num))
	case 0xd5:
		// Array of length up to 0xffff
		var num uint16
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readArr(hf, uint32(num))
	case 0xd6:
		// Array of length up to 0xffffffff
		var num uint32
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readArr(hf, num)
	case 0xd8:
		// Map of length up to 0xff
		var num uint8
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readMap(hf, uint32(num))
	case 0xd9:
		// Map of length up to 0xffff
		var num uint16
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readMap(hf, uint32(num))
	case 0xda:
		// Map of length up to 0xffffffff
		var num uint32
		if err = u.readNum(&num); err != nil {
			return nil, err
		}
		return u.readMap(hf, num)
	}

	return nil, &IllegalFormatError{msg: fmt.Sprintf("Unknown marker: %02x", marker)}
}

func (u *Unpacker) UnpackStruct(hf HydratorFactory) (interface{}, error) {
	// Read struct marker
	buf, err := u.read(1)
	if err != nil {
		return nil, err
	}
	return u.readStruct(hf, int(buf[0]-0xb0))
}
