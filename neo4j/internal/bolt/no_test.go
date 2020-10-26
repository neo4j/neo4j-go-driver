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
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

type testStruct struct {
	tag    byte
	fields []interface{}
}

func (t *testStruct) String() string {
	s := fmt.Sprintf("Struct{tag: %d, fields: [", t.tag)
	for i, x := range t.fields {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%+v", x)
	}
	return s + "]}"
}

// Utility to test hydration
func serverHydrator(unpacker *packstream.Unpacker) interface{} {
	switch unpacker.Curr {
	case packstream.PackedInt:
		return unpacker.Int()
	case packstream.PackedFloat:
		return unpacker.Float()
	case packstream.PackedStr:
		return unpacker.String()
	case packstream.PackedStruct:
		panic("No support for unpacking struct in server stub")
	case packstream.PackedByteArray:
		return unpacker.ByteArray()
	case packstream.PackedArray:
		n := unpacker.Len()
		a := make([]interface{}, n)
		for i := range a {
			unpacker.Next()
			a[i] = serverHydrator(unpacker)
		}
		return a
	case packstream.PackedMap:
		n := unpacker.Len()
		m := make(map[string]interface{}, n)
		for ; n > 0; n-- {
			unpacker.Next()
			key := unpacker.String()
			unpacker.Next()
			m[key] = serverHydrator(unpacker)
		}
		return m
	case packstream.PackedNil:
		return nil
	case packstream.PackedTrue:
		return true
	case packstream.PackedFalse:
		return false
	default:
		panic("Unsupported type to unpack")
	}
}
