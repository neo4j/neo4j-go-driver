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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

// Benchmarks reception of messages over the protocol parts chunking+packstream unpacking
// Should correspond to how setup is done in boltx protocol implementations besides not
// the underlying reader not being network in this case.

// Message consisting of some small entries
var rawMsgSomeSmall = []byte{
	// Length of chunk
	0x00, 0x0b,
	0xb0 + 1, /* Num fields*/
	byte(msgRecord),
	0x98, /* Tiny array of 8 */
	0x01, /* Tiny byte */
	0x02, /* Tiny byte */
	0x03, /* Tiny byte */
	0x04, /* Tiny byte */
	0x05, /* Tiny byte */
	0x06, /* Tiny byte */
	0x07, /* Tiny byte */
	0x08, /* Tiny byte */
	// Signal end of message to chunk layer
	0x00, 0x00,
}

func BenchmarkUnpackingSmall(b *testing.B) {
	buf := []byte{}
	network := &bytes.Buffer{}
	unpacker := &packstream.Unpacker{}
	var err error

	for i := 0; i < b.N; i++ {
		network.Write(rawMsgSomeSmall)
		buf, err = dechunkMessage(network, buf)
		if err != nil {
			b.Fatal(err)
		}
		_, err = unpacker.UnpackStruct(buf, hydrate)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnpackingStructs(b *testing.B) {
	// Build an example record containing some spatial types instances. Spatial types are pretty
	// simple. Temporal types needs separate benchmarking.
	packer := &packstream.Packer{}
	buf, _ := packer.PackStruct([]byte{}, dehydrate, msgRecord,
		&dbtype.Point2D{}, &dbtype.Point3D{}, &dbtype.Point2D{}, &dbtype.Point3D{}, &dbtype.Point2D{}, &dbtype.Point3D{})
	unpacker := &packstream.Unpacker{}

	for i := 0; i < b.N; i++ {
		unpacker.UnpackStruct(buf, hydrate)
	}
}
