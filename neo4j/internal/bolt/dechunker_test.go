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
	"fmt"
	"io"
	"testing"
)

func TestDechunker(ot *testing.T) {
	cases := []struct {
		name     string
		stream   []byte
		reads    []int
		expected []byte
	}{
		{
			name: "one byte",
			stream: []byte{
				0x00, 0x02, 0x66, 0x77, 0x00, 0x00, // Chunk of size 2
			},
			reads:    []int{1},
			expected: []byte{0x66},
		},
		{
			name: "all bytes in chunk, two reads",
			stream: []byte{
				0x00, 0x02, 0x66, 0x77, 0x00, 0x00, // Chunk of size 2
			},
			reads:    []int{1, 1},
			expected: []byte{0x66, 0x77},
		},
		{
			name: "all bytes in two chunks, one read",
			stream: []byte{
				0x00, 0x02, 0x66, 0x77, 0x00, 0x00, // Chunk of size 2
				0x00, 0x02, 0x88, 0x99, 0x00, 0x00, // Chunk of size 2
			},
			reads:    []int{4},
			expected: []byte{0x66, 0x77, 0x88, 0x99},
		},
		{
			name: "all bytes in two chunks, evil reads",
			stream: []byte{
				0x00, 0x03, 0x11, 0x22, 0x33, 0x00, 0x00, // Chunk of size 3
				0x00, 0x07, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0x00, 0x00, // Chunk of size 7
			},
			reads:    []int{1, 4, 2, 3},
			expected: []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa},
		},
	}

	for _, c := range cases {
		ot.Run(fmt.Sprintf("Dechunk %s", c.name), func(t *testing.T) {
			// Initialize underlying reader, typically corresponds to reading from network.
			urd := bytes.NewBuffer(c.stream)

			dec := newDechunker(urd)

			// Perform the reads as specified in test case
			res := make([]byte, 0)
			for i, r := range c.reads {
				// Make an output buffer same size as current read operation.
				// The result in this buffer should be same size as in the test case.
				buf := make([]byte, r)
				// Use io.ReadFull to emulate how a real consumer would use the reader in
				// a correct way (probably by also using io.ReadFull). Since it is up to the Read
				// implementation how many actual reads that are needed we shouldn't test the
				// exact behaviour of the dechunker but that it behaves correctly.
				n, err := io.ReadFull(dec, buf)
				if err != nil {
					t.Fatalf("Unexpected read %d error: %s", i, err)
				}
				if n != r {
					t.Fatalf("Length of read %d differs, expected %d but was %d", i, r, n)
				}
				res = append(res, buf...)
			}
			if !bytes.Equal(res, c.expected) {
				t.Errorf("Buffer differs, expected %+v but was %+v", c.expected, res)
			}
		})
	}
}
