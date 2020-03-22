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
	"testing"
)

type chunkReceiver struct {
	chunks [][]byte
}

func (r *chunkReceiver) Write(p []byte) (int, error) {
	r.chunks = append(r.chunks, p)
	return len(p), nil
}

func TestChunker(ot *testing.T) {
	cases := []struct {
		name   string
		size   int
		writes [][]byte
		chunks [][]byte
	}{
		{
			name:   "zero writes",
			size:   1,
			writes: [][]byte{},
			chunks: [][]byte{},
		},
		{
			name:   "1 write 1/2 of size",
			size:   2,
			writes: [][]byte{{0x01}},
			chunks: [][]byte{{0x00, 0x01, 0x01, 0x00, 0x00}},
		},
		{
			name:   "1 write 2/2 of size",
			size:   2,
			writes: [][]byte{{0x01, 0x02}},
			chunks: [][]byte{{0x00, 0x02, 0x01, 0x02, 0x00, 0x00}},
		},
		{
			name:   "1 write 3/2 of size",
			size:   2,
			writes: [][]byte{{0x01, 0x02, 0x03}},
			chunks: [][]byte{
				{0x00, 0x02, 0x01, 0x02, 0x00, 0x00},
				{0x00, 0x01, 0x03, 0x00, 0x00},
			},
		},
		{
			name:   "1 write 7/2 of size",
			size:   2,
			writes: [][]byte{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}},
			chunks: [][]byte{
				{0x00, 0x02, 0x01, 0x02, 0x00, 0x00},
				{0x00, 0x02, 0x03, 0x04, 0x00, 0x00},
				{0x00, 0x02, 0x05, 0x06, 0x00, 0x00},
				{0x00, 0x01, 0x07, 0x00, 0x00},
			},
		},
		{
			name:   "multiple writes 7/2 of size",
			size:   2,
			writes: [][]byte{{0x01}, []byte{0x02, 0x03, 0x04}, []byte{05, 06}, []byte{0x07}},
			chunks: [][]byte{
				{0x00, 0x02, 0x01, 0x02, 0x00, 0x00},
				{0x00, 0x02, 0x03, 0x04, 0x00, 0x00},
				{0x00, 0x02, 0x05, 0x06, 0x00, 0x00},
				{0x00, 0x01, 0x07, 0x00, 0x00},
			},
		},
		{
			name:   "multiple writes 7/3 of size",
			size:   3,
			writes: [][]byte{{0x01}, []byte{0x02, 0x03, 0x04}, []byte{05, 06}, []byte{0x07}},
			chunks: [][]byte{
				{0x00, 0x03, 0x01, 0x02, 0x03, 0x00, 0x00},
				{0x00, 0x03, 0x04, 0x05, 0x06, 0x00, 0x00},
				{0x00, 0x01, 0x07, 0x00, 0x00},
			},
		},
	}
	for _, c := range cases {
		ot.Run(fmt.Sprintf("Chunking %s", c.name), func(t *testing.T) {
			rec := &chunkReceiver{}
			chunker := newChunker(rec, c.size)
			chunker.add()
			for _, p := range c.writes {
				chunker.Write(p)
			}
			chunker.send()

			// Check what has been received
			if len(rec.chunks) != len(c.chunks) {
				t.Fatalf("Wrong number of sent chunks, expected %d but was %d", len(c.chunks), len(rec.chunks))
			}
			for i, x := range rec.chunks {
				if bytes.Compare(c.chunks[i], x) != 0 {
					t.Errorf("Received chunk %d differs. Expected:\n%+v\nBut was:\n%+v", i, c.chunks[i], x)
				}
			}
		})
	}
}
