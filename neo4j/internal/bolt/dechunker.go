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
	"encoding/binary"
	"io"
)

type dechunker struct {
	rd   io.Reader
	size int // Size of current chunk or -1 if no current
}

func newDechunker(rd io.Reader) *dechunker {
	return &dechunker{
		rd:   rd,
		size: -1,
	}
}

func (d *dechunker) Read(p []byte) (n int, err error) {
	// Read end of chunk zeroes
	if d.size == 0 {
		buf := []byte{0x00, 0x00}
		_, err := io.ReadFull(d.rd, buf)
		if err != nil {
			return 0, err
		}
		d.size = -1
	}

	// Read chunk length if not in a chunk already
	if d.size == -1 {
		buf := []byte{0x00, 0x00}
		_, err := io.ReadFull(d.rd, buf)
		if err != nil {
			return 0, err
		}
		d.size = int(binary.BigEndian.Uint16(buf))
	}

	// Just forward read to underlying reader and count off the bytes.
	// BUT don't read more than what's left of the current chunk.
	if len(p) > d.size {
		p = p[:d.size]
	}
	n, err = d.rd.Read(p)
	d.size -= n
	return n, err
}
