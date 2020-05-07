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

type chunker struct {
	wr     io.Writer
	chunks [][]byte
}

const max_chunk_size = 0xffff

func newChunker(wr io.Writer) *chunker {
	return &chunker{
		wr:     wr,
		chunks: make([][]byte, 0, 2),
	}
}

func (c *chunker) beginMessage() {
	c.chunk()
}

func (c *chunker) chunk() {
	chunk := make([]byte, 0, 0x100)
	chunk = append(chunk, 0x00, 0x00)
	c.chunks = append(c.chunks, chunk)
}

func (c *chunker) endMessage() {
	c.chunks = append(c.chunks, []byte{0x00, 0x00})
}

// Writes to current chunk or creates new chunks as needed.
func (c *chunker) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	written := 0
	for len(p) > 0 {
		index := len(c.chunks) - 1
		chunk := c.chunks[index]

		currChunkSize := len(chunk)
		leftInChunk := (max_chunk_size + 2) - currChunkSize

		// There is room left in current chunk to write all
		if len(p) <= leftInChunk {
			c.chunks[index] = append(chunk, p...)
			written += len(p)
			return written, nil
		}

		// This message spills over to another chunk
		c.chunks[index] = append(chunk, p[:leftInChunk]...)
		written += leftInChunk
		p = p[leftInChunk:]
		c.chunk()
	}

	return written, nil
}

// Sends all chunks to server
func (c *chunker) send() error {
	// Discard chunks while writing them
	for len(c.chunks) > 0 {
		// Pop chunk
		chunk := c.chunks[0]
		c.chunks = c.chunks[1:]

		// First two bytes is size of chunk, needs to be updated
		// Last two bytes should always be zero. Size only includes
		// user data, not size itself or trailing zeroes.
		size := uint16(len(chunk) - 2)
		binary.BigEndian.PutUint16(chunk, size)

		// Write chunk to underlying writer (probably the TCP connection)
		_, err := c.wr.Write(chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

// Discards all chunks
func (c *chunker) reset() {
	// Preserve capacity
	c.chunks = c.chunks[:0]
}
