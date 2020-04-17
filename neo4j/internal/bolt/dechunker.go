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
	"errors"
	"io"
)

type dechunker struct {
	rd   io.Reader
	size int
}

func newDechunker(rd io.Reader) *dechunker {
	return &dechunker{rd: rd}
}

func (d *dechunker) beginMessage() error {
	return d.readSize()
}

func (d *dechunker) readSize() error {
	buf := []byte{0x00, 0x00}
	_, err := io.ReadFull(d.rd, buf)
	if err != nil {
		return err
	}
	d.size = int(binary.BigEndian.Uint16(buf))
	return nil
}

func (d *dechunker) endMessage() error {
	if d.size > 0 {
		// Reader hasn't consumed everything!
		return errors.New("Unconsumed data")
	}
	// At end of message there should be a zero chunk
	if err := d.readSize(); err != nil {
		return err
	}
	if d.size != 0 {
		return errors.New("Not a zero chunk")
	}
	return nil
}

func (d *dechunker) Read(p []byte) (n int, err error) {
	// Adjustment of how much reading that is done will make sure that we
	// hit the zero.
	// Try to continue in the next chunk, if it is the zero chunk we should
	// return an end of file like error since reader is trying to read beyond the message
	// boundary.
	if d.size == 0 {
		if err := d.readSize(); err != nil {
			return 0, err
		}
		if d.size == 0 {
			return 0, errors.New("out of message")
		}
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
