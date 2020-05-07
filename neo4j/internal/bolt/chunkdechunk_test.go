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
	"io"
	"testing"
)

func TestChunker(ot *testing.T) {
	msgN := make([]byte, 0xffff)
	for i := range msgN {
		msgN[i] = byte(i)
	}
	msgS := []byte{1, 2, 3, 4, 5, 6, 7}
	msgL := append([]byte{}, msgN...)
	msgL = append(msgL, msgN...)
	msgL = append(msgL, msgS...)

	writeSmall := func(c *chunker, out []byte) []byte {
		c.beginMessage()
		out = append(out, 0x00, byte(len(msgS)))
		c.Write(msgS)
		out = append(out, msgS...)
		c.endMessage()
		out = append(out, 0x00, 0x00)
		return out
	}

	writeLarge := func(c *chunker, out []byte) []byte {
		c.beginMessage()
		// Write this in parts to reflect what really will happen
		m := msgL[0:]
		parts := []int{12, 189, 91, 6}
		p := 0
		for len(m) > 0 {
			l := parts[p%len(parts)]
			if len(m) < l {
				c.Write(m)
				break
			}
			x := m[:l]
			c.Write(x)
			m = m[l:]
			p++
		}

		out = append(out, 0xff, 0xff)
		out = append(out, msgN...)
		out = append(out, 0xff, 0xff)
		out = append(out, msgN...)
		out = append(out, 0x00, byte(len(msgS)))
		out = append(out, msgS...)
		c.endMessage()
		out = append(out, 0x00, 0x00)
		return out
	}

	assertBuf := func(t *testing.T, buf bytes.Buffer, exp []byte) {
		t.Helper()
		if bytes.Compare(exp, buf.Bytes()) != 0 {
			t.Errorf("Chunked output differs")
		}
	}

	assertSlices := func(t *testing.T, inp, exp []byte) {
		t.Helper()
		if bytes.Compare(exp, inp) != 0 {
			t.Errorf("Dechunked output differs")
		}
	}

	receiveAndAssertMessage := func(t *testing.T, dec *dechunker, expected []byte) {
		t.Helper()
		err := dec.beginMessage()
		assertNoError(t, err)
		msg := make([]byte, len(expected))
		_, err = io.ReadFull(dec, msg)
		assertNoError(t, err)
		err = dec.endMessage()
		assertNoError(t, err)
		assertSlices(t, msg, expected)
	}

	ot.Run("Small message", func(t *testing.T) {
		// Chunk
		cbuf := bytes.Buffer{}
		chunker := newChunker(&cbuf)
		chunked := []byte{}
		chunked = writeSmall(chunker, chunked)
		err := chunker.send()
		assertNoError(t, err)
		assertBuf(t, cbuf, chunked)

		// Dechunk
		dbuf := bytes.NewBuffer(chunked)
		dechunker := newDechunker(dbuf)
		receiveAndAssertMessage(t, dechunker, msgS)
	})

	ot.Run("Two small messages", func(t *testing.T) {
		cbuf := bytes.Buffer{}
		chunker := newChunker(&cbuf)
		chunked := []byte{}
		chunked = writeSmall(chunker, chunked)
		chunked = writeSmall(chunker, chunked)
		err := chunker.send()
		assertNoError(t, err)
		assertBuf(t, cbuf, chunked)

		// Dechunk
		dbuf := bytes.NewBuffer(chunked)
		dechunker := newDechunker(dbuf)
		receiveAndAssertMessage(t, dechunker, msgS)
		receiveAndAssertMessage(t, dechunker, msgS)
	})

	ot.Run("Large message", func(t *testing.T) {
		cbuf := bytes.Buffer{}
		chunker := newChunker(&cbuf)
		chunked := []byte{}
		chunked = writeLarge(chunker, chunked)
		chunker.send()
		assertBuf(t, cbuf, chunked)

		// Dechunk
		dbuf := bytes.NewBuffer(chunked)
		dechunker := newDechunker(dbuf)
		receiveAndAssertMessage(t, dechunker, msgL)
	})

	ot.Run("Small and large message", func(t *testing.T) {
		cbuf := bytes.Buffer{}
		chunker := newChunker(&cbuf)
		chunked := []byte{}
		chunked = writeSmall(chunker, chunked)
		chunked = writeLarge(chunker, chunked)
		chunker.send()
		assertBuf(t, cbuf, chunked)

		// Dechunk
		dbuf := bytes.NewBuffer(chunked)
		dechunker := newDechunker(dbuf)
		receiveAndAssertMessage(t, dechunker, msgS)
		receiveAndAssertMessage(t, dechunker, msgL)
	})
}
