/*
 * Copyright (c) "Neo4j"
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
	"net"
	"testing"

	. "github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
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
		// Packstream has direct access to this buffer
		c.beginMessage()
		c.buf = append(c.buf, msgS...)
		c.endMessage()
		// Construct expected
		out = append(out, 0x00, byte(len(msgS)))
		out = append(out, msgS...)
		out = append(out, 0x00, 0x00)
		return out
	}

	writeLarge := func(c *chunker, out []byte) []byte {
		// Packstream has direct access to this buffer
		c.beginMessage()
		c.buf = append(c.buf, msgL...)
		c.endMessage()
		// Construct expected
		out = append(out, 0xff, 0xff)
		out = append(out, msgN...)
		out = append(out, 0xff, 0xff)
		out = append(out, msgN...)
		out = append(out, 0x00, byte(len(msgS)))
		out = append(out, msgS...)
		out = append(out, 0x00, 0x00)
		return out
	}

	assertBuf := func(t *testing.T, buf *bytes.Buffer, exp []byte) {
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

	receiveAndAssertMessage := func(t *testing.T, conn net.Conn, expected []byte) {
		t.Helper()
		_, msg, err := dechunkMessage(conn, []byte{}, -1, nil, "", "")
		AssertNoError(t, err)
		assertSlices(t, msg, expected)
	}

	ot.Run("Small message", func(t *testing.T) {
		// Chunk
		cbuf := &bytes.Buffer{}
		chunker := newChunker()
		var chunked []byte
		chunked = writeSmall(&chunker, chunked)
		err := chunker.send(cbuf)
		AssertNoError(t, err)
		assertBuf(t, cbuf, chunked)

		// Dechunk
		serv, cli := net.Pipe()
		go func() {
			_, err = cli.Write(chunked)
			AssertNoError(t, err)
		}()
		receiveAndAssertMessage(t, serv, msgS)
		AssertNoError(t, serv.Close())
		AssertNoError(t, cli.Close())
	})

	ot.Run("Two small messages", func(t *testing.T) {
		// Chunk
		cbuf := &bytes.Buffer{}
		chunker := newChunker()
		var chunked []byte
		chunked = writeSmall(&chunker, chunked)
		chunked = writeSmall(&chunker, chunked)
		err := chunker.send(cbuf)
		AssertNoError(t, err)
		assertBuf(t, cbuf, chunked)

		// Dechunk
		serv, cli := net.Pipe()
		go func() {
			_, err = cli.Write(chunked)
			AssertNoError(t, err)
		}()
		receiveAndAssertMessage(t, serv, msgS)
		receiveAndAssertMessage(t, serv, msgS)
		AssertNoError(t, serv.Close())
		AssertNoError(t, cli.Close())
	})

	ot.Run("One large message", func(t *testing.T) {
		// Chunk
		cbuf := &bytes.Buffer{}
		chunker := newChunker()
		chunked := []byte{}
		chunked = writeLarge(&chunker, chunked)
		err := chunker.send(cbuf)
		AssertNoError(t, err)
		assertBuf(t, cbuf, chunked)

		// Dechunk
		serv, cli := net.Pipe()
		go func() {
			_, err = cli.Write(chunked)
			AssertNoError(t, err)
		}()
		receiveAndAssertMessage(t, serv, msgL)
		AssertNoError(t, serv.Close())
		AssertNoError(t, cli.Close())
	})

	ot.Run("Small and large message", func(t *testing.T) {
		// Chunk
		cbuf := &bytes.Buffer{}
		chunker := newChunker()
		chunked := []byte{}
		chunked = writeSmall(&chunker, chunked)
		chunked = writeLarge(&chunker, chunked)
		err := chunker.send(cbuf)
		AssertNoError(t, err)
		assertBuf(t, cbuf, chunked)

		// Dechunk
		serv, cli := net.Pipe()
		go func() {
			_, err = cli.Write(chunked)
			AssertNoError(t, err)
		}()
		receiveAndAssertMessage(t, serv, msgS)
		receiveAndAssertMessage(t, serv, msgL)
		AssertNoError(t, serv.Close())
		AssertNoError(t, cli.Close())
	})
}
