/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
	"context"
	"encoding/binary"
	"net"
	"reflect"
	"testing"
	"time"

	. "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func TestDechunker(t *testing.T) {
	var err error
	prevCap := 0
	var buf []byte
	messages := []struct {
		size uint32
		max  uint16
	}{
		{size: 3, max: 0xffff},
		{size: 1021, max: 0x7},
		{size: 0xffff78, max: 0x30},
		{size: 3, max: 0xffff},
		{size: 1021, max: 0xff90},
		{size: 0xffff78, max: 0xff90},
	}
	for msgi, msg := range messages {
		// Prepare message
		str := &bytes.Buffer{}
		total := msg.size
		this := uint16(0)

		b := byte(0)
		for total > 0 {
			// Write size
			if total > uint32(msg.max) {
				this = msg.max
			} else {
				this = uint16(total)
			}
			buf := []byte{0, 0}
			binary.BigEndian.PutUint16(buf, this)
			str.Write(buf)
			total -= uint32(this)

			// Write data
			buf = make([]byte, int(this))
			for i := range buf {
				buf[i] = b
				b++
			}
			str.Write(buf)
		}
		// Write end of message marker
		str.Write([]byte{0x00, 0x00})

		// Dechunk the message
		var msgBuf []byte
		serv, cli := net.Pipe()
		go func() {
			AssertWriteSucceeds(t, cli, str.Bytes())
		}()
		buf, msgBuf, err = dechunkMessage(context.Background(), serv, buf, -1)
		AssertNoError(t, err)
		AssertLen(t, msgBuf, int(msg.size))
		// Check content of buffer
		b = 0
		for i := range msgBuf {
			if msgBuf[i] != b {
				t.Errorf("Wrong content in buffer at %d, %d vs %d (message %d)", i, msgBuf[i], b, msgi)
				AssertNoError(t, serv.Close())
				AssertNoError(t, cli.Close())
				return
			}
			b++
		}

		// Check that buffer increases or stays put
		if cap(buf) < prevCap {
			t.Errorf("Underlying buffer should be reused")
		}
		prevCap = cap(buf)
		AssertNoError(t, serv.Close())
		AssertNoError(t, cli.Close())
	}
}

func TestDechunkerWithTimeout(ot *testing.T) {
	timeout := time.Millisecond * 600

	ot.Run("Resets connection deadline upon successful reads", func(t *testing.T) {
		serv, cli := net.Pipe()
		defer closePipe(ot, serv, cli)
		go func() {
			time.Sleep(timeout / 2)
			AssertWriteSucceeds(t, cli, []byte{0x00, 0x00})
			time.Sleep(2 * timeout / 3)
			AssertWriteSucceeds(t, cli, []byte{0x00, 0x02, 0xCA, 0xFE})
			time.Sleep(timeout / 2)
			AssertWriteSucceeds(t, cli, []byte{0x00, 0x00})
		}()
		buffer := make([]byte, 2)
		_, _, err := dechunkMessage(context.Background(), serv, buffer, timeout)
		AssertNoError(t, err)
		AssertTrue(t, reflect.DeepEqual(buffer, []byte{0xCA, 0xFE}))
	})

	ot.Run("Fails when connection deadline is reached", func(t *testing.T) {
		serv, cli := net.Pipe()
		defer closePipe(ot, serv, cli)

		_, _, err := dechunkMessage(context.Background(), serv, nil, timeout)

		AssertError(t, err)
		AssertStringContain(t, err.Error(), "context deadline exceeded")
	})

	ot.Run("Fails when connection deadline is reached via context", func(t *testing.T) {
		serv, cli := net.Pipe()
		defer closePipe(ot, serv, cli)
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()

		_, _, err := dechunkMessage(ctx, serv, nil, -1)

		AssertError(t, err)
		AssertStringContain(t, err.Error(), "context deadline exceeded")
	})

}

func closePipe(t *testing.T, srv, cli net.Conn) {
	AssertNoError(t, srv.Close())
	AssertNoError(t, cli.Close())
}
