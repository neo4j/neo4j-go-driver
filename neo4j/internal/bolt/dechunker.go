/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bolt

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/errorutil"
	rio "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/racing"
)

// dechunkMessage takes a buffer to be reused and returns the reusable buffer
// (might have been reallocated to handle growth), the message buffer and
// error.
// Reads will race against the provided context ctx
// If the server provides the connection read timeout hint readTimeout, a new context will be created from that timeout
// and the user-provided context ctx before every read
func dechunkMessage(ctx context.Context, conn io.Reader, msgBuf []byte, readTimeout time.Duration) ([]byte, []byte, error) {

	sizeBuf := []byte{0x00, 0x00}
	off := 0

	// When the server set a read-timeout hint and the connection can carry a
	// socket deadline, bound each read with SetReadDeadline instead of allocating
	// a context.WithTimeout (plus timer and racing goroutine) per read. This is
	// only safe when the caller's context is not cancelable: a cancelable context
	// still needs the racing path to honor mid-read cancellation, so it keeps the
	// legacy per-read timeout context.
	deadliner, canSetDeadline := conn.(readDeadliner)
	useSocketDeadline := readTimeout >= 0 && canSetDeadline && ctx.Done() == nil

	// The racing reader (and its per-read goroutine) is only needed when a read
	// must be cancelable via context. The socket-deadline fast path reads
	// directly, avoiding a per-message reader allocation.
	var reader rio.RacingReader
	if !useSocketDeadline {
		reader = rio.NewRacingReader(conn)
	}

	readFull := func(buf []byte) error {
		if useSocketDeadline {
			_ = deadliner.SetReadDeadline(time.Now().Add(readTimeout))
			_, err := io.ReadFull(conn, buf)
			return err
		}
		updatedCtx, cancelFunc := newReadContext(ctx, readTimeout)
		_, err := reader.ReadFull(updatedCtx, buf)
		if cancelFunc != nil { // reading has been completed, time to release the context
			cancelFunc()
		}
		return err
	}

	for {
		if err := readFull(sizeBuf); err != nil {
			return msgBuf, nil, processReadError(err, ctx, readTimeout)
		}
		chunkSize := int(binary.BigEndian.Uint16(sizeBuf))
		if chunkSize == 0 {
			if off > 0 {
				if useSocketDeadline {
					// Clear the deadline so a later read on a reused connection is
					// not bounded by this now-stale deadline.
					_ = deadliner.SetReadDeadline(time.Time{})
				}
				return msgBuf, msgBuf[:off], nil
			}
			// Got a nop chunk
			continue
		}

		// Need to expand buffer
		if (off + chunkSize) > cap(msgBuf) {
			newMsgBuf := make([]byte, (off+chunkSize)+4096)
			copy(newMsgBuf, msgBuf)
			msgBuf = newMsgBuf
		}
		// Read the chunk into buffer
		if err := readFull(msgBuf[off:(off + chunkSize)]); err != nil {
			return msgBuf, nil, processReadError(err, ctx, readTimeout)
		}
		off += chunkSize
	}
}

// readDeadliner is implemented by connections that can bound a read with a
// socket deadline, allowing the read path to avoid a per-read timeout context.
type readDeadliner interface {
	SetReadDeadline(t time.Time) error
}

// newReadContext returns the context to use for a single racing read. When a
// read timeout is configured it derives a per-read timeout context; otherwise it
// returns the caller's context unchanged.
func newReadContext(ctx context.Context, readTimeout time.Duration) (context.Context, context.CancelFunc) {
	if readTimeout < 0 {
		return ctx, nil
	}
	return context.WithTimeout(ctx, readTimeout)
}

func processReadError(err error, ctx context.Context, readTimeout time.Duration) error {
	if errorutil.IsTimeoutError(err) {
		return &errorutil.ConnectionReadTimeout{
			UserContext: ctx,
			ReadTimeout: readTimeout,
			Err:         err,
		}
	}
	if err == context.Canceled {
		return &errorutil.ConnectionReadCanceled{
			Err: err,
		}
	}
	return err
}
