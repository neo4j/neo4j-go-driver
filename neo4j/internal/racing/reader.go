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

package racing

import (
	"context"
	"io"
	"time"
)

type RacingReader interface {
	Read(ctx context.Context, bytes []byte) (int, error)
	ReadFull(ctx context.Context, bytes []byte) (int, error)
	Close()
}

func NewRacingReader(reader io.Reader) RacingReader {
	return &racingReader{reader: reader, in: make(chan *ioResult, 1)}
}

type racingReader struct {
	reader io.Reader
	in     chan *ioResult
}

func (selfPtr *racingReader) Close() {
	close(selfPtr.in)
}

func (selfPtr *racingReader) Read(ctx context.Context, bytes []byte) (int, error) {
	return selfPtr.race(ctx, bytes, read)
}

func (selfPtr *racingReader) ReadFull(ctx context.Context, bytes []byte) (int, error) {
	return selfPtr.race(ctx, bytes, readFull)
}

func (selfPtr *racingReader) race(ctx context.Context, bytes []byte, readFn func(io.Reader, []byte) (int, error)) (int, error) {
	deadline, hasDeadline := ctx.Deadline()
	err := ctx.Err()
	switch {
	case !hasDeadline && err == nil:
		return readFn(selfPtr.reader, bytes)
	case deadline.Before(time.Now()) || err != nil:
		return 0, err
	}
	go func() {
		n, err := readFn(selfPtr.reader, bytes)
		selfPtr.in <- &ioResult{
			n:   n,
			err: err,
		}
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case result := <-selfPtr.in:
		return result.n, result.err
	}
}

func read(reader io.Reader, bytes []byte) (int, error) {
	return reader.Read(bytes)
}

func readFull(reader io.Reader, bytes []byte) (int, error) {
	return io.ReadFull(reader, bytes)
}
