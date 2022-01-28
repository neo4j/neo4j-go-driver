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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package racingio

import (
	"context"
	"io"
)

type RacingReader interface {
	Read(ctx context.Context, bytes []byte) (int, error)
	ReadFull(ctx context.Context, bytes []byte) (int, error)
}

func NewRacingReader(reader io.Reader) RacingReader {
	return &racingReader{reader: reader}
}

type racingReader struct {
	reader io.Reader
}

func (rr *racingReader) Read(ctx context.Context, bytes []byte) (int, error) {
	return rr.race(ctx, bytes, read)
}

func (rr *racingReader) ReadFull(ctx context.Context, bytes []byte) (int, error) {
	return rr.race(ctx, bytes, readFull)
}

func (rr *racingReader) race(ctx context.Context, bytes []byte, readFn func(io.Reader, []byte) (int, error)) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resultChan := make(chan *ioResult, 1)
	defer close(resultChan)
	go func() {
		n, err := readFn(rr.reader, bytes)
		defer func() {
			// When the read operation completes, the outer function may have returned already.
			// In that situation, the channel will have been closed and the result emission will crash.
			// Let's just swallow the panic that may happen and ignore it
			_ = recover()
		}()
		resultChan <- &ioResult{
			n:   n,
			err: err,
		}
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case result := <-resultChan:
		return result.n, result.err
	}
}

func read(reader io.Reader, bytes []byte) (int, error) {
	return reader.Read(bytes)
}

func readFull(reader io.Reader, bytes []byte) (int, error) {
	return io.ReadFull(reader, bytes)
}
