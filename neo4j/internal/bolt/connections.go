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
	"io"
	"net"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/errorutil"
)

// DefaultReadBufferSize specifies the default size (in bytes) of the buffer used for reading data from the network connection.
const DefaultReadBufferSize = 8192

// pendingRead is one socket read waiting to be taken.
type pendingRead struct {
	bytes []byte
	err   error
}

// socketConnection reads ahead on its own goroutine, so a close by the remote end is seen while
// the connection is idle in the pool. wantRead returns the buffer to that goroutine, so a
// pending read and an outstanding read never coexist.
type socketConnection struct {
	net.Conn
	pending   chan pendingRead
	wantRead  chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func (c *socketConnection) Read(p []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	var read pendingRead
	select {
	case read = <-c.pending:
	case <-c.done:
		return 0, net.ErrClosed
	}
	n := copy(p, read.bytes)
	if n < len(read.bytes) {
		c.pending <- pendingRead{bytes: read.bytes[n:], err: read.err}
		return n, nil
	}
	c.wantRead <- struct{}{}
	return n, read.err
}

func (c *socketConnection) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return c.Conn.Close()
}

func (c *socketConnection) readAhead(bufferSize int) {
	buffer := make([]byte, bufferSize)
	for {
		select {
		case <-c.wantRead:
		case <-c.done:
			return
		}
		n, err := c.Conn.Read(buffer)
		c.pending <- pendingRead{bytes: buffer[:n], err: err}
	}
}

func peerAlive(conn io.ReadWriteCloser) bool {
	if c, ok := conn.(*socketConnection); ok {
		return c.peerAlive()
	}
	return true
}

func (c *socketConnection) peerAlive() bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case read := <-c.pending:
		c.pending <- read
		return read.err == nil
	default:
		return true
	}
}

func bufferedConnection(conn net.Conn, readBufferSize int) io.ReadWriteCloser {
	// Reading into a zero-length buffer never blocks.
	if readBufferSize <= 0 {
		readBufferSize = DefaultReadBufferSize
	}
	c := &socketConnection{
		Conn:     conn,
		pending:  make(chan pendingRead, 1),
		wantRead: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	c.wantRead <- struct{}{}
	go c.readAhead(readBufferSize)
	return c
}

type ConnectionErrorListener interface {
	OnNeo4jError(context.Context, idb.Connection, *db.Neo4jError) error
	OnIoError(context.Context, idb.Connection, error)
	OnDialError(context.Context, string, error)
}

func handleTerminatedContextError(err error, connection io.Closer) error {
	if !contextTerminatedErr(err) {
		return nil
	}
	closeErr := connection.Close()
	if closeErr == nil {
		return nil
	}
	return errorutil.CombineErrors(err, closeErr)
}

func contextTerminatedErr(err error) bool {
	switch err.(type) {
	case *errorutil.ConnectionWriteTimeout:
		return true
	case *errorutil.ConnectionReadTimeout:
		return true
	case *errorutil.ConnectionWriteCanceled:
		return true
	case *errorutil.ConnectionReadCanceled:
		return true
	}
	return false
}
