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
	"bufio"
	"context"
	"io"
	"net"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/errorutil"
)

// DefaultReadBufferSize specifies the default size (in bytes) of the buffer used for reading data from the network connection.
const DefaultReadBufferSize = 8192

const aliveRoutineBufferSize = 128

type readProbe struct {
	err error
	buf []byte
}

// socketConnection keeps the net.Conn reachable beneath the read buffer.
type socketConnection struct {
	net.Conn
	reader          *bufio.Reader // nil when read buffering is off
	readChan        chan readProbe
	readRequestChan chan struct{}
	closed          bool
}

func (c *socketConnection) Read(p []byte) (int, error) {
	//fmt.Println("socketConnection.Read: waiting for read probe", len(p), "bytes")
	probe, ok := <-c.readChan
	if !ok {
		return 0, io.EOF // TODO: find out right error to return when Read after Close
	}
	n := copy(p, probe.buf)
	//fmt.Println("socketConnection.Read: read", n, "bytes, err:", probe.err)
	//fmt.Println("socketConnection.Read: buff len", len(probe.buf), "remaining len", len(probe.buf[n:]))
	if n < len(probe.buf) {
		remaining := probe.buf[n:]
		//fmt.Println("socketConnection.Read: putting remaining", len(remaining), "bytes back to readChan")
		c.readChan <- readProbe{
			err: probe.err,
			buf: remaining,
		}
		probe.err = nil // defer error to later read
	} else if !c.closed {
		//fmt.Println("socketConnection.Read: requesting next read probe")
		c.readRequestChan <- struct{}{}
	}
	return n, probe.err
}

func (c *socketConnection) Close() error {
	c.closed = true
	close(c.readRequestChan)
	return c.Conn.Close()
}

func (c *socketConnection) aliveRoutine() {
	for {
		//fmt.Println("aliveRoutine: waiting for read request")
		select {
		case _, ok := <-c.readRequestChan:
			if !ok {
				close(c.readChan)
				return
			}
		}
		buffer := make([]byte, aliveRoutineBufferSize)
		var n int
		var err error
		//fmt.Println("aliveRoutine: reading from connection")
		if c.reader != nil {
			n, err = c.Conn.Read(buffer)
		} else {
			n, err = c.Conn.Read(buffer)
		}
		buffer = buffer[:n]
		//fmt.Println("aliveRoutine: read", n, "bytes, err:", err)
		c.readChan <- readProbe{
			err: err,
			buf: buffer,
		}
	}
}

func (c *socketConnection) peerAlive() bool {
	select {
	case probe, ok := <-c.readChan:
		if !ok {
			return false
		}
		res := probe.err == nil && len(probe.buf) > 0
		c.readChan <- probe
		return res
	default:
		return true
	}
}

func bufferedConnection(conn net.Conn, readBufferSize int) io.ReadWriteCloser {
	c := &socketConnection{
		Conn:            conn,
		readChan:        make(chan readProbe, 1),
		readRequestChan: make(chan struct{}, 1),
		closed:          false,
	}
	if readBufferSize > 0 {
		c.reader = bufio.NewReaderSize(conn, readBufferSize)
	}
	go c.aliveRoutine()
	c.readRequestChan <- struct{}{}
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
