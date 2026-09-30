//go:build unix || windows

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
	"crypto/tls"
	"errors"
	"syscall"
)

// Waiting bytes need the read to tell a NOOP chunk from a TLS close_notify.
func (c *socketConnection) peerAlive() bool {
	raw := c.Conn
	if tlsConn, ok := raw.(*tls.Conn); ok {
		raw = tlsConn.NetConn()
	}
	sc, ok := raw.(syscall.Conn)
	if !ok {
		return c.peerAliveByRead(peerProbeTimeout)
	}
	n, err := peek(sc)
	if errors.Is(err, errNothingWaiting) {
		return true
	}
	if err != nil || n == 0 {
		return false
	}
	return c.peerAliveByRead(peerClassifyTimeout)
}

// peek returns how many bytes are waiting without consuming them, 0 meaning the peer closed.
func peek(sc syscall.Conn) (n int, err error) {
	rawConn, err := sc.SyscallConn()
	if err != nil {
		return 0, err
	}
	var peekErr error
	err = rawConn.Read(func(fd uintptr) bool {
		n, peekErr = peekSocket(fd)
		return true
	})
	if err != nil {
		return 0, err
	}
	return n, peekErr
}
