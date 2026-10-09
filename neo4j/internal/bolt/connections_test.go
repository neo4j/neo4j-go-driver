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
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	. "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	AssertNoError(t, err)
	defer listener.Close()
	client, err = net.Dial("tcp", listener.Addr().String())
	AssertNoError(t, err)
	server, err = listener.Accept()
	AssertNoError(t, err)
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func tlsPair(t *testing.T) (client, server *tls.Conn) {
	t.Helper()
	cert, err := tls.LoadX509KeyPair("../../auth/testdata/test_cert.pem", "../../auth/testdata/test_key.pem")
	AssertNoError(t, err)
	rawClient, rawServer := tcpPair(t)
	server = tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}})
	client = tls.Client(rawClient, &tls.Config{InsecureSkipVerify: true})
	handshake := make(chan error, 1)
	go func() { handshake <- server.Handshake() }()
	AssertNoError(t, client.Handshake())
	AssertNoError(t, <-handshake)
	return client, server
}

func socket(conn net.Conn) *socketConnection {
	return bufferedConnection(conn, DefaultReadBufferSize).(*socketConnection)
}

func await(t *testing.T, want bool, probe func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for probe() != want {
		if time.Now().After(deadline) {
			t.Fatalf("probe never returned %v", want)
		}
		time.Sleep(time.Millisecond)
	}
}

// pollAlive checks across the moment the read-ahead delivers, catching a check that consumed it.
func pollAlive(t *testing.T, c *socketConnection) {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		AssertTrue(t, c.peerAlive())
	}
}

func assertReads(t *testing.T, r io.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(r, got)
		read <- err
	}()
	select {
	case err := <-read:
		AssertNoError(t, err)
		AssertStringEqual(t, string(got), want)
	case <-time.After(5 * time.Second):
		t.Fatal("read blocked")
	}
}

// scriptedConn hands out read outcomes a real socket cannot be made to produce.
type scriptedConn struct {
	net.Conn
	reads     []pendingRead
	delivered chan struct{}
	blocked   chan struct{}
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if len(c.reads) == 0 {
		<-c.blocked
		return 0, io.EOF
	}
	read := c.reads[0]
	c.reads = c.reads[1:]
	n := copy(p, read.bytes)
	if len(c.reads) == 0 {
		close(c.delivered)
	}
	return n, read.err
}

func awaitDelivered(t *testing.T, fake *scriptedConn) {
	t.Helper()
	select {
	case <-fake.delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("the read-ahead never ran")
	}
}

func scripted(t *testing.T, reads ...pendingRead) (*socketConnection, *scriptedConn) {
	t.Helper()
	client, _ := net.Pipe()
	fake := &scriptedConn{Conn: client, reads: reads, delivered: make(chan struct{}), blocked: make(chan struct{})}
	c := bufferedConnection(fake, DefaultReadBufferSize).(*socketConnection)
	t.Cleanup(func() {
		close(fake.blocked)
		c.Close()
	})
	return c, fake
}

func TestSocketConnectionPeerAlive(outer *testing.T) {
	outer.Run("open connection answers without waiting", func(t *testing.T) {
		client, _ := tcpPair(t)
		c := socket(client)
		start := time.Now()
		AssertTrue(t, c.peerAlive())
		AssertTrue(t, time.Since(start) < time.Second)
	})

	outer.Run("closed by peer", func(t *testing.T) {
		client, server := tcpPair(t)
		c := socket(client)
		AssertNoError(t, server.Close())
		await(t, false, c.peerAlive)
	})

	outer.Run("closed locally", func(t *testing.T) {
		client, _ := tcpPair(t)
		c := socket(client)
		AssertNoError(t, c.Close())
		AssertFalse(t, c.peerAlive())
	})

	outer.Run("waiting bytes stay in the stream", func(t *testing.T) {
		client, server := tcpPair(t)
		c := socket(client)
		AssertWriteSucceeds(t, server, []byte{0, 0})
		pollAlive(t, c)
		assertReads(t, c, "\x00\x00")
	})

	outer.Run("a transport that is not a socket is taken as alive", func(t *testing.T) {
		client, _ := net.Pipe()
		defer client.Close()
		AssertTrue(t, peerAlive(client))
	})
}

func TestSocketConnectionPeerAliveOverTls(outer *testing.T) {
	outer.Run("closed with close_notify", func(t *testing.T) {
		client, server := tlsPair(t)
		c := socket(client)
		AssertNoError(t, server.Close())
		await(t, false, c.peerAlive)
	})

	outer.Run("closed without close_notify", func(t *testing.T) {
		client, server := tlsPair(t)
		c := socket(client)
		AssertNoError(t, server.NetConn().Close())
		await(t, false, c.peerAlive)
	})

	outer.Run("waiting bytes stay in the stream", func(t *testing.T) {
		client, server := tlsPair(t)
		c := socket(client)
		AssertWriteSucceeds(t, server, []byte("record"))
		pollAlive(t, c)
		assertReads(t, c, "record")
	})
}

func TestIsPeerAlive(t *testing.T) {
	client, server := tcpPair(t)
	conn := NewBolt6("server", bufferedConnection(client, DefaultReadBufferSize), nil, logger, nil)
	AssertTrue(t, conn.IsPeerAlive())
	AssertNoError(t, server.Close())
	await(t, false, conn.IsPeerAlive)
}

func TestSocketConnectionRead(outer *testing.T) {
	// The read-ahead must not reuse the buffer while bytes are still pending.
	outer.Run("streams in order when the caller takes less than a buffer", func(t *testing.T) {
		client, server := tcpPair(t)
		c := bufferedConnection(client, 64)
		payload := make([]byte, 16*1024)
		for i := range payload {
			payload[i] = byte(i)
		}
		go server.Write(payload)
		got := make([]byte, 0, len(payload))
		chunk := make([]byte, 7)
		for len(got) < len(payload) {
			n, err := c.Read(chunk)
			AssertNoError(t, err)
			got = append(got, chunk[:n]...)
		}
		AssertTrue(t, bytes.Equal(got, payload))
	})

	outer.Run("keeps the bytes the caller did not take", func(t *testing.T) {
		client, server := tcpPair(t)
		c := socket(client)
		AssertWriteSucceeds(t, server, []byte("first second"))
		assertReads(t, c, "first ")
		assertReads(t, c, "second")
	})

	outer.Run("holds back an error until the bytes it came with are taken", func(t *testing.T) {
		c, _ := scripted(t, pendingRead{bytes: []byte("last"), err: io.EOF})
		got := make([]byte, 2)
		n, err := c.Read(got)
		AssertIntEqual(t, n, 2)
		AssertNoError(t, err)
		AssertStringEqual(t, string(got), "la")
		n, err = c.Read(got)
		AssertIntEqual(t, n, 2)
		AssertStringEqual(t, string(got), "st")
		AssertError(t, err)
	})

	outer.Run("read after close errors even when bytes were waiting", func(t *testing.T) {
		// Asserting it always errors takes more than one attempt.
		for i := 0; i < 50; i++ {
			c, fake := scripted(t, pendingRead{bytes: []byte("xy")})
			awaitDelivered(t, fake)
			AssertNoError(t, c.Close())
			n, err := c.Read(make([]byte, 8))
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("read %d bytes and got %v, want %v", n, err, net.ErrClosed)
			}
			AssertIntEqual(t, n, 0)
		}
	})
}

func TestSocketConnectionEmptyRead(t *testing.T) {
	// A read with no bytes and no error says nothing about the peer.
	c, fake := scripted(t, pendingRead{})
	awaitDelivered(t, fake)
	pollAlive(t, c)
}

func TestSocketConnectionReadBufferSize(outer *testing.T) {
	for name, size := range map[string]int{"zero": 0, "negative": -1} {
		outer.Run(name+" falls back to the default", func(t *testing.T) {
			client, server := tcpPair(t)
			c := bufferedConnection(client, size)
			go server.Write(bytes.Repeat([]byte("x"), DefaultReadBufferSize*2))
			n := readOnce(t, c, DefaultReadBufferSize*2)
			AssertTrue(t, n > 0)
			AssertTrue(t, n <= DefaultReadBufferSize)
		})
	}

	outer.Run("a size below the default is honoured", func(t *testing.T) {
		client, server := tcpPair(t)
		c := bufferedConnection(client, 16)
		go server.Write(bytes.Repeat([]byte("x"), 1024))
		n := readOnce(t, c, 1024)
		AssertTrue(t, n > 0)
		AssertTrue(t, n <= 16)
	})
}

func readOnce(t *testing.T, r io.Reader, size int) int {
	t.Helper()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := r.Read(make([]byte, size))
		done <- result{n, err}
	}()
	select {
	case got := <-done:
		AssertNoError(t, got.err)
		return got.n
	case <-time.After(5 * time.Second):
		t.Fatal("read blocked")
		return 0
	}
}

func TestSocketConnectionClose(outer *testing.T) {
	outer.Run("twice", func(t *testing.T) {
		client, _ := tcpPair(t)
		c := socket(client)
		AssertNoError(t, c.Close())
		_ = c.Close()
	})

	outer.Run("unblocks a waiting read", func(t *testing.T) {
		client, _ := tcpPair(t)
		c := socket(client)
		done := make(chan error, 1)
		go func() {
			_, err := c.Read(make([]byte, 1))
			done <- err
		}()
		time.Sleep(50 * time.Millisecond)
		AssertNoError(t, c.Close())
		select {
		case err := <-done:
			AssertError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("read still blocked after close")
		}
	})

	// Half have bytes waiting, so both goroutine exits are covered.
	outer.Run("ends the read-ahead goroutine", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		AssertNoError(t, err)
		defer listener.Close()
		accepted := make(chan net.Conn, 64)
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				accepted <- conn
			}
		}()

		before := settledGoroutines()
		const connections = 50
		open := make([]io.Closer, 0, connections)
		servers := make([]net.Conn, 0, connections)
		for i := 0; i < connections; i++ {
			client, err := net.Dial("tcp", listener.Addr().String())
			AssertNoError(t, err)
			open = append(open, bufferedConnection(client, DefaultReadBufferSize))
			server := <-accepted
			servers = append(servers, server)
			if i%2 == 0 {
				AssertWriteSucceeds(t, server, []byte("waiting"))
			}
		}
		AssertIntEqual(t, settledGoroutines()-before, connections)

		for _, c := range open {
			AssertNoError(t, c.Close())
		}
		for _, server := range servers {
			server.Close()
		}
		await(t, true, func() bool { return runtime.NumGoroutine() <= before+1 })
	})
}

func settledGoroutines() int {
	for i := 0; i < 20; i++ {
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}
