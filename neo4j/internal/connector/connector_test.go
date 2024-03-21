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

package connector_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/connector"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

type noopErrorListener struct{}

func (n noopErrorListener) OnNeo4jError(context.Context, idb.Connection, *db.Neo4jError) error {
	return nil
}

func (n noopErrorListener) OnIoError(context.Context, idb.Connection, error) {}

func (n noopErrorListener) OnDialError(context.Context, string, error) {}

func TestConnect(outer *testing.T) {
	outer.Parallel()

	ctx := context.Background()

	outer.Run("closes connection if Bolt handshake does not reach agreement", func(t *testing.T) {
		clientConnection, server := setUp(t)
		go func() {
			server.acceptVersion(1, 0)
		}()
		connectionDelegate := &ConnDelegate{Delegate: clientConnection}
		connector := &connector.Connector{
			SupplyConnection: supplyThis(connectionDelegate),
			SkipEncryption:   true,
			Config:           &config.Config{},
		}

		connection, err := connector.Connect(ctx, "irrelevant", nil, noopErrorListener{}, nil)

		AssertNil(t, connection)
		AssertErrorMessageContains(t, err, "unsupported version 1.0")
		AssertTrue(t, connectionDelegate.Closed)
	})

	outer.Run("closes connection if Bolt handshake errors", func(t *testing.T) {
		clientConnection, server := setUp(t)
		go func() {
			server.failAcceptingVersion()
		}()
		connectionDelegate := &ConnDelegate{Delegate: clientConnection}
		connector := &connector.Connector{
			SupplyConnection: supplyThis(connectionDelegate),
			SkipEncryption:   true,
			Config:           &config.Config{},
		}

		connection, err := connector.Connect(ctx, "irrelevant", nil, noopErrorListener{}, nil)

		AssertNil(t, connection)
		AssertError(t, err)
		AssertTrue(t, connectionDelegate.Closed)
	})
}

type Provider struct {
	cert *tls.Certificate
}

func (p *Provider) GetCertificate() *tls.Certificate {
	return p.cert
}

func TestTlsConfig(t *testing.T) {
	cert1 := &tls.Certificate{
		Certificate: [][]byte{{1}},
	}
	cert2 := &tls.Certificate{
		Certificate: [][]byte{{2}},
	}

	provider := Provider{cert: cert2}

	connector := &connector.Connector{
		SkipVerify: true,
		Config: &config.Config{
			TlsConfig: &tls.Config{
				Certificates: []tls.Certificate{*cert1},
			},
			ClientCertificateProvider: &provider,
		},
	}

	configs := make([]*tls.Config, 10)
	for i := 0; i < len(configs); i++ {
		tlsConfig := connector.TlsConfig(fmt.Sprintf("foo%d", i))
		configs[i] = tlsConfig
	}
	for i, tlsConfig := range configs {
		AssertNotNil(t, tlsConfig)
		AssertTrue(t, tlsConfig.ServerName == fmt.Sprintf("foo%d", i))
		AssertTrue(t, len(tlsConfig.Certificates) == 2)
		AssertDeepEquals(t, tlsConfig.Certificates[0], *cert1)
		AssertDeepEquals(t, tlsConfig.Certificates[1], *cert2)
	}
}

func setUp(t *testing.T) (net.Conn, *boltHandshakeServer) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	address := listener.Addr()
	clientConnection, err := net.Dial(address.Network(), address.String())
	if err != nil {
		t.Fatalf("Dial error: %s", err)
	}
	t.Cleanup(func() {
		_ = clientConnection.Close()
	})
	serverConnection, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept error: %s", err)
	}
	t.Cleanup(func() {
		_ = serverConnection.Close()
	})
	handshakeServer := &boltHandshakeServer{t, serverConnection}
	return clientConnection, handshakeServer
}

func supplyThis(connection net.Conn) func(ctx context.Context, address string) (net.Conn, error) {
	return func(ctx context.Context, address string) (net.Conn, error) {
		return connection, nil
	}
}

type boltHandshakeServer struct {
	t    *testing.T
	conn net.Conn
}

func (server *boltHandshakeServer) waitForHandshake() []byte {
	handshake := make([]byte, 4*5)
	if _, err := io.ReadFull(server.conn, handshake); err != nil {
		server.t.Fatalf("Unable to read client versions: %s", err)
	}
	return handshake
}

func (server *boltHandshakeServer) acceptVersion(major, minor byte) {
	server.waitForHandshake()
	if _, err := server.conn.Write([]byte{0x00, 0x00, minor, major}); err != nil {
		panic(err)
	}
}

func (server *boltHandshakeServer) failAcceptingVersion() {
	_ = server.conn.Close()
}

type ConnDelegate struct {
	Closed   bool
	Delegate net.Conn
}

func (cd *ConnDelegate) Read(b []byte) (n int, err error) {
	return cd.Delegate.Read(b)
}

func (cd *ConnDelegate) Write(b []byte) (n int, err error) {
	return cd.Delegate.Write(b)
}

func (cd *ConnDelegate) Close() error {
	cd.Closed = true
	return cd.Delegate.Close()
}

func (cd *ConnDelegate) LocalAddr() net.Addr {
	return cd.Delegate.LocalAddr()
}

func (cd *ConnDelegate) RemoteAddr() net.Addr {
	return cd.Delegate.RemoteAddr()
}

func (cd *ConnDelegate) SetDeadline(t time.Time) error {
	return cd.Delegate.SetDeadline(t)
}

func (cd *ConnDelegate) SetReadDeadline(t time.Time) error {
	return cd.Delegate.SetReadDeadline(t)
}

func (cd *ConnDelegate) SetWriteDeadline(t time.Time) error {
	return cd.Delegate.SetWriteDeadline(t)
}
