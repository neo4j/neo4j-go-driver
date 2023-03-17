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

// Package connector is responsible for connecting to a database server.
package connector

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

type Connector struct {
	SkipEncryption   bool
	SkipVerify       bool
	RootCAs          *x509.CertPool
	DialTimeout      time.Duration
	SocketKeepAlive  bool
	Auth             map[string]interface{}
	Log              log.Logger
	UserAgent        string
	RoutingContext   map[string]string
	Network          string
	SupplyConnection func(address string) (net.Conn, error)
}

type ConnectError struct {
	inner error
}

func (e *ConnectError) Error() string {
	return e.inner.Error()
}

type TlsError struct {
	inner error
}

func (e *TlsError) Error() string {
	return e.inner.Error()
}

func (c Connector) Connect(address string, boltLogger log.BoltLogger) (db.Connection, error) {
	if c.SupplyConnection == nil {
		c.SupplyConnection = c.createConnection
	}
	conn, err := c.SupplyConnection(address)
	if err != nil {
		return nil, &ConnectError{inner: err}
	}

	// TLS not requested
	if c.SkipEncryption {
		connection, err := bolt.Connect(address, conn, c.Auth, c.UserAgent, c.RoutingContext, c.Log, boltLogger)
		if err != nil {
			if connErr := conn.Close(); connErr != nil {
				c.Log.Warnf(log.Driver, "", "Could not close underlying socket after Bolt handshake error")
			}
			return nil, err
		}
		return connection, err
	}

	// TLS requested, continue with handshake
	serverName, _, err := net.SplitHostPort(address)
	if err != nil {
		conn.Close()
		return nil, err
	}
	config := tls.Config{
		InsecureSkipVerify: c.SkipVerify,
		RootCAs:            c.RootCAs,
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
	}
	tlsconn := tls.Client(conn, &config)
	err = tlsconn.Handshake()
	if err != nil {
		if err == io.EOF {
			// Give a bit nicer error message
			err = errors.New("Remote end closed the connection, check that TLS is enabled on the server")
		}
		conn.Close()
		return nil, &TlsError{inner: err}
	}
	connection, err := bolt.Connect(address, tlsconn, c.Auth, c.UserAgent, c.RoutingContext, c.Log, boltLogger)
	if err != nil {
		if connErr := conn.Close(); connErr != nil {
			c.Log.Warnf(log.Driver, "", "Could not close underlying socket after Bolt handshake error")
		}
		return nil, err
	}
	return connection, nil
}

func (c Connector) createConnection(address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: c.DialTimeout}
	if !c.SocketKeepAlive {
		dialer.KeepAlive = -1 * time.Second // Turns keep-alive off
	}
	return dialer.Dial(c.Network, address)
}
