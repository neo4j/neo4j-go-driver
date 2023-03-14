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

// Package connector is responsible for connecting to a database server.
package connector

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"io"
	"net"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type Connector struct {
	SkipEncryption   bool
	SkipVerify       bool
	Auth             map[string]any
	Log              log.Logger
	RoutingContext   map[string]string
	Network          string
	Config           *config.Config
	SupplyConnection func(context.Context, string) (net.Conn, error)
}

func (c Connector) Connect(ctx context.Context, address string, boltLogger log.BoltLogger) (db.Connection, error) {
	if c.SupplyConnection == nil {
		c.SupplyConnection = c.createConnection
	}

	conn, err := c.SupplyConnection(ctx, address)
	if err != nil {
		return nil, err
	}

	notificationConfig := db.NotificationConfig{
		MinSev:  c.Config.NotificationsMinSeverity,
		DisCats: c.Config.NotificationsDisabledCategories,
	}

	// TLS not requested
	if c.SkipEncryption {
		connection, err := bolt.Connect(
			ctx,
			address,
			conn,
			c.Auth,
			c.Config.UserAgent,
			c.RoutingContext,
			c.Log,
			boltLogger,
			notificationConfig,
		)
		if err != nil {
			if connErr := conn.Close(); connErr != nil {
				c.Log.Warnf(log.Driver, address, "could not close underlying socket after Bolt handshake error")
			}
			return nil, err
		}
		return connection, nil
	}

	// TLS requested, continue with handshake
	serverName, _, err := net.SplitHostPort(address)
	if err != nil {
		conn.Close()
		return nil, err
	}
	tlsConn := tls.Client(conn, c.tlsConfig(serverName))
	err = tlsConn.HandshakeContext(ctx)
	if err != nil {
		if err == io.EOF {
			// Give a bit nicer error message
			err = errors.New("remote end closed the connection, check that TLS is enabled on the server")
		}
		conn.Close()
		return nil, &TlsError{inner: err}
	}
	connection, err := bolt.Connect(ctx,
		address,
		tlsConn,
		c.Auth,
		c.Config.UserAgent,
		c.RoutingContext,
		c.Log,
		boltLogger,
		notificationConfig,
	)
	if err != nil {
		if connErr := conn.Close(); connErr != nil {
			c.Log.Warnf(log.Driver, address, "could not close underlying socket after Bolt handshake error")
		}
		return nil, err
	}
	return connection, nil
}

func (c Connector) createConnection(ctx context.Context, address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: c.Config.SocketConnectTimeout}
	if !c.Config.SocketKeepalive {
		dialer.KeepAlive = -1 * time.Second // Turns keep-alive off
	}

	return dialer.DialContext(ctx, c.Network, address)
}

func (c Connector) tlsConfig(serverName string) *tls.Config {
	var config *tls.Config
	if c.Config.TlsConfig == nil {
		//lint:ignore SA1019 RootCAs is supported until 6.0
		config = &tls.Config{RootCAs: c.Config.RootCAs}
	} else {
		config = c.Config.TlsConfig
	}
	if config.MinVersion == 0 {
		config.MinVersion = tls.VersionTLS12
	}
	config.InsecureSkipVerify = c.SkipVerify
	config.ServerName = serverName
	return config
}

// TlsError encapsulates all errors related to TLS connection creation
// This is needed since the tls package does not provide a common error type
// à la net.Error, and a common type is needed to properly classify the error
// for Testkit
type TlsError struct {
	inner error
}

func (e *TlsError) Error() string {
	return e.inner.Error()
}
