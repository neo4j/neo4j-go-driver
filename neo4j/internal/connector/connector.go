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

// Package connector is responsible for connecting to a database server.
package connector

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type Connector struct {
	SkipEncryption     bool
	SkipVerify         bool
	Log                log.Logger
	LogId              string
	RoutingContext     map[string]string
	Network            string
	Config             *config.Config
	SupplyConnection   func(context.Context, string) (net.Conn, error)
	TestKitDnsResolver func(string) []string
}

func (c *Connector) Connect(
	ctx context.Context,
	address string,
	auth *db.ReAuthToken,
	errorListener bolt.ConnectionErrorListener,
	boltLogger log.BoltLogger,
) (connection db.Connection, err error) {
	if c.SupplyConnection == nil {
		c.SupplyConnection = c.createConnection
	}

	conn, err := c.SupplyConnection(ctx, address)
	if err != nil {
		errorListener.OnDialError(ctx, address, err)
		return nil, err
	}

	defer func() {
		if err != nil && connection == nil {
			if err := conn.Close(); err != nil {
				c.Log.Warnf(log.Driver, address, "could not close socket after failed connection %s", err)
			}
		}
	}()

	notificationConfig := db.NotificationConfig{
		MinSev:  c.Config.NotificationsMinSeverity,
		DisCats: c.Config.NotificationsDisabledCategories,
		DisClas: c.Config.NotificationsDisabledClassifications,
	}

	// TLS not requested
	if c.SkipEncryption {
		connection, err := bolt.Connect(
			ctx,
			address,
			conn,
			auth,
			c.Config.UserAgent,
			c.RoutingContext,
			errorListener,
			c.Log,
			boltLogger,
			notificationConfig,
			c.Config.ReadBufferSize,
		)
		if err != nil {
			return nil, err
		}
		return connection, nil
	}

	// TLS requested, continue with handshake
	serverName, _, err := net.SplitHostPort(address)
	if err != nil {
		errorListener.OnDialError(ctx, address, err)
		return nil, err
	}
	tlsConfig := c.tlsConfig(serverName)
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.HandshakeContext(ctx)
	if err != nil {
		if err == io.EOF {
			// Give a bit nicer error message
			err = errors.New("remote end closed the connection, check that TLS is enabled on the server")
		}
		err = &errorutil.TlsError{Inner: err}
		errorListener.OnDialError(ctx, address, err)
		return nil, err
	}
	connection, err = bolt.Connect(
		ctx,
		address,
		tlsConn,
		auth,
		c.Config.UserAgent,
		c.RoutingContext,
		errorListener,
		c.Log,
		boltLogger,
		notificationConfig,
		c.Config.ReadBufferSize,
	)
	if err != nil {
		return nil, err
	}
	return
}

func (c Connector) createConnection(ctx context.Context, address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: c.Config.SocketConnectTimeout}
	if !c.Config.SocketKeepalive {
		dialer.KeepAlive = -1 * time.Second // Turns keep-alive off
	}

	if c.TestKitDnsResolver == nil {
		c.Log.Debugf(log.Driver, c.LogId, "dialing %s", address)
		return dialer.DialContext(ctx, c.Network, address)
	}

	addresses := c.TestKitDnsResolver(address)

	if len(addresses) == 0 {
		return nil, errors.New("TestKit DNS resolver returned no address")
	}

	var (
		err error
		con net.Conn
	)
	for _, address := range addresses {
		con, err = dialer.DialContext(ctx, c.Network, address)
		if err == nil {
			c.Log.Debugf(log.Driver, c.LogId, "dialing %s", address)
			return con, nil
		}
	}
	return nil, err
}

func (c Connector) tlsConfig(serverName string) *tls.Config {
	var config *tls.Config
	if c.Config.TlsConfig != nil {
		// Use Clone to safely copy the config without copying the mutex.
		config = c.Config.TlsConfig.Clone()
	} else {
		config = &tls.Config{
			// Use RootCAs from the connector's config.
			//lint:ignore SA1019 RootCAs is supported until 6.0
			RootCAs: c.Config.RootCAs,
			// It's safe to set MinVersion and other settings here since we're initializing a new config.
			MinVersion: tls.VersionTLS12,
		}
	}

	// Ensure MinVersion is set to at least TLS 1.2 if no version is specified.
	if config.MinVersion == 0 {
		config.MinVersion = tls.VersionTLS12
	}

	// Update the config with the client certificate, if provided.
	if c.Config.ClientCertificateProvider != nil && !c.SkipEncryption {
		cert := c.Config.ClientCertificateProvider.GetCertificate()
		if cert != nil {
			// Append the obtained certificate to the Certificates slice.
			config.Certificates = append(config.Certificates, *cert)
		}
	}

	// Configure server name and whether to skip certificate verification.
	config.ServerName = serverName
	config.InsecureSkipVerify = c.SkipVerify

	return config
}
