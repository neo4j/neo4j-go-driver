/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package neo4j

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/log"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

type connector struct {
	config *Config
	auth   map[string]interface{}
	log    log.Logger
	logId  string
}

var connectorid uint32

type tlsError struct {
	err error
}

func (e *tlsError) Error() string {
	return fmt.Sprintf("TLS error: %s", e.err)
}

type connectError struct {
	err error
}

func (e *connectError) Error() string {
	return fmt.Sprintf("Connection error: %s", e.err)
}

func newConnector(config *Config, auth map[string]interface{}, logger log.Logger) *connector {
	id := atomic.AddUint32(&connectorid, 1)
	c := &connector{
		config: config,
		auth:   auth,
		log:    logger,
		logId:  fmt.Sprintf("connector %d", id),
	}
	c.log.Infof(c.logId, "Created {TLS: %t, SkipVerify: %t, SkipVerifyHostName: %t, CustomCerts: %t }",
		config.Encrypted, config.TrustStrategy.skipVerify, config.TrustStrategy.skipVerifyHostname,
		len(config.TrustStrategy.certificates) > 0)
	return c
}

func (c *connector) wrapInTls(target string, rawConn net.Conn) (net.Conn, error) {
	trust := c.config.TrustStrategy
	conf := tls.Config{}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		// No port
		host = target
	}
	conf.ServerName = host

	if len(trust.certificates) > 0 {
		conf.RootCAs = x509.NewCertPool()
		for _, cert := range trust.certificates {
			conf.RootCAs.AddCert(cert)
		}
	}

	// Check trust config to configure TLS accordingly
	switch {
	// Skip verification of root CAs, chains and hostname
	case trust.skipVerify && trust.skipVerifyHostname:
		conf.InsecureSkipVerify = true
	// Skip verification of root CAs and chains
	case trust.skipVerify:
		conf.InsecureSkipVerify = true
		conf.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			// Duplicate part of Go TLS handshake as specified in TLS example.
			certs := make([]*x509.Certificate, len(rawCerts))
			for i, asn1Data := range rawCerts {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return errors.New("tls: failed to parse certificate from server: " + err.Error())
				}
				certs[i] = cert
			}
			return certs[0].VerifyHostname(host)
		}
	// Verify root CAs and chains but not the hostname
	case trust.skipVerifyHostname:
		conf.InsecureSkipVerify = true
		conf.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			// Duplicate part of Go TLS handshake as specified in TLS example.
			certs := make([]*x509.Certificate, len(rawCerts))
			for i, asn1Data := range rawCerts {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return errors.New("tls: failed to parse certificate from server: " + err.Error())
				}
				certs[i] = cert
			}

			opts := x509.VerifyOptions{
				Roots:         conf.RootCAs,
				DNSName:       "", // This is what disables the hostname check
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	}

	tlsConn := tls.Client(rawConn, &conf)
	err = tlsConn.Handshake()
	if err != nil {
		if err == io.EOF {
			err = errors.New("Remote end closed the connection, check that TLS is enabled on the server")
		}
		rawConn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func (c *connector) connect(target string) (pool.Connection, error) {
	keepAlive := 0 * time.Second // Implies net default TCP keep-alive value
	if !c.config.SocketKeepalive {
		keepAlive = -1 * time.Second // Turns keep-alive off
	}
	d := net.Dialer{
		Timeout:   c.config.SocketConnectTimeout,
		KeepAlive: keepAlive,
	}

	// Make a TCP connection
	conn, err := d.Dial("tcp", target)
	if err != nil {
		c.log.Error(c.logId, err)
		return nil, &connectError{err: err}
	}

	// Wrap connection in TLS if configured
	if c.config.Encrypted {
		conn, err = c.wrapInTls(target, conn)
		if err != nil {
			c.log.Error(c.logId, err)
			return nil, &tlsError{err: err}
		}
	}

	// Pass ownership of connection to bolt upon success
	boltConn, err := bolt.Connect(target, conn, c.auth, c.log)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return boltConn, nil
}
