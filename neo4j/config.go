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
	"crypto/x509"
	"math"
	"net/url"
	"time"
)

// TrustStrategy defines how the driver will establish trust with the neo4j instance
type TrustStrategy struct {
	certificates       []*x509.Certificate
	skipVerify         bool
	skipVerifyHostname bool
}

// TrustAny returns a trust strategy which skips trust verification (trusts any certificate
// provided) and whose hostname verification can be enabled/disabled based on the provided
// parameter
func TrustAny(verifyHostname bool) TrustStrategy {
	return TrustStrategy{
		certificates:       nil,
		skipVerify:         true,
		skipVerifyHostname: !verifyHostname,
	}
}

// TrustSystem returns a trust strategy which uses system provided certificates for
// trust verification and whose hostname verification can be enabled/disabled based
// on the provided parameter
func TrustSystem(verifyHostname bool) TrustStrategy {
	return TrustStrategy{
		certificates:       nil,
		skipVerify:         false,
		skipVerifyHostname: !verifyHostname,
	}
}

// TrustOnly returns a trust strategy which uses provided certificates for trust
// verification and whose hostname verification can be enabled/disabled based
// on the provided parameter
func TrustOnly(verifyHostname bool, certs ...*x509.Certificate) TrustStrategy {
	return TrustStrategy{
		certificates:       certs,
		skipVerify:         false,
		skipVerifyHostname: !verifyHostname,
	}
}

// A Config contains options that can be used to customize certain
// aspects of the driver
type Config struct {
	// Whether to turn on/off TLS encryption.
	//
	// default: true
	Encrypted bool
	// Sets how the driver establishes trust with the Neo4j instance
	// it is connected to.
	//
	// default: TrustAny(false)
	TrustStrategy TrustStrategy
	// Logging target the driver will send its log outputs
	//
	// default: No Op Logger
	Log Logging
	// Resolver that would be used to resolve initial router address. This may
	// be useful if you want to provide more than one URL for initial router.
	// If not specified, the provided bolt+routing URL is used as the initial
	// router.
	//
	// default: nil
	AddressResolver ServerAddressResolver
	// Maximum amount of time a retriable operation would continue retrying. It
	// cannot be specified as a negative value.
	//
	// default: 30 * time.Second
	MaxTransactionRetryTime time.Duration
	// Maximum number of connections per URL to allow on this driver. It
	// cannot be specified as 0 and negative values are interpreted as
	// math.MaxInt32.
	//
	// default: 100
	MaxConnectionPoolSize int
	// Maximum connection life time on pooled connections. Values less than
	// or equal to 0 disables the lifetime check.
	//
	// default: 1 * time.Hour
	MaxConnectionLifetime time.Duration
	// Maximum amount of time to either acquire an idle connection from the pool
	// or create a new connection (when the pool is not full). Negative values
	// result in an infinite wait time where 0 value results in no timeout which
	// results in immediate failure when there are no available connections.
	//
	// default: 1 * time.Minute
	ConnectionAcquisitionTimeout time.Duration
	// Connect timeout that will be set on underlying sockets. Values less than
	// or equal to 0 results in no timeout being applied.
	//
	// default: 5 * time.Second
	SocketConnectTimeout time.Duration
	// Whether to enable TCP keep alive on underlying sockets.
	//
	// default: true
	SocketKeepalive bool
}

func defaultConfig() *Config {
	return &Config{
		Encrypted:                    true,
		TrustStrategy:                TrustAny(false),
		AddressResolver:              nil,
		MaxTransactionRetryTime:      30 * time.Second,
		MaxConnectionPoolSize:        100,
		MaxConnectionLifetime:        1 * time.Hour,
		ConnectionAcquisitionTimeout: 1 * time.Minute,
		SocketConnectTimeout:         5 * time.Second,
		SocketKeepalive:              true,
	}
}

func validateAndNormaliseConfig(config *Config) error {
	// Max Transaction Retry Time
	if config.MaxTransactionRetryTime < 0 {
		return newDriverError("maximum transaction retry time cannot be smaller than 0, but was %v", config.MaxTransactionRetryTime)
	}

	// Max Connection Pool Size
	if config.MaxConnectionPoolSize == 0 {
		return newDriverError("maximum connection pool size cannot be 0")
	}

	if config.MaxConnectionPoolSize < 0 {
		config.MaxConnectionPoolSize = math.MaxInt32
	}

	// Max Connection Lifetime
	if config.MaxConnectionLifetime < 0 {
		config.MaxConnectionLifetime = 0
	}

	// Connection Acquisition Timeout
	if config.ConnectionAcquisitionTimeout < 0 {
		config.ConnectionAcquisitionTimeout = -1
	}

	// Socket Connect Timeout
	if config.SocketConnectTimeout < 0 {
		config.SocketConnectTimeout = 0
	}

	return nil
}

// ServerAddress represents a host and port. Host can either be an IP address or a DNS name.
// Both IPv4 and IPv6 hosts are supported.
type ServerAddress interface {
	// Hostname returns the host portion of this ServerAddress.
	Hostname() string
	// Port returns the port portion of this ServerAddress.
	Port() string
}

// ServerAddressResolver is a function type that defines the resolver function used by the routing driver to
// resolve the initial address used to create the driver.
type ServerAddressResolver func(address ServerAddress) []ServerAddress

func newServerAddressURL(hostname string, port string) *url.URL {
	if hostname == "" {
		return nil
	}

	hostAndPort := hostname
	if port != "" {
		hostAndPort = hostAndPort + ":" + port
	}

	return &url.URL{Host: hostAndPort}
}

// NewServerAddress generates a ServerAddress with provided hostname and port information.
func NewServerAddress(hostname string, port string) ServerAddress {
	return newServerAddressURL(hostname, port)
}
