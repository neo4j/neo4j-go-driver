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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
	"math"
	"net/url"
	"time"
)

// Deprecated: please use config.Config directly. This alias will be removed in 6.0.
type Config = config.Config

// Deprecated: please use config.ServerAddressResolver directly. This alias will be removed in 6.0.
type ServerAddressResolver = config.ServerAddressResolver

// Deprecated: please use config.ServerAddress directly. This alias will be removed in 6.0.
type ServerAddress = config.ServerAddress

func defaultConfig() *Config {
	return &Config{
		AddressResolver:                 nil,
		MaxTransactionRetryTime:         30 * time.Second,
		MaxConnectionPoolSize:           100,
		MaxConnectionLifetime:           1 * time.Hour,
		ConnectionAcquisitionTimeout:    1 * time.Minute,
		SocketConnectTimeout:            5 * time.Second,
		SocketKeepalive:                 true,
		RootCAs:                         nil,
		UserAgent:                       UserAgent,
		FetchSize:                       FetchDefault,
		NotificationsMinSeverity:        notifications.DefaultLevel,
		NotificationsDisabledCategories: notifications.NotificationDisabledCategories{},
		TelemetryDisabled:               false,
	}
}

func validateAndNormaliseConfig(config *Config) error {
	// Max Transaction Retry Time
	if config.MaxTransactionRetryTime < 0 {
		return &UsageError{Message: "Maximum transaction retry time cannot be smaller than 0"}
	}

	// Max Connection Pool Size
	if config.MaxConnectionPoolSize == 0 {
		return &UsageError{Message: "Maximum connection pool cannot be 0"}
	}

	if config.MaxConnectionPoolSize < 0 {
		config.MaxConnectionPoolSize = math.MaxInt32
	}

	// Max Connection Lifetime
	if config.MaxConnectionLifetime <= 0 {
		config.MaxConnectionLifetime = 1<<63 - 1
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
