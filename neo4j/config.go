/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
	"time"
)

// A Config contains options that can be used to customize certain
// aspects of the driver
type Config struct {
	// Whether to turn on/off TLS encryption (default: true)
	Encrypted bool
	// Sets how the driver establishes trust with the Neo4j instance
	// it is connected to.
	TrustStrategy TrustStrategy
	// Logging target the driver will send its log outputs
	Log Logging
	// Resolver that would be used to resolve initial router address. This may
	// be useful if you want to provide more than one URL for initial router.
	// If not specified, the provided bolt+routing URL is used as the initial
	// router.
	AddressResolver ServerAddressResolver
	// Maximum amount of duration a retriable operation would continue retrying
	MaxTransactionRetryDuration time.Duration
	// Maximum number of connections per URL to allow on this driver
	MaxConnectionPoolSize int
}

func defaultConfig() *Config {
	return &Config{
		Encrypted:                   true,
		TrustStrategy:               TrustAny(false),
		Log:                         NoOpLogger(),
		AddressResolver:             nil,
		MaxTransactionRetryDuration: 30 * time.Second,
		MaxConnectionPoolSize:       100,
	}
}

func validateConfig(config *Config) error {
	return nil
}
