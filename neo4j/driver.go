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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package neo4j provides required functionality to connect and execute statements against a Neo4j Database.
package neo4j

import (
	"net/url"
)

// AccessMode defines modes that routing driver decides to which cluster member
// a connection should be opened.
type AccessMode int

const (
	// AccessModeWrite tells the driver to use a connection to 'Leader'
	AccessModeWrite AccessMode = 0
	// AccessModeRead tells the driver to use a connection to one of the 'Follower' or 'Read Replica'.
	AccessModeRead AccessMode = 1
)

// Driver represents a pool(s) of connections to a neo4j server or cluster. It's
// safe for concurrent use.
type Driver interface {
	// The url this driver is bootstrapped
	Target() url.URL
	Session(accessMode AccessMode, bookmarks ...string) (Session, error)
	// Close the driver and all underlying connections
	Close() error
}

// NewDriver is the entry point to the neo4j driver to create an instance of a Driver. It is the first function to
// be called in order to establish a connection to a neo4j database. It requires a Bolt URI and an authentication
// token as parameters and can also take optional configuration function(s) as variadic parameters.
//
// In order to connect to a single instance database, you need to pass a URI with scheme 'bolt'
//	driver, err = NewDriver("bolt://db.server:7687", BasicAuth(username, password))
//
// In order to connect to a causal cluster database, you need to pass a URI with scheme 'bolt+routing' or 'neo4j'
// and its host part set to be one of the core cluster members. Please note that 'bolt+routing' scheme will be
// removed with 2.0 series of drivers.
//	driver, err = NewDriver("bolt+routing://core.db.server:7687", BasicAuth(username, password))
//
// You can override default configuration options by providing a configuration function(s)
//	driver, err = NewDriver(uri, BasicAuth(username, password), function (config *Config) {
// 		config.MaxConnectionPoolSize = 10
// 	})
func NewDriver(target string, auth AuthToken, configurers ...func(*Config)) (Driver, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}

	if parsed.Scheme != "bolt" && parsed.Scheme != "bolt+routing" && parsed.Scheme != "neo4j" {
		return nil, newDriverError("url scheme %s is not supported", parsed.Scheme)
	}

	if parsed.Scheme == "bolt" && len(parsed.RawQuery) > 0 {
		return nil, newDriverError("routing context is not supported for direct driver")
	}

	config := defaultConfig()
	for _, configurer := range configurers {
		configurer(config)
	}

	if err := validateAndNormaliseConfig(config); err != nil {
		return nil, err
	}

	return newGoboltDriver(parsed, auth, config)
}
