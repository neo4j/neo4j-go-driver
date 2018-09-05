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
	"fmt"
	"net/url"

	"github.com/neo4j-drivers/gobolt"
)

// AccessMode defines modes that routing driver decides to which cluster member
// a connection should be opened.
type AccessMode int

const (
	// AccessModeWrite tells the driver to use a connection to 'Leader'
	AccessModeWrite AccessMode = 0
	// AccessModeRead tells the driver to use a connection to one of the 'Follower' or 'Read Replica'.
	AccessModeRead = 1
)

// Driver represents a pool(s) of connections to a neo4j server or cluster. It's
// safe for concurrent use.
type Driver interface {
	// The url this driver is bootstrapped
	Target() url.URL
	Session(accessMode AccessMode, bookmarks ...string) (Session, error)
	// Close the driver and all underlying connections
	Close() error

	configuration() *Config
	acquire(mode AccessMode) (gobolt.Connection, error)
}

// NewDriver is the entry method to the neo4j driver to create an instance of a Driver
func NewDriver(target string, auth AuthToken, configurers ...func(*Config)) (Driver, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}

	if parsed.Scheme != "bolt" && parsed.Scheme != "bolt+routing" {
		return nil, fmt.Errorf("url scheme %s is not supported", parsed.Scheme)
	}

	if parsed.Scheme == "bolt" && len(parsed.RawQuery) > 0 {
		return nil, fmt.Errorf("routing context is not supported for direct driver")
	}

	config := defaultConfig()
	for _, configurer := range configurers {
		configurer(config)
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return newGoboltDriver(parsed, auth, config)
}
