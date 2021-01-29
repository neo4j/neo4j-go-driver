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

package neo4j

import (
	"net/url"

	"github.com/neo4j-drivers/gobolt"
)

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

func wrapAddressResolverOrNil(addressResolver ServerAddressResolver) gobolt.URLAddressResolver {
	if addressResolver == nil {
		return nil
	}

	return func(address *url.URL) []*url.URL {
		var result []*url.URL

		for _, address := range addressResolver(address) {
			result = append(result, newServerAddressURL(address.Hostname(), address.Port()))
		}

		return result
	}
}
