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

// ServerAddressResolver is an interface that defines a resolver function used by the routing driver to
// resolve the initial address used to create the driver.
type ServerAddressResolver interface {
	// Resolve resolves the given address to a set of other addresses. If the returned slice is empty, the driver
	// will continue using the original address.
	Resolve(address ServerAddress) []ServerAddress
}

type serverAddressResolverWrapper struct {
	actualResolver ServerAddressResolver
}

func newServerAddressUrl(hostname string, port string) *url.URL {
	if hostname == "" {
		return nil
	}

	hostAndPort := hostname
	if port != "" {
		hostAndPort = hostAndPort + ":" + port
	}

	return &url.URL{Host: hostAndPort}
}

// A helper function that generates a ServerAddress with provided hostname and port information.
func NewServerAddress(hostname string, port string) ServerAddress {
	return newServerAddressUrl(hostname, port)
}

func wrapAddressResolverOrNil(addressResolver ServerAddressResolver) gobolt.UrlAddressResolver {
	if addressResolver == nil {
		return nil
	}

	return &serverAddressResolverWrapper{actualResolver: addressResolver}
}

func (wrapper *serverAddressResolverWrapper) Resolve(address *url.URL) []*url.URL {
	var result []*url.URL

	for _, address := range wrapper.actualResolver.Resolve(address) {
		result = append(result, newServerAddressUrl(address.Hostname(), address.Port()))
	}

	return result
}
