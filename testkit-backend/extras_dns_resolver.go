//go:build !internal_neo4j_testkit_no_dns_resolver

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

package main

import (
	"fmt"
	"net"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const extrasNameDns = "dns"

func init() {
	registerExtra(
		extrasNameDns,
		ExtrasRegisterEntry{
			newBackendExtraData: func() any {
				return make(map[string][]any)
			},
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"DomainNameResolutionCompleted": domainNameResolutionCompletedHandler,
			},
			extraNewDriverHandler: extrasDnsNewDriverHandler,
		},
	)
}

func extrasDnsGetBackendExtraData(backend *backend) map[string][]any {
	return getBackendExtraData(backend, extrasNameDns).(map[string][]any)
}

func domainNameResolutionCompletedHandler(backend *backend, data map[string]any) {
	requestId := data["requestId"].(string)
	addresses := data["addresses"].([]any)
	extrasDnsGetBackendExtraData(backend)[requestId] = addresses
}

func extrasDnsNewDriverHandler(b *backend, data map[string]any, driver neo4j.DriverWithContext) error {
	if data["domainNameResolverRegistered"] != nil && data["domainNameResolverRegistered"].(bool) {
		neo4j.RegisterDnsResolver(driver, b.extrasDnsResolverFunction())
	}
	return nil
}

func (b *backend) extrasDnsResolverFunction() func(address string) []string {
	return func(address string) []string {
		id := b.nextId()
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			b.writeError(fmt.Errorf(
				"couldn't parse address for custom DNS resulution (probably a bug in backend): %w", err,
			))
			return nil
		}
		b.writeResponse("DomainNameResolutionRequired", map[string]string{
			"id":   id,
			"name": host,
		})
		for b.process() {
			if addresses, ok := extrasDnsGetBackendExtraData(b)[id]; ok {
				delete(extrasDnsGetBackendExtraData(b), id)
				result := make([]string, len(addresses))
				for i, address := range addresses {
					result[i] = fmt.Sprintf("%s:%s", address, port)
				}
				return result
			}
		}
		return nil
	}
}
