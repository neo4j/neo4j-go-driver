//go:build !internal_neo4j_testkit_no_mtls

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
	"crypto/tls"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

const extrasNameMTLS = "mTLS"

func init() {
	registerExtra(
		extrasNameMTLS,
		ExtrasRegisterEntry{
			newBackendExtraData: func() any {
				return extrasMTLSExtraData{
					clientCertificateProviders: make(map[string]auth.ClientCertificateProvider),
					resolvedClientCertificates: make(map[string]auth.ClientCertificate),
				}
			},
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"NewClientCertificateProvider":       newClientCertificateProviderHandler,
				"ClientCertificateProviderClose":     clientCertificateProviderCloseHandler,
				"ClientCertificateProviderCompleted": clientCertificateProviderCompletedHandler,
			},
			extraDriverConfigurer: extrasMTLSDriverConfig,
		},
	)
}

type extrasMTLSExtraData struct {
	clientCertificateProviders map[string]auth.ClientCertificateProvider
	resolvedClientCertificates map[string]auth.ClientCertificate
}

func extrasMTLSGetBackendExtraData(backend *backend) extrasMTLSExtraData {
	return getBackendExtraData(backend, extrasNameMTLS).(extrasMTLSExtraData)
}

func extrasMTLSDriverConfig(backend *backend, data map[string]any, config *config.Config) error {
	clientCertificateProviderId := data["clientCertificateProviderId"]
	extraData := extrasMTLSGetBackendExtraData(backend)
	if clientCertificateProviderId != nil {
		provider := extraData.clientCertificateProviders[clientCertificateProviderId.(string)]
		config.ClientCertificateProvider = provider
	} else {
		if data["clientCertificate"] != nil {
			clientCertificate := backend.extrasMTLSToClientCertificate(data)
			provider, err := auth.NewStaticClientCertificateProvider(clientCertificate)
			if err != nil {
				return err
			}
			config.ClientCertificateProvider = provider
		}
	}
	return nil
}

func (b *backend) extrasMTLSToClientCertificate(data map[string]any) auth.ClientCertificate {
	clientCertificateData := data["clientCertificate"].(map[string]any)["data"].(map[string]any)
	return auth.ClientCertificate{
		CertFile: clientCertificateData["certfile"].(string),
		KeyFile:  clientCertificateData["keyfile"].(string),
		Password: b.toStringPointer(clientCertificateData["password"]),
	}
}

func newClientCertificateProviderHandler(backend *backend, data map[string]any) {
	provider := NewTestKitClientCertificateProvider(backend.nextId(), backend)
	extraData := extrasMTLSGetBackendExtraData(backend)
	extraData.clientCertificateProviders[provider.id] = TestKitClientCertificateProvider{id: provider.id, backend: backend}
	backend.writeResponse("ClientCertificateProvider", map[string]any{"id": provider.id})
}

func clientCertificateProviderCloseHandler(backend *backend, data map[string]any) {
	providerId := data["id"].(string)
	extraData := extrasMTLSGetBackendExtraData(backend)
	delete(extraData.clientCertificateProviders, providerId)
	backend.writeResponse("ClientCertificateProvider", map[string]any{"id": providerId})
}

func clientCertificateProviderCompletedHandler(backend *backend, data map[string]any) {
	requestId := data["requestId"].(string)
	extraData := extrasMTLSGetBackendExtraData(backend)
	if data["clientCertificate"] != nil {
		clientCertificate := backend.extrasMTLSToClientCertificate(data)
		extraData.resolvedClientCertificates[requestId] = clientCertificate
	} else {
		extraData.resolvedClientCertificates[requestId] = auth.ClientCertificate{}
	}
}

type TestKitClientCertificateProvider struct {
	id      string
	backend *backend
}

func NewTestKitClientCertificateProvider(id string, backend *backend) *TestKitClientCertificateProvider {
	return &TestKitClientCertificateProvider{
		id:      id,
		backend: backend,
	}
}

func (p TestKitClientCertificateProvider) GetCertificate() *tls.Certificate {
	requestId := p.backend.nextId()
	p.backend.writeResponse("ClientCertificateProviderRequest", map[string]any{
		"id":                          requestId,
		"clientCertificateProviderId": p.id,
	})
	for {
		p.backend.process()
		extraData := extrasMTLSGetBackendExtraData(p.backend)
		if clientCertificate, ok := extraData.resolvedClientCertificates[requestId]; ok {
			delete(extraData.resolvedClientCertificates, requestId)

			provider, err := auth.NewStaticClientCertificateProvider(clientCertificate)
			if err != nil {
				panic(fmt.Sprintf("Unable to create provider for client certificate: %v : %s", clientCertificate, err))
			}
			return provider.GetCertificate()
		}
	}
}
