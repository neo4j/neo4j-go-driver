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

package auth

import (
	"crypto/tls"
	"sync"
)

// ClientCertificate holds paths to a TLS certificate file and its corresponding private key file.
// This struct is used to load certificate-key pairs for use in TLS connections.
type ClientCertificate struct {
	CertFile string // Path to the TLS certificate file.
	KeyFile  string // Path to the TLS private key file.
}

// ClientCertificateProvider defines an interface for retrieving a tls.Certificate.
// Implementations of this interface can provide static or dynamically updatable certificates.
type ClientCertificateProvider interface {
	// GetCertificate returns a tls.Certificate for use in TLS connections.
	// Implementations should ensure thread-safety and handle any necessary logic
	// to provide the most up-to-date certificate.
	GetCertificate() *tls.Certificate
}

// StaticClientCertificateProvider is an implementation of ClientCertificateProvider
// that provides a static, unchangeable tls.Certificate. It is intended for use cases
// where the certificate does not need to be updated over the lifetime of the application.
type StaticClientCertificateProvider struct {
	certificate *tls.Certificate
}

// NewStaticClientCertificateProvider creates a new StaticClientCertificateProvider given a ClientCertificate.
// This function loads the certificate-key pair specified in the ClientCertificate and returns
// a provider that will always return this loaded certificate.
//
// Example:
//
//	certProvider, err := auth.NewStaticClientCertificateProvider(auth.ClientCertificate{
//		CertFile: "path/to/cert.pem",
//		KeyFile: "path/to/key.pem"
//	})
//	if err != nil {
//	    log.Fatalf("Failed to load certificate: %v", err)
//	}
func NewStaticClientCertificateProvider(cert ClientCertificate) (*StaticClientCertificateProvider, error) {
	tlsCert, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	if err != nil {
		return nil, err
	}
	return &StaticClientCertificateProvider{certificate: &tlsCert}, nil
}

func (p *StaticClientCertificateProvider) GetCertificate() *tls.Certificate {
	return p.certificate
}

// RotatingClientCertificateProvider is an implementation of ClientCertificateProvider
// that supports dynamic updates to the tls.Certificate it provides. It is useful for scenarios
// where certificates need to be rotated or updated without restarting the application.
type RotatingClientCertificateProvider struct {
	mu          sync.RWMutex
	certificate *tls.Certificate
}

// NewRotatingClientCertificateProvider creates a new RotatingClientCertificateProvider given a ClientCertificate.
// This function loads the certificate-key pair specified in the ClientCertificate and returns
// a provider that allows updating the certificate dynamically through UpdateCertificate method.
//
// Example:
//
//	certProvider, err := auth.NewRotatingClientCertificateProvider(auth.ClientCertificate{
//		CertFile: "path/to/cert.pem",
//		KeyFile: "path/to/key.pem"
//	})
//	if err != nil {
//	    log.Fatalf("Failed to load certificate: %v", err)
//	}
func NewRotatingClientCertificateProvider(cert ClientCertificate) (*RotatingClientCertificateProvider, error) {
	tlsCert, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	if err != nil {
		return nil, err
	}
	return &RotatingClientCertificateProvider{certificate: &tlsCert}, nil
}

func (p *RotatingClientCertificateProvider) GetCertificate() *tls.Certificate {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.certificate
}

// UpdateCertificate updates the certificate stored in the provider with a new certificate specified
// by the ClientCertificate. This method allows dynamic updates to the certificate used in TLS connections,
// facilitating use cases such as certificate rotation.
//
// Example usage:
//
//	err := certProvider.UpdateCertificate(auth.ClientCertificate{
//		CertFile: "path/to/new_cert.pem",
//		KeyFile: "path/to/new_key.pem"
//	})
//	if err != nil {
//	    log.Fatalf("Failed to update certificate: %v", err)
//	}
func (p *RotatingClientCertificateProvider) UpdateCertificate(cert ClientCertificate) error {
	tlsCert, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.certificate = &tlsCert
	return nil
}
