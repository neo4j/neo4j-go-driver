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
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"sync"
)

// ClientCertificate holds paths to a TLS certificate file and its corresponding private key file.
// This struct is used to load client certificate-key pairs for use in mutual TLS.
type ClientCertificate struct {
	CertFile string  // Path to the TLS certificate file.
	KeyFile  string  // Path to the TLS private key file.
	Password *string // Optional password for decrypting the private key file. Nil indicates no password is set.
}

// ClientCertificateProvider defines an interface for retrieving a tls.Certificate.
// Implementations of this interface can provide static or dynamically updatable certificates.
type ClientCertificateProvider interface {
	// GetCertificate returns a tls.Certificate for use in TLS connections.
	// Implementations should ensure thread-safety and handle any necessary logic
	// to provide the most up-to-date certificate.
	//
	// If a nil value is returned, it indicates that no client certificate is available for use in the TLS connection.
	// This might be the case if the certificate is not yet available, or if no certificate is configured for use.
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
func NewStaticClientCertificateProvider(cert ClientCertificate) (*StaticClientCertificateProvider, error) {
	tlsCert, err := loadCertificate(cert.CertFile, cert.KeyFile, cert.Password)
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
// a provider that allows updating the certificate dynamically through its UpdateCertificate method.
func NewRotatingClientCertificateProvider(cert ClientCertificate) (*RotatingClientCertificateProvider, error) {
	tlsCert, err := loadCertificate(cert.CertFile, cert.KeyFile, cert.Password)
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
func (p *RotatingClientCertificateProvider) UpdateCertificate(cert ClientCertificate) error {
	tlsCert, err := loadCertificate(cert.CertFile, cert.KeyFile, cert.Password)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.certificate = &tlsCert
	return nil
}

// loadCertificate attempts to load a certificate and its corresponding private key
// from the given paths. If a password is provided, it will attempt to decrypt
// the private key assuming it is encrypted.
func loadCertificate(certFile string, keyFile string, password *string) (tls.Certificate, error) {
	// Load certificate PEM block
	certPEMBlock, err := os.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	// Load private key PEM block
	keyPEMBlock, err := os.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	// Decrypt the private key if a password is provided
	if password != nil {
		decodedKeyPEMBlock, _ := pem.Decode(keyPEMBlock)
		if decodedKeyPEMBlock == nil {
			return tls.Certificate{}, errors.New("failed to parse PEM block containing the key")
		}
		//lint:ignore SA1019 Using due to lack of stdlib alternatives; aware of RFC 1423's insecurity.
		decryptedDERBlock, decryptErr := x509.DecryptPEMBlock(decodedKeyPEMBlock, []byte(*password))
		if decryptErr != nil {
			return tls.Certificate{}, decryptErr
		}
		// Re-encode the decrypted block to PEM
		keyPEMBlock = pem.EncodeToMemory(&pem.Block{Type: decodedKeyPEMBlock.Type, Bytes: decryptedDERBlock})
	}
	// Create the tls.Certificate
	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}
