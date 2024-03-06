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

// TODO: better docs
// TODO: Password is useless
type ClientCertificate struct {
	CertFile string // Path to the certificate file.
	KeyFile  string // Path to the private key file.
	Password string // Password for the private key file (optional).
}

// TODO: better docs
type ClientCertificateProvider interface {
	HasUpdate() bool
	GetCertificate() (*tls.Certificate, error)
}

// TODO: better docs StaticClientCertificateProvider returns a fixed certificate and does not support updates.
type StaticClientCertificateProvider struct {
	certificate *tls.Certificate
}

func NewStaticClientCertificateProvider(cert ClientCertificate) (*StaticClientCertificateProvider, error) {
	tlsCert, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	if err != nil {
		return nil, err
	}
	return &StaticClientCertificateProvider{certificate: &tlsCert}, nil
}

func (p *StaticClientCertificateProvider) HasUpdate() bool {
	return false
}

func (p *StaticClientCertificateProvider) GetCertificate() (*tls.Certificate, error) {
	return p.certificate, nil
}

// TODO: better docs RotatingClientCertificateProvider supports dynamic certificate updates.
type RotatingClientCertificateProvider struct {
	mu             sync.RWMutex
	certificate    *tls.Certificate
	updateRequired bool
}

func NewRotatingClientCertificateProvider(cert ClientCertificate) (*RotatingClientCertificateProvider, error) {
	tlsCert, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	if err != nil {
		return nil, err
	}
	return &RotatingClientCertificateProvider{certificate: &tlsCert, updateRequired: true}, nil
}

func (p *RotatingClientCertificateProvider) HasUpdate() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	update := p.updateRequired
	p.updateRequired = false
	return update
}

func (p *RotatingClientCertificateProvider) GetCertificate() (*tls.Certificate, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.certificate, nil
}

func (p *RotatingClientCertificateProvider) UpdateCertificate(cert ClientCertificate) error {
	tlsCert, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.certificate = &tlsCert
	p.updateRequired = true
	return nil
}
