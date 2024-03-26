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
	"testing"
)

var validPassword = "thepassword1"
var invalidPassword = "invalid_password"

type testCase struct {
	description string
	certFile    string
	keyFile     string
	password    *string
}

func TestNewStaticClientCertificateProvider(t *testing.T) {
	errorTestCases := []testCase{
		{
			description: "Invalid cert file path",
			certFile:    "invalid_path",
			keyFile:     "./testdata/test_key.pem",
		},
		{
			description: "Invalid key file path",
			certFile:    "./testdata/test_cert.pem",
			keyFile:     "invalid_path",
		},
		{
			description: "Incorrect password",
			certFile:    "./testdata/test_cert.pem",
			keyFile:     "./testdata/test_key_with_thepassword1.pem",
			password:    &invalidPassword,
		},
		{
			description: "Missing password",
			certFile:    "./testdata/test_cert.pem",
			keyFile:     "./testdata/test_key_with_thepassword1.pem",
		},
	}

	t.Run("Load certificate with password", func(t *testing.T) {
		provider, err := NewStaticClientCertificateProvider(ClientCertificate{
			CertFile: "./testdata/test_cert.pem",
			KeyFile:  "./testdata/test_key_with_thepassword1.pem",
			Password: &validPassword,
		})
		if err != nil {
			t.Fatalf("Expected no error for valid certificate and key, got %v", err)
		}
		if provider.GetCertificate() == nil {
			t.Error("Expected a non-nil certificate")
		}
	})

	t.Run("Load certificate with no password", func(t *testing.T) {
		provider, err := NewStaticClientCertificateProvider(ClientCertificate{
			CertFile: "./testdata/test_cert.pem",
			KeyFile:  "./testdata/test_key.pem",
		})
		if err != nil {
			t.Fatalf("Expected no error for valid certificate and key, got %v", err)
		}
		if provider.GetCertificate() == nil {
			t.Error("Expected a non-nil certificate")
		}
	})

	for _, testCase := range errorTestCases {
		t.Run(testCase.description, func(t *testing.T) {
			_, err := NewStaticClientCertificateProvider(ClientCertificate{
				CertFile: testCase.certFile,
				KeyFile:  testCase.keyFile,
				Password: testCase.password,
			})
			if err == nil {
				t.Errorf("Expected an error, got nil")
			}
		})
	}
}

func TestNewRotatingClientCertificateProvider(t *testing.T) {
	errorTestCases := []testCase{
		{
			description: "Invalid cert file path",
			certFile:    "invalid_path",
			keyFile:     "./testdata/test_key.pem",
		},
		{
			description: "Invalid key file path",
			certFile:    "./testdata/test_cert.pem",
			keyFile:     "invalid_path",
		},
		{
			description: "Incorrect password",
			certFile:    "./testdata/test_cert.pem",
			keyFile:     "./testdata/test_key_with_thepassword1.pem",
			password:    &invalidPassword,
		},
		{
			description: "Missing password",
			certFile:    "./testdata/test_cert.pem",
			keyFile:     "./testdata/test_key_with_thepassword1.pem",
		},
	}

	t.Run("Load certificate with password", func(t *testing.T) {
		provider, err := NewRotatingClientCertificateProvider(ClientCertificate{
			CertFile: "./testdata/test_cert.pem",
			KeyFile:  "./testdata/test_key_with_thepassword1.pem",
			Password: &validPassword,
		})
		if err != nil {
			t.Fatalf("Expected no error for valid certificate and key, got %v", err)
		}
		if provider.GetCertificate() == nil {
			t.Error("Expected a non-nil certificate")
		}
	})

	t.Run("Load certificate with no password", func(t *testing.T) {
		provider, err := NewRotatingClientCertificateProvider(ClientCertificate{
			CertFile: "./testdata/test_cert.pem",
			KeyFile:  "./testdata/test_key.pem",
		})
		if err != nil {
			t.Fatalf("Expected no error for valid certificate and key, got %v", err)
		}
		if provider.GetCertificate() == nil {
			t.Error("Expected a non-nil certificate")
		}
	})

	for _, testCase := range errorTestCases {
		t.Run(testCase.description, func(t *testing.T) {
			_, err := NewRotatingClientCertificateProvider(ClientCertificate{
				CertFile: testCase.certFile,
				KeyFile:  testCase.keyFile,
				Password: testCase.password,
			})
			if err == nil {
				t.Errorf("Expected an error, got nil")
			}
		})
	}
}

func TestRotatingClientCertificateProvider_UpdateCertificate(t *testing.T) {
	provider, err := NewRotatingClientCertificateProvider(ClientCertificate{
		CertFile: "./testdata/test_cert.pem",
		KeyFile:  "./testdata/test_key_with_thepassword1.pem",
		Password: &validPassword,
	})
	if err != nil {
		t.Fatalf("Expected no error for valid certificate and key, got %v", err)
	}
	initialCert := provider.GetCertificate()
	if initialCert == nil {
		t.Fatalf("Expected a non-nil certificate")
	}
	// Update certificate here with a no password example
	err = provider.UpdateCertificate(ClientCertificate{
		CertFile: "./testdata/test_cert.pem",
		KeyFile:  "./testdata/test_key.pem",
	})
	if err != nil {
		t.Fatalf("Failed to update certificate: %v", err)
	}
	updatedCert := provider.GetCertificate()
	if updatedCert == nil {
		t.Fatalf("Expected a non-nil certificate")
	}
	if updatedCert == initialCert {
		t.Error("Expected the updated certificate to be different from the initial one")
	}
}
