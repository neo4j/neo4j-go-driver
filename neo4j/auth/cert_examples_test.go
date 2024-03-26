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

package auth_test

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"log"
)

func ExampleNewStaticClientCertificateProvider() {
	password := "thepassword1"
	provider, err := auth.NewStaticClientCertificateProvider(auth.ClientCertificate{
		CertFile: "path/to/cert.pem",
		KeyFile:  "path/to/key.pem",
		Password: &password,
	})
	if err != nil {
		log.Fatalf("Failed to load certificate: %v", err)
	}
	_, _ = neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "password", ""), func(config *config.Config) {
		config.ClientCertificateProvider = provider
	})
}

func ExampleNewRotatingClientCertificateProvider() {
	password := "thepassword1"
	provider, err := auth.NewRotatingClientCertificateProvider(auth.ClientCertificate{
		CertFile: "path/to/cert.pem",
		KeyFile:  "path/to/key.pem",
		Password: &password,
	})
	if err != nil {
		log.Fatalf("Failed to load certificate: %v", err)
	}
	_, _ = neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "password", ""), func(config *config.Config) {
		config.ClientCertificateProvider = provider
	})
	// Some time later we update the certificate
	err = provider.UpdateCertificate(auth.ClientCertificate{
		CertFile: "path/to/new_cert.pem",
		KeyFile:  "path/to/new_key.pem",
		Password: &password,
	})
	if err != nil {
		// Handle the error
		log.Fatalf("Failed to update certificate: %v", err)
	}
}
