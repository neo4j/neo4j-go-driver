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
	provider.UpdateCertificate(auth.ClientCertificate{
		CertFile: "path/to/new_cert.pem",
		KeyFile:  "path/to/new_key.pem",
		Password: &password,
	})
}
