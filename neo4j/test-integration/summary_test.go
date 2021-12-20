package test_integration

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Result Summary", func() {

	const extraDatabase = "extra"

	var server dbserver.DbServer
	var driver neo4j.Driver
	var bookmark string
	noParams := map[string]interface{}{}

	BeforeEach(func() {
		server = dbserver.GetDbServer()
		driver = server.Driver()
		Expect(driver).NotTo(BeNil())
	})

	Context("from single-tenant Neo4j servers", func() {
		BeforeEach(func() {
			if isMultiTenant(server) {
				Skip(`Multi-tenant servers are covered in other tests`)
			}
		})

		It("does not include any database information", func() {
			session := driver.NewSession(neo4j.SessionConfig{Bookmarks: []string{bookmark}})
			defer assertCloses(session)
			result, err := session.Run(context.TODO(), "RETURN 42", noParams)
			Expect(err).NotTo(HaveOccurred())

			summary, err := result.Consume()
			Expect(err).NotTo(HaveOccurred())
			if server.Version.GreaterThanOrEqual(V4) {
				Expect(summary.Database().Name()).To(Equal("neo4j"))
			} else {
				Expect(summary.Database()).To(BeNil())
			}
		})
	})

	Context("from multi-tenant Neo4j servers", func() {
		BeforeEach(func() {
			if !isMultiTenant(server) {
				Skip("Multi-tenancy is a Neo4j 4+ feature")
			}
		})

		BeforeEach(func() {
			session := driver.NewSession(neo4j.SessionConfig{DatabaseName: "system"})
			defer assertCloses(session)
			_, err := session.Run(context.TODO(), fmt.Sprintf("CREATE DATABASE %s", extraDatabase), map[string]interface{}{})
			Expect(err).NotTo(HaveOccurred())
			bookmark = session.LastBookmark()
		})

		AfterEach(func() {
			session := driver.NewSession(neo4j.SessionConfig{DatabaseName: "system", Bookmarks: []string{bookmark}})
			defer assertCloses(session)
			_, err := session.Run(context.TODO(), fmt.Sprintf("DROP DATABASE %s", extraDatabase), map[string]interface{}{})
			Expect(err).NotTo(HaveOccurred())
			bookmark = ""
		})

		It("includes the default database information", func() {
			session := driver.NewSession(neo4j.SessionConfig{Bookmarks: []string{bookmark}})
			defer assertCloses(session)
			result, err := session.Run(context.TODO(), "RETURN 42", noParams)
			Expect(err).NotTo(HaveOccurred())

			summary, err := result.Consume()
			Expect(err).NotTo(HaveOccurred())
			Expect(summary.Database().Name()).To(Equal("neo4j"))
		})

		It("includes the database information, based on session configuration", func() {
			session := driver.NewSession(neo4j.SessionConfig{DatabaseName: extraDatabase, Bookmarks: []string{bookmark}})
			defer assertCloses(session)
			result, err := session.Run(context.TODO(), "RETURN 42", noParams)
			Expect(err).NotTo(HaveOccurred())

			summary, err := result.Consume()
			Expect(err).NotTo(HaveOccurred())
			Expect(summary.Database().Name()).To(Equal(extraDatabase))
		})
	})

})

func isMultiTenant(server dbserver.DbServer) bool {
	return server.Version.GreaterThanOrEqual(V4) && server.IsEnterprise
}

func assertCloses(closer io.Closer) {
	err := closer.Close()
	Expect(err).NotTo(HaveOccurred())
}
