package test_integration

import (
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
		driver = server.Driver(func(config *neo4j.Config) {
			config.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
		})
		Expect(driver).NotTo(BeNil())
	})

	Context("from single-tenant Neo4j servers", func() {
		BeforeEach(func() {
			if isMultiTenant(server) {
				Skip(`Multi-tenant servers are covered in other tests`)
			}
		})

		It("does not include any database information", func() {
			session := driver.NewSession(neo4j.SessionConfig{Bookmarks: neo4j.BookmarksFromRawValues(bookmark)})
			defer assertCloses(session)
			result, err := session.Run("RETURN 42", noParams)
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
			session := driver.NewSession(neo4j.SessionConfig{DatabaseName: "system", BoltLogger: neo4j.ConsoleBoltLogger()})
			defer assertCloses(session)
			res, err := session.Run(fmt.Sprintf("CREATE DATABASE %s", extraDatabase), map[string]interface{}{})
			Expect(err).NotTo(HaveOccurred())
			_, err = res.Consume()  // consume result to obtain bookmark
			Expect(err).NotTo(HaveOccurred())
			bookmarks := neo4j.BookmarksToRawValues(session.LastBookmarks())
			Expect(len(bookmarks)).To(Equal(1))
			bookmark = bookmarks[0]
		})

		AfterEach(func() {
			session := driver.NewSession(neo4j.SessionConfig{DatabaseName: "system", Bookmarks: neo4j.BookmarksFromRawValues(bookmark)})
			defer assertCloses(session)
			_, err := session.Run(fmt.Sprintf("DROP DATABASE %s", extraDatabase), map[string]interface{}{})
			Expect(err).NotTo(HaveOccurred())
			bookmark = ""
		})

		It("includes the default database information", func() {
			session := driver.NewSession(neo4j.SessionConfig{Bookmarks: neo4j.BookmarksFromRawValues(bookmark)})
			defer assertCloses(session)
			result, err := session.Run("RETURN 42", noParams)
			Expect(err).NotTo(HaveOccurred())

			summary, err := result.Consume()
			Expect(err).NotTo(HaveOccurred())
			Expect(summary.Database().Name()).To(Equal("neo4j"))
		})

		It("includes the database information, based on session configuration", func() {
			session := driver.NewSession(neo4j.SessionConfig{DatabaseName: extraDatabase, Bookmarks: neo4j.BookmarksFromRawValues(bookmark)})
			defer assertCloses(session)
			result, err := session.Run("RETURN 42", noParams)
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
