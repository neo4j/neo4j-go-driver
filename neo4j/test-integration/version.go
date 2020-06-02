package test_integration

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/utils"
)

var (
	// V340 identifies server version 3.4.0
	V340 = utils.VersionOf("3.4.0")
	// V350 identifies server version 3.5.0
	V350 = utils.VersionOf("3.5.0")
)

func versionOfDriver(driver neo4j.Driver) utils.Version {
	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	result, err := session.Run("RETURN 1", nil)
	if err != nil {
		panic(err)
	}

	summary, err := result.Consume()
	if err != nil {
		panic(err)
	}

	return utils.VersionOf(summary.Server().Version())
}
