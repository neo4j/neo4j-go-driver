package test_integration

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/utils"
)

var (
	V3_4_0 = utils.VersionOf("3.4.0")
)

func VersionOfDriver(driver neo4j.Driver) utils.Version {
	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return utils.VersionOf("0.0.0")
	}
	defer session.Close()

	result, err := session.Run("RETURN 1", nil)
	if err != nil {
		return utils.VersionOf("0.0.0")
	}

	summary, err := result.Consume()
	if err != nil {
		return utils.VersionOf("0.0.0")
	}

	return utils.VersionOf(summary.Server().Version())
}
