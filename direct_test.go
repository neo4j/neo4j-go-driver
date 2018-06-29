package neo4j_go_driver

import (
	"neo4j-go-connector/pkg"
	"net/url"
)

func newDirectWithConnector(target string, connector neo4j.Connector) Driver {
	targetUrl, err := url.Parse(target)
	if err != nil {
		return nil
	}

	return &directDriver{
		target: *targetUrl,
		connector: connector,
	}
}
