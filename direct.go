package neo4j_go_driver

import (
	"net/url"
	"neo4j-go-connector/pkg"
)

type directDriver struct {
	target    url.URL
	connector neo4j.Connector
}

func newDirectDriver(target *url.URL, token AuthToken, config *Config) (*directDriver, error) {
	connector, err := neo4j.NewConnector(target.String(), token.tokens, nil)
	if err != nil {
		return nil, err
	}

	driver := directDriver{
		target:    *target,
		connector: connector,
	}
	return &driver, nil
}

func (driver *directDriver) Target() url.URL {
	return driver.target
}

func (driver *directDriver) Session() (*Session, error) {
	return driver.SessionWithAccessMode(AccessModeWrite)
}

func (driver *directDriver) SessionWithAccessMode(accessMode AccessMode) (*Session, error) {
	return driver.SessionWithAccessModeAndBookmark(accessMode)
}

func (driver *directDriver) SessionWithBookmark(bookmark string) (*Session, error) {
	return driver.SessionWithAccessModeAndBookmark(AccessModeWrite, bookmark)
}

func (driver *directDriver) SessionWithAccessModeAndBookmark(accessMode AccessMode, bookmarks ...string) (*Session, error) {
	return newSession(driver, accessMode, bookmarks), nil
}

func (driver *directDriver) Close() error {
	// TODO
	return nil
}

func (driver *directDriver) acquire(mode AccessMode) (neo4j.Connection, error) {
	pool, err := driver.connector.GetPool()
	if err != nil {
		return nil, err
	}

	return pool.Acquire()
}
