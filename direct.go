package neo4j_go_driver

import (
	"net/url"
)

type DirectDriver struct {
	target     url.URL
	connection *Connection
}

func NewDirectDriver(target *url.URL) (*DirectDriver, error) {
	driver := DirectDriver{
		target:    *target,
	}
	return &driver, nil
}

func (driver *DirectDriver) acquire(mode AccessMode) (*Connection, error) {
	return driver.acquireDirect(driver.target.Host)
}

func (driver *DirectDriver) acquireDirect(address string) (*Connection, error) {
	// TODO: connection pool
	conn, err := Connect(driver.target.Host)
	if err != nil {
		return nil, err
	}
	return &conn, nil
}

func (driver *DirectDriver) Target() url.URL {
	return driver.target
}

func (driver *DirectDriver) Session(defaultAccessMode AccessMode) (*Session, error) {
	return newSession(driver, defaultAccessMode), nil
}

func (driver *DirectDriver) Close() error {
	// TODO
	return nil
}
