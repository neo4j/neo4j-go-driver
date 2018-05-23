package neo4j_go_driver

import "net/url"

type RoutingDriver struct {
	target    url.URL
	connector Connection
}

func NewRoutingDriver(target *url.URL) (*RoutingDriver, error) {
	conn, err := Connect(target.Host)
	if err != nil {
		return nil, err
	}
	return &RoutingDriver{
		target:    *target,
		connector: conn,
	}, nil
}

func (driver *RoutingDriver) acquire(mode AccessMode) (*Connection, error) {
	return nil, nil
}

func (driver *RoutingDriver) acquireDirect(address string) (*Connection, error) {
	return nil, nil
}

func (driver *RoutingDriver) Target() url.URL {
	return driver.target
}

func (driver *RoutingDriver) Session(defaultAccessMode AccessMode) (*Session, error) {
	return newSession(driver, defaultAccessMode), nil
}

func (driver *RoutingDriver) Close() error {
	// TODO
	return nil
}
