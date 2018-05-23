package neo4j_go_driver

import (
	"net/url"
	"fmt"
)


type Driver interface {
	Target() url.URL
	Session(defaultAccessMode AccessMode) (*Session, error)
	Close() error
	acquire(mode AccessMode) (*Connection, error)
	acquireDirect(address string) (*Connection, error)
}

func GraphDatabaseDriver(target string) (Driver, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "bolt" {
		return NewDirectDriver(parsed)
	}
	if parsed.Scheme == "bolt+routing" {
		return NewRoutingDriver(parsed)
	}
	return nil, fmt.Errorf("URL scheme %s is not supported", parsed.Scheme)
}


type AccessMode int

const (
	WriteAccess AccessMode = iota
	ReadAccess             = iota
)


type Session struct {
	driver            Driver
	defaultAccessMode AccessMode
	conn              *Connection
}

func newSession(driver Driver, defaultAccessMode AccessMode) *Session {
	return &Session{
		driver:            driver,
		defaultAccessMode: defaultAccessMode,
	}
}

func (session *Session) Driver() Driver {
	return session.driver
}

func (session *Session) Run(cypher string) (*Result, error) {
	conn, err := session.driver.acquire(session.defaultAccessMode)
	if err != nil {
		return nil, err
	}
	session.conn = conn
	// TODO
	return &Result{}, nil
}

func (session *Session) Close() error {
	// TODO
	return nil
}


type Result struct {

}
