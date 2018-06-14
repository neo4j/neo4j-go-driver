package neo4j_go_driver

import (
	"net/url"
	"fmt"
	"neo4j-go-connector/pkg"
)

// AccessMode defines modes that routing driver decides to which cluster member
// a connection should be opened.
type AccessMode int

const (
	// Write Access Mode tells the driver to use a connection to 'Leader'
	AccessModeWrite AccessMode = 0
	// Read Access Mode tells the driver to use a connection to one of
	// the 'Follower' or 'Read Replica'.
	AccessModeRead = 1
)

// Driver represents a pool(s) of connections to a neo4j server or cluster. It's
// safe for concurrent use.
type Driver interface {
	// The url this driver is bootstrapped
	Target() url.URL
	// Acquire a session with `AccessModeWrite` mode
	Session() (*Session, error)
	// Acquire a session with provided access mode
	SessionWithAccessMode(accessMode AccessMode) (*Session, error)
	// Acquire a session with 'AccessModeWrite' and provided bookmark
	SessionWithBookmark(bookmark string) (*Session, error)
	// Acquire a session with provided access mode and provided bookmarks
	SessionWithAccessModeAndBookmark(accessMode AccessMode, bookmarks ...string) (*Session, error)
	// Close the driver and all underlying connections
	Close() error

	acquire(mode AccessMode) (neo4j.Connection, error)
}

// Entry method to the neo4j driver to create an instance of a Driver
func NewDriver(target string, auth AuthToken, config *Config) (Driver, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "bolt" {
		return newDirectDriver(parsed, auth, config)
	}
	return nil, fmt.Errorf("URL scheme %s is not supported", parsed.Scheme)
}
