/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package neo4j provides required functionality to connect and execute statements against a Neo4j Database.
package neo4j

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/connector"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/router"
)

// AccessMode defines modes that routing driver decides to which cluster member
// a connection should be opened.
type AccessMode int

const (
	// AccessModeWrite tells the driver to use a connection to 'Leader'
	AccessModeWrite AccessMode = 0
	// AccessModeRead tells the driver to use a connection to one of the 'Follower' or 'Read Replica'.
	AccessModeRead AccessMode = 1
)

// Driver represents a pool(s) of connections to a neo4j server or cluster. It's
// safe for concurrent use.
type Driver interface {
	// The url this driver is bootstrapped
	Target() url.URL
	// Creates a new session based on the specified session configuration.
	NewSession(config SessionConfig) (Session, error)
	// Deprecated: Use NewSession instead
	Session(accessMode AccessMode, bookmarks ...string) (Session, error)
	// Verifies that the driver can connect to a remote server or cluster by
	// establishing a network connection with the remote. Returns nil if succesful
	// or error describing the problem.
	VerifyConnectivity() error
	// Close the driver and all underlying connections
	Close() error
}

var driverLogName = "driver"

// NewDriver is the entry point to the neo4j driver to create an instance of a Driver. It is the first function to
// be called in order to establish a connection to a neo4j database. It requires a Bolt URI and an authentication
// token as parameters and can also take optional configuration function(s) as variadic parameters.
//
// In order to connect to a single instance database, you need to pass a URI with scheme 'bolt', 'bolt+s' or 'bolt+ssc'.
//	driver, err = NewDriver("bolt://db.server:7687", BasicAuth(username, password))
//
// In order to connect to a causal cluster database, you need to pass a URI with scheme 'neo4j', 'neo4j+s' or 'neo4j+ssc'
// and its host part set to be one of the core cluster members.
//	driver, err = NewDriver("neo4j://core.db.server:7687", BasicAuth(username, password))
//
// You can override default configuration options by providing a configuration function(s)
//	driver, err = NewDriver(uri, BasicAuth(username, password), function (config *Config) {
// 		config.MaxConnectionPoolSize = 10
// 	})
func NewDriver(target string, auth AuthToken, configurers ...func(*Config)) (Driver, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	if parsed.Port() == "" {
		parsed.Host = parsed.Host + ":7687"
	}

	d := driver{target: parsed}

	routing := true
	switch parsed.Scheme {
	case "bolt":
		routing = false
		d.connector.SkipEncryption = true
	case "bolt+s":
		routing = false
	case "bolt+ssc":
		d.connector.SkipVerify = true
		routing = false
	case "neo4j":
		d.connector.SkipEncryption = true
	case "neo4j+ssc":
		d.connector.SkipVerify = true
	case "neo4j+s":
	default:
		return nil, &UsageError{
			Message: fmt.Sprintf("URL scheme %s is not supported", parsed.Scheme),
		}
	}

	if !routing && len(parsed.RawQuery) > 0 {
		return nil, &UsageError{
			Message: fmt.Sprintf("Routing context is not supported for URL scheme %s", parsed.Scheme),
		}
	}

	// Apply client hooks for setting up configuration
	d.config = defaultConfig()
	for _, configurer := range configurers {
		configurer(d.config)
	}
	if err := validateAndNormaliseConfig(d.config); err != nil {
		return nil, err
	}

	// Setup logging
	d.log = d.config.Log
	if d.log == nil {
		// Default to void logger
		d.log = &VoidLog{}
	}
	d.logId = strconv.FormatUint(uint64(atomic.AddUint32(&driverid, 1)), 10)

	// Continue to setup connector
	d.connector.UserAgent = d.config.UserAgent
	d.connector.RootCAs = d.config.RootCAs
	d.connector.Log = d.log
	d.connector.Auth = auth.tokens

	d.pool = pool.New(d.config.MaxConnectionPoolSize, d.config.MaxConnectionLifetime, d.connector.Connect, d.log)

	if !routing {
		d.router = &directRouter{server: parsed.Host}
	} else {
		routingContext, err := routingContextFromUrl(parsed)
		if err != nil {
			d.log.Error(driverLogName, d.logId, err)
			return nil, err
		}

		var routersResolver func() []string
		addressResolverHook := d.config.AddressResolver
		if addressResolverHook != nil {
			routersResolver = func() []string {
				addresses := addressResolverHook(parsed)
				servers := make([]string, len(addresses))
				for i, a := range addresses {
					servers[i] = fmt.Sprintf("%s:%s", a.Hostname(), a.Port())
				}
				return servers
			}
		}
		d.router = router.New(parsed.Host, routersResolver, routingContext, d.pool, d.log)
	}

	d.log.Infof(driverLogName, d.logId, "Created { target: %s }", target)
	return &d, nil
}

var driverid uint32

func routingContextFromUrl(u *url.URL) (map[string]string, error) {
	queryValues := u.Query()
	routingContext := make(map[string]string, len(queryValues))
	for k, vs := range queryValues {
		if len(vs) > 1 {
			return nil, &UsageError{
				Message: fmt.Sprintf("Duplicated routing context key '%s'", k),
			}
		}
		if len(vs) == 0 {
			return nil, &UsageError{
				Message: fmt.Sprintf("Empty routing context key '%s'", k),
			}
		}
		v := vs[0]
		v = strings.TrimSpace(v)
		if len(v) == 0 {
			return nil, &UsageError{
				Message: fmt.Sprintf("Empty routing context value for key '%s'", k),
			}
		}
		routingContext[k] = v
	}
	return routingContext, nil
}

type sessionRouter interface {
	Readers(database string) ([]string, error)
	Writers(database string) ([]string, error)
	Invalidate(database string)
	CleanUp()
}

type driver struct {
	target    *url.URL
	config    *Config
	pool      *pool.Pool
	mut       sync.Mutex
	connector connector.Connector
	router    sessionRouter
	logId     string
	log       log.Logger
}

func (d *driver) Target() url.URL {
	return *d.target
}

func (d *driver) Session(accessMode AccessMode, bookmarks ...string) (Session, error) {
	d.mut.Lock()
	defer d.mut.Unlock()
	if d.pool == nil {
		return nil, &UsageError{
			Message: "Trying to create session on closed driver",
		}
	}
	return newSession(
		d.config, d.router,
		d.pool, db.AccessMode(accessMode), bookmarks, db.DefaultDatabase, d.log), nil
}

func (d *driver) NewSession(config SessionConfig) (Session, error) {
	databaseName := db.DefaultDatabase
	if config.DatabaseName != "" {
		databaseName = config.DatabaseName
	}

	d.mut.Lock()
	defer d.mut.Unlock()
	if d.pool == nil {
		return nil, &UsageError{
			Message: "Trying to create session on closed driver",
		}
	}
	return newSession(
		d.config, d.router,
		d.pool, db.AccessMode(config.AccessMode), config.Bookmarks, databaseName, d.log), nil
}

func (d *driver) VerifyConnectivity() error {
	session, err := d.NewSession(SessionConfig{AccessMode: AccessModeRead})
	if err != nil {
		return err
	}
	result, err := session.Run("RETURN 1 AS n", nil)
	if err != nil {
		return err
	}
	_, err = result.Consume()
	return err
}

func (d *driver) Close() error {
	d.mut.Lock()
	defer d.mut.Unlock()
	// Safeguard against closing more than once
	if d.pool != nil {
		d.pool.Close()
	}
	d.pool = nil
	d.log.Infof(driverLogName, d.logId, "Closed")
	return nil
}
