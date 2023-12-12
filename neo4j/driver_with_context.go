// Package neo4j provides required functionality to connect and execute statements against a Neo4j Database.

/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/connector"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/router"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"net/url"
	"strings"
	"sync"
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

// DriverWithContext represents a pool of connections to a neo4j server or cluster. It's
// safe for concurrent use.
type DriverWithContext interface {
	// ExecuteQueryBookmarkManager returns the bookmark manager instance used by ExecuteQuery by default.
	//
	// This is useful when ExecuteQuery is called without custom bookmark managers and the lower-level
	// neo4j.SessionWithContext APIs are called as well.
	// In that case, the recommended approach is as follows:
	// 	results, err := driver.ExecuteQuery(ctx, query, params)
	// 	// [...] do something with results and error
	//	bookmarkManager := driver.ExecuteQueryBookmarkManager()
	// 	// maintain consistency with sessions as well
	//	session := driver.NewSession(ctx, neo4j.SessionConfig {BookmarkManager: bookmarkManager})
	//	// [...] run something within the session
	ExecuteQueryBookmarkManager() BookmarkManager
	// Target returns the url this driver is bootstrapped
	Target() url.URL
	// NewSession creates a new session based on the specified session configuration.
	NewSession(ctx context.Context, config SessionConfig) SessionWithContext
	// VerifyConnectivity checks that the driver can connect to a remote server or cluster by
	// establishing a network connection with the remote. Returns nil if successful
	// or error describing the problem.
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	VerifyConnectivity(ctx context.Context) error
	// VerifyAuthentication verifies that the authentication information is valid.
	//
	// It's much like `DriverWithContext.VerifyConnectivity`, but for checking authentication.
	//
	// Passing `nil` as `auth` will use the authentication information that was used to create the driver.
	//
	// Tries to establish a working read connection to the remote server or a member of a cluster and exchange some data.
	// In a cluster, there is no guarantee about which server will be contacted.
	// If the data exchange is successful, the authentication information is valid no error is returned.
	// Otherwise, the error will be matched against a list of known authentication errors.
	// If the error is on that list, an `neo4j.InvalidAuthenticationError` is returned.
	// Otherwise, the original error is returned.
	VerifyAuthentication(ctx context.Context, auth *AuthToken) error
	// Close the driver and all underlying connections
	Close(ctx context.Context) error
	// IsEncrypted determines whether the driver communication with the server
	// is encrypted. This is a static check. The function can also be called on
	// a closed Driver.
	IsEncrypted() bool
	// GetServerInfo attempts to obtain server information from the target Neo4j
	// deployment
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	GetServerInfo(ctx context.Context) (ServerInfo, error)
}

// ResultTransformer is a record accumulator that produces an instance of T when the processing of records is over.
type ResultTransformer[T any] interface {
	// Accept is called whenever a new record is fetched from the server
	// Implementers are free to accumulate or discard the specified record
	Accept(*Record) error

	// Complete is called when the record fetching is over and no error occurred.
	// In particular, it is important to note that Accept may be called several times before an error occurs.
	// In that case, Complete will not be called.
	Complete(keys []string, summary ResultSummary) (T, error)
}

// NewDriverWithContext is the entry point to the neo4j driver to create an instance of a Driver. It is the first function to
// be called in order to establish a connection to a neo4j database. It requires a Bolt URI and authentication
// as parameters and can also take optional configuration function(s) as variadic parameters.
//
// No connectivity happens when NewDriverWithContext is called.
// Call DriverWithContext.VerifyConnectivity once the driver is created if you want to eagerly check that the provided
// URI and credentials are correct.
//
// In order to connect to a single instance database, you need to pass a URI with scheme 'bolt', 'bolt+s' or 'bolt+ssc'.
//
//	driver, err = NewDriverWithContext("bolt://db.server:7687", BasicAuth(username, password))
//
// In order to connect to a causal cluster database, you need to pass a URI with scheme 'neo4j', 'neo4j+s' or 'neo4j+ssc'
// and its host part set to be one of the core cluster members.
//
//	driver, err = NewDriverWithContext("neo4j://core.db.server:7687", BasicAuth(username, password))
//
// You can override default configuration options by providing a configuration function(s)
//
//	driver, err = NewDriverWithContext(uri, BasicAuth(username, password), function (config *Config) {
//		config.MaxConnectionPoolSize = 10
//	})
//
// The authentication can be nil, in which case the driver will not authenticate with the server.
// Else, an implementation of `auth.TokenManager` is required.
// The `neo4j` package provides default implementations of `auth.TokenManager` for common authentication schemes:
//   - `neo4j.NoAuth` (equivalent to `nil`)
//   - `neo4j.BasicAuth`
//   - `neo4j.KerberosAuth`
//   - `neo4j.BearerAuth`
//   - `neo4j.CustomAuth`
func NewDriverWithContext(target string, auth auth.TokenManager, configurers ...func(*Config)) (DriverWithContext, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}

	d := driverWithContext{target: parsed, mut: racing.NewMutex(), auth: auth}

	routing := true
	d.connector.Network = "tcp"
	address := parsed.Host
	switch parsed.Scheme {
	case "bolt":
		routing = false
		d.connector.SkipEncryption = true
	case "bolt+unix":
		// bolt+unix://<path to socket>
		routing = false
		d.connector.SkipEncryption = true
		d.connector.Network = "unix"
		if parsed.Host != "" {
			return nil, &UsageError{
				Message: fmt.Sprintf("Host part should be empty for scheme %s", parsed.Scheme),
			}
		}
		address = parsed.Path
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
			Message: fmt.Sprintf("URI scheme %s is not supported", parsed.Scheme),
		}
	}

	if parsed.Host != "" && parsed.Port() == "" {
		address += ":7687"
		parsed.Host = address
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
	if auth == nil {
		auth = NoAuth()
	}

	// Setup logging
	d.log = d.config.Log
	if d.log == nil {
		// Default to void logger
		d.log = &log.Void{}
	}
	d.logId = log.NewId()

	routingContext, err := routingContextFromUrl(routing, parsed)
	if err != nil {
		return nil, err
	}

	d.connector.Log = d.log
	d.connector.RoutingContext = routingContext
	d.connector.Config = d.config

	// Let the pool use the same log ID as the driver to simplify log reading.
	d.pool = pool.New(d.config, d.connector.Connect, d.log, d.logId)

	if !routing {
		d.router = &directRouter{address: address}
	} else {
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
		// Let the router use the same log ID as the driver to simplify log reading.
		d.router = router.New(
			address,
			routersResolver,
			routingContext,
			d.pool,
			d.config.ConnectionLivenessCheckTimeout,
			d.log,
			d.logId,
		)
	}

	d.pool.SetRouter(d.router)

	d.log.Infof(log.Driver, d.logId, "Created { target: %s }", address)
	return &d, nil
}

const routingContextAddressKey = "address"

func routingContextFromUrl(useRouting bool, u *url.URL) (map[string]string, error) {
	if !useRouting {
		return nil, nil
	}
	queryValues := u.Query()
	routingContext := make(map[string]string, len(queryValues)+1 /*For address*/)
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
		if k == routingContextAddressKey {
			return nil, &UsageError{Message: fmt.Sprintf("Illegal key '%s' for routing context", k)}
		}
		routingContext[k] = v
	}
	routingContext[routingContextAddressKey] = u.Host
	return routingContext, nil
}

type sessionRouter interface {
	// GetOrUpdateReaders returns the list of servers that can serve reads on the requested database.
	// note: bookmarks are lazily supplied, only when a new routing table needs to be fetched
	// this is needed because custom bookmark managers may provide bookmarks from external systems
	// they should not be called when it is not needed (e.g. when a routing table is cached)
	GetOrUpdateReaders(ctx context.Context, bookmarks func(context.Context) ([]string, error), database string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) ([]string, error)
	// Readers returns the list of servers that can serve reads on the requested database.
	Readers(database string) []string
	// GetOrUpdateWriters returns the list of servers that can serve writes on the requested database.
	// note: bookmarks are lazily supplied, see Readers documentation to learn why
	GetOrUpdateWriters(ctx context.Context, bookmarks func(context.Context) ([]string, error), database string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) ([]string, error)
	// Writers returns the list of servers that can serve writes on the requested database.
	Writers(database string) []string
	// GetNameOfDefaultDatabase returns the name of the default database for the specified user.
	// The correct database name is needed when requesting readers or writers.
	// the bookmarks are eagerly provided since this method always fetches a new routing table
	GetNameOfDefaultDatabase(ctx context.Context, bookmarks []string, user string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) (string, error)
	Invalidate(db string)
	CleanUp()
	InvalidateWriter(db string, server string)
	InvalidateReader(db string, server string)
	InvalidateServer(server string)
}

type driverWithContext struct {
	target    *url.URL
	config    *Config
	pool      *pool.Pool
	mut       racing.Mutex
	connector connector.Connector
	router    sessionRouter
	logId     string
	log       log.Logger
	// visible for tests
	executeQueryBookmarkManagerInitializer sync.Once
	// instance of the bookmark manager only used by default by managed sessions of ExecuteQuery
	// this is *not* used by default by user-created session (see NewSession)
	executeQueryBookmarkManager BookmarkManager
	auth                        auth.TokenManager
}

func (d *driverWithContext) Target() url.URL {
	return *d.target
}

func (d *driverWithContext) NewSession(ctx context.Context, config SessionConfig) SessionWithContext {
	if config.DatabaseName == "" {
		config.DatabaseName = idb.DefaultDatabase
	}

	var reAuthToken *idb.ReAuthToken
	if config.Auth == nil {
		reAuthToken = &idb.ReAuthToken{
			Manager:     d.auth,
			FromSession: false,
			ForceReAuth: config.forceReAuth,
		}
	} else {
		reAuthToken = &idb.ReAuthToken{
			Manager:     config.Auth,
			FromSession: true,
			ForceReAuth: config.forceReAuth,
		}
	}

	if !d.mut.TryLock(ctx) {
		return &erroredSessionWithContext{
			err: racing.LockTimeoutError("could not acquire lock in time when creating session")}
	}
	defer d.mut.Unlock()
	if d.pool == nil {
		return &erroredSessionWithContext{
			err: &UsageError{Message: "Trying to create session on closed driver"}}
	}
	return newSessionWithContext(d.config, config, d.router, d.pool, d.log, reAuthToken)
}

func (d *driverWithContext) VerifyConnectivity(ctx context.Context) error {
	_, err := d.GetServerInfo(ctx)
	return err
}

func (d *driverWithContext) IsEncrypted() bool {
	return !d.connector.SkipEncryption
}

func (d *driverWithContext) GetServerInfo(ctx context.Context) (_ ServerInfo, err error) {
	session := d.NewSession(ctx, SessionConfig{})
	defer func() {
		err = deferredClose(ctx, session, err)
	}()
	return session.getServerInfo(ctx)
}

func (d *driverWithContext) Close(ctx context.Context) error {
	if !d.mut.TryLock(ctx) {
		return racing.LockTimeoutError("could not acquire lock in time when closing driver")
	}
	defer d.mut.Unlock()
	// Safeguard against closing more than once
	if d.pool != nil {
		d.pool.Close(ctx)
		d.pool = nil
		d.log.Infof(log.Driver, d.logId, "Closed")
	}
	return nil
}

func (d *driverWithContext) VerifyAuthentication(ctx context.Context, auth *AuthToken) (err error) {
	session := d.NewSession(ctx, SessionConfig{Auth: auth, forceReAuth: true, DatabaseName: "system"})
	defer func() {
		err = deferredClose(ctx, session, err)
	}()
	err = session.verifyAuthentication(ctx)

	if tokenExpiredError, ok := err.(*TokenExpiredError); ok {
		return &InvalidAuthenticationError{inner: tokenExpiredError}
	}
	if neo4jError, ok := err.(*Neo4jError); ok {
		if neo4jError.Code == "Neo.ClientError.Security.CredentialsExpired" ||
			neo4jError.Code == "Neo.ClientError.Security.Forbidden" ||
			neo4jError.Code == "Neo.ClientError.Security.Unauthorized" {
			return &InvalidAuthenticationError{inner: neo4jError}
		}
	}
	return err
}

// ExecuteQuery runs the specified query with its parameters and returns the query result, transformed by the specified
// ResultTransformer function.
//
//	result, err := ExecuteQuery[*EagerResult](ctx, driver, query, params, EagerResultTransformer)
//
// Passing a nil ResultTransformer function is invalid and will return an error.
//
// Likewise, passing a function that returns a nil ResultTransformer is invalid and will return an error.
//
// ExecuteQuery runs the query in a single explicit, retryable transaction within a session entirely managed by
// the driver.
//
// Retries occur in the same conditions as when calling SessionWithContext.ExecuteRead and
// SessionWithContext.ExecuteWrite.
//
// Because it is an explicit transaction from the server point of view, Cypher queries using
// "CALL {} IN TRANSACTIONS" or the older "USING PERIODIC COMMIT" construct will not work (call
// SessionWithContext.Run for these).
//
// Specific settings can be configured via configuration callbacks. Built-in callbacks are provided such as:
//
//		neo4j.ExecuteQueryWithDatabase
//		neo4j.ExecuteQueryWithWritersRouting
//	 ...
//
// see neo4j.ExecuteQueryConfiguration for all possibilities.
//
// These built-in callbacks can be used and combined as follows:
//
//	ExecuteQuery[T](ctx, driver, query, params, transformerFunc,
//		neo4j.ExecuteQueryWithDatabase("my-db"),
//		neo4j.ExecuteQueryWithWritersRouting())
//
// For complete control over the configuration, you can also define your own callback:
//
//	ExecuteQuery[T](ctx, driver, query, params, transformerFunc, func(config *neo4j.ExecuteQueryConfiguration) {
//		config.Database = "my-db"
//		config.RoutingControl = neo4j.Write
//		config.ImpersonatedUser = "selda_bağcan"
//	})
//
// ExecuteQuery causal consistency is guaranteed by default across different successful calls to ExecuteQuery
// targeting the same database.
// In other words, a successful read query run by ExecuteQuery is guaranteed to be able to read results created
// from a previous successful write query run by ExecuteQuery on the same database.
// This is achieved through the use of bookmarks, managed by a default neo4j.BookmarkManager instance.
// This default BookmarkManager instance can be retrieved with DriverWithContext.DefaultExecuteQueryBookmarkManager.
// Such a consistency guarantee is *not* maintained between ExecuteQuery calls and the lower-level
// neo4j.SessionWithContext API calls, unless sessions are explicitly configured with the same bookmark manager.
// That guarantee may also break if a custom implementation of neo4j.BookmarkManager is provided via for instance
// the built-in callback neo4j.ExecuteQueryWithBookmarkManager.
// You can disable bookmark management by passing the neo4j.ExecuteQueryWithoutBookmarkManager callback to ExecuteQuery.
//
// The equivalent functionality of ExecuteQuery can be replicated with pre-existing APIs as follows:
//
//	 // all the error handling bits have been omitted for brevity (do not do this in production!)
//		session := driver.NewSession(ctx, neo4j.SessionConfig{
//			DatabaseName:     "<DATABASE>",
//			ImpersonatedUser: "<USER>",
//			BookmarkManager:  bookmarkManager,
//		})
//		defer handleClose(ctx, session)
//		// session.ExecuteRead is called if the routing is set to neo4j.Read
//		result, _ := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
//			result, _ := tx.Run(ctx, "<CYPHER>", parameters)
//			records, _ := result.Collect(ctx) // real implementation does not use Collect
//			keys, _ := result.Keys()
//			summary, _ := result.Consume(ctx)
//			return &neo4j.EagerResult{
//				Keys:    keys,
//				Records: records,
//				Summary: summary,
//			}, nil
//		})
//		eagerResult := result.(*neo4j.EagerResult)
//		// do something with eagerResult
//
// The available ResultTransformer implementation, EagerResultTransformer, computes an *EagerResult.
// As the latter's name suggests, this is not optimal when the result is made from a large number of records.
// In that situation, it is advised to create a custom implementation of ResultTransformer APIs, which do not require
// keeping all records in memory.
//
// The provided ResultTransformer function may be called several times since ExecuteQuery relies on transaction
// functions.
// Since ResultTransformer implementations are inherently stateful, the function must return a new ResultTransformer
// instance every time it is called.
//
// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
func ExecuteQuery[T any](
	ctx context.Context,
	driver DriverWithContext,
	query string,
	parameters map[string]any,
	newResultTransformer func() ResultTransformer[T],
	settings ...ExecuteQueryConfigurationOption) (res T, err error) {

	if driver == nil {
		return *new(T), &UsageError{Message: "nil is not a valid DriverWithContext argument."}
	}

	if newResultTransformer == nil {
		return *new(T), &UsageError{Message: "nil is not a valid ResultTransformer function argument. " +
			"Consider passing EagerResultTransformer or a function that returns an instance of your own " +
			"ResultTransformer implementation"}
	}

	bookmarkManager := driver.ExecuteQueryBookmarkManager()
	configuration := &ExecuteQueryConfiguration{
		BookmarkManager: bookmarkManager,
	}
	for _, setter := range settings {
		setter(configuration)
	}
	session := driver.NewSession(ctx, configuration.toSessionConfig())
	defer func() {
		err = errorutil.CombineAllErrors(err, session.Close(ctx))
	}()
	txFunction, err := configuration.selectTxFunctionApi(session)
	if err != nil {
		return *new(T), err
	}
	result, err := txFunction(ctx, executeQueryCallback(ctx, query, parameters, newResultTransformer), configuration.TransactionConfigurers...)
	if err != nil {
		return *new(T), err
	}
	return result.(T), err
}

func (d *driverWithContext) ExecuteQueryBookmarkManager() BookmarkManager {
	d.executeQueryBookmarkManagerInitializer.Do(func() {
		if d.executeQueryBookmarkManager == nil { // this allows tests to init the field themselves
			d.executeQueryBookmarkManager = NewBookmarkManager(BookmarkManagerConfig{})
		}
	})
	return d.executeQueryBookmarkManager
}

func executeQueryCallback[T any](
	ctx context.Context,
	query string,
	parameters map[string]any,
	newTransformer func() ResultTransformer[T]) ManagedTransactionWork {

	return func(tx ManagedTransaction) (any, error) {
		transformer := newTransformer()
		if transformer == nil {
			return nil, &UsageError{Message: "expected the result transformer function to return a valid " +
				"ResultTransformer instance, but got nil"}
		}
		cursor, err := tx.Run(ctx, query, parameters)
		if err != nil {
			return nil, err
		}
		for cursor.Next(ctx) {
			if err := transformer.Accept(cursor.Record()); err != nil {
				return nil, err
			}
		}
		if err = cursor.Err(); err != nil {
			return nil, err
		}
		keys, err := cursor.Keys()
		if err != nil {
			return nil, err
		}
		summary, err := cursor.Consume(ctx)
		if err != nil {
			return nil, err
		}
		return transformer.Complete(keys, summary)
	}
}

func EagerResultTransformer() ResultTransformer[*EagerResult] {
	return &eagerResultTransformer{}
}

type eagerResultTransformer struct {
	records []*Record
}

func (e *eagerResultTransformer) Accept(record *Record) error {
	e.records = append(e.records, record)
	return nil
}

func (e *eagerResultTransformer) Complete(keys []string, summary ResultSummary) (*EagerResult, error) {
	return &EagerResult{
		Keys:    keys,
		Records: e.records,
		Summary: summary,
	}, nil
}

// ExecuteQueryConfigurationOption is a callback that configures the execution of DriverWithContext.ExecuteQuery
type ExecuteQueryConfigurationOption func(*ExecuteQueryConfiguration)

// ExecuteQueryWithReadersRouting configures DriverWithContext.ExecuteQuery to route to reader members of the cluster
func ExecuteQueryWithReadersRouting() ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.Routing = Read
	}
}

// ExecuteQueryWithWritersRouting configures DriverWithContext.ExecuteQuery to route to writer members of the cluster
func ExecuteQueryWithWritersRouting() ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.Routing = Write
	}
}

// ExecuteQueryWithImpersonatedUser configures DriverWithContext.ExecuteQuery to impersonate the specified user
func ExecuteQueryWithImpersonatedUser(user string) ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.ImpersonatedUser = user
	}
}

// ExecuteQueryWithDatabase configures DriverWithContext.ExecuteQuery to target the specified database
func ExecuteQueryWithDatabase(db string) ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.Database = db
	}
}

// ExecuteQueryWithBookmarkManager configures DriverWithContext.ExecuteQuery to rely on the specified BookmarkManager
func ExecuteQueryWithBookmarkManager(bookmarkManager BookmarkManager) ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.BookmarkManager = bookmarkManager
	}
}

// ExecuteQueryWithoutBookmarkManager configures DriverWithContext.ExecuteQuery to not rely on any BookmarkManager
func ExecuteQueryWithoutBookmarkManager() ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.BookmarkManager = nil
	}
}

// ExecuteQueryWithBoltLogger configures DriverWithContext.ExecuteQuery to log Bolt messages with the provided BoltLogger
func ExecuteQueryWithBoltLogger(boltLogger log.BoltLogger) ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.BoltLogger = boltLogger
	}
}

// ExecuteQueryWithTransactionConfig configures DriverWithContext.ExecuteQuery with additional transaction configuration.
func ExecuteQueryWithTransactionConfig(configurers ...func(*TransactionConfig)) ExecuteQueryConfigurationOption {
	return func(configuration *ExecuteQueryConfiguration) {
		configuration.TransactionConfigurers = configurers
	}
}

// ExecuteQueryConfiguration holds all the possible configuration settings for DriverWithContext.ExecuteQuery
type ExecuteQueryConfiguration struct {
	Routing                RoutingControl
	ImpersonatedUser       string
	Database               string
	BookmarkManager        BookmarkManager
	BoltLogger             log.BoltLogger
	TransactionConfigurers []func(*TransactionConfig)
}

// RoutingControl specifies how the query executed by DriverWithContext.ExecuteQuery is to be routed
type RoutingControl int

const (
	// Write routes the query to execute to a writer member of the cluster
	Write RoutingControl = iota
	// Read routes the query to execute to a reader member of the cluster
	Read
)

func (c *ExecuteQueryConfiguration) toSessionConfig() SessionConfig {
	return SessionConfig{
		ImpersonatedUser: c.ImpersonatedUser,
		DatabaseName:     c.Database,
		BookmarkManager:  c.BookmarkManager,
		BoltLogger:       c.BoltLogger,
	}
}

type transactionFunction func(context.Context, ManagedTransactionWork, ...func(*TransactionConfig)) (any, error)

func (c *ExecuteQueryConfiguration) selectTxFunctionApi(session SessionWithContext) (transactionFunction, error) {
	switch c.Routing {
	case Read:
		return session.executeQueryRead, nil
	case Write:
		return session.executeQueryWrite, nil
	}
	return nil, fmt.Errorf("unsupported routing control, expected %d (Write) or %d (Read) "+
		"but got: %d", Write, Read, c.Routing)
}

// EagerResult holds the result and result metadata of the query executed via DriverWithContext.ExecuteQuery
type EagerResult struct {
	Keys    []string
	Records []*Record
	Summary ResultSummary
}
