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
	"math"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collections"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/retry"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/telemetry"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

// TransactionWork represents a unit of work that will be executed against the provided
// transaction
// ManagedTransactionWork is created via the context-aware driver returned
// by NewDriverWithContext.
//
// Deprecated: use ManagedTransactionWork instead. TransactionWork will be removed in 6.0.
type TransactionWork func(tx Transaction) (any, error)

// ManagedTransactionWork represents a unit of work that will be executed against the provided
// transaction
type ManagedTransactionWork func(tx ManagedTransaction) (any, error)

// SessionWithContext represents a logical connection (which is not tied to a physical connection)
// to the server
type SessionWithContext interface {
	// LastBookmarks returns the bookmark received following the last successfully completed transaction.
	// If no bookmark was received or if this transaction was rolled back, the initial set of bookmarks will be
	// returned.
	LastBookmarks() Bookmarks
	lastBookmark() string
	// BeginTransaction starts a new explicit transaction on this session
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	BeginTransaction(ctx context.Context, configurers ...func(*TransactionConfig)) (ExplicitTransaction, error)
	// ExecuteRead executes the given unit of work in a AccessModeRead transaction with
	// retry logic in place
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	ExecuteRead(ctx context.Context, work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error)
	// ExecuteWrite executes the given unit of work in a AccessModeWrite transaction with
	// retry logic in place
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	ExecuteWrite(ctx context.Context, work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error)
	// Run executes an auto-commit statement and returns a result
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Run(ctx context.Context, cypher string, params map[string]any, configurers ...func(*TransactionConfig)) (ResultWithContext, error)
	// Close closes any open resources and marks this session as unusable
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Close(ctx context.Context) error
	executeQueryRead(ctx context.Context, work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error)
	executeQueryWrite(ctx context.Context, work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error)
	legacy() Session
	getServerInfo(ctx context.Context) (ServerInfo, error)
	verifyAuthentication(ctx context.Context) error
}

// SessionConfig is used to configure a new session, its zero value uses safe defaults.
type SessionConfig struct {
	// AccessMode used when using Session.Run and explicit transactions. Used to route query
	// to read or write servers when running in a cluster. Session.ReadTransaction and Session.WriteTransaction
	// does not rely on this mode.
	AccessMode AccessMode
	// Bookmarks are the initial bookmarks used to ensure that the executing server is at least up
	// to date to the point represented by the latest of the provided bookmarks. After running commands
	// on the session the bookmark can be retrieved with Session.LastBookmark. All commands executing
	// within the same session will automatically use the bookmark from the previous command in the
	// session.
	Bookmarks Bookmarks
	// DatabaseName sets the target database name for the queries executed within the session created with this
	// configuration.
	// Usage of Cypher clauses like USE is not a replacement for this option.
	// Drive​r sends Cypher to the server for processing.
	// This option has no explicit value by default, but it is recommended to set one if the target database is known
	// in advance. This has the benefit of ensuring a consistent target database name throughout the session in a
	// straightforward way and potentially simplifies driver logic as well as reduces network communication resulting
	// in better performance.
	// When no explicit name is set, the driver behavior depends on the connection URI scheme supplied to the driver on
	// instantiation and Bolt protocol version.
	//
	// Specifically, the following applies:
	//
	// - for bolt schemes
	//		queries are dispatched to the server for execution without explicit database name supplied,
	// 		meaning that the target database name for query execution is determined by the server.
	//		It is important to note that the target database may change (even within the same session), for instance if the
	//		user's home database is changed on the server.
	//
	// - for neo4j schemes
	//		providing that Bolt protocol version 4.4, which was introduced with Neo4j server 4.4, or above
	//		is available, the driver fetches the user's home database name from the server on first query execution
	//		within the session and uses the fetched database name explicitly for all queries executed within the session.
	//		This ensures that the database name remains consistent within the given session. For instance, if the user's
	//		home database name is 'movies' and the server supplies it to the driver upon database name fetching for the
	//		session, all queries within that session are executed with the explicit database name 'movies' supplied.
	//		Any change to the user’s home database is reflected only in sessions created after such change takes effect.
	//		This behavior requires additional network communication.
	//		In clustered environments, it is strongly recommended to avoid a single point of failure.
	//		For instance, by ensuring that the connection URI resolves to multiple endpoints.
	//		For older Bolt protocol versions, the behavior is the same as described for the bolt schemes above.
	DatabaseName string
	// FetchSize defines how many records to pull from server in each batch.
	// From Bolt protocol v4 (Neo4j 4+) records can be fetched in batches as compared to fetching
	// all in previous versions.
	//
	// If FetchSize is set to FetchDefault, the driver decides the appropriate size. If set to a positive value
	// that size is used if the underlying protocol supports it otherwise it is ignored.
	//
	// To turn off fetching in batches and always fetch everything, set FetchSize to FetchAll.
	// If a single large result is to be retrieved this is the most performant setting.
	FetchSize int
	// Logging target the session will send its Bolt message traces
	//
	// Possible to use custom logger (implement log.BoltLogger interface) or
	// use neo4j.ConsoleBoltLogger.
	BoltLogger log.BoltLogger
	// ImpersonatedUser sets the Neo4j user that the session will be acting as.
	// If not set, the user configured for the driver will be used.
	//
	// If user impersonation is used, the default database for that impersonated
	// user will be used unless DatabaseName is set.
	//
	// In the former case, when routing is enabled, using impersonation
	// without DatabaseName will cause the driver to query the
	// cluster for the name of the default database of the impersonated user.
	// This is done at the beginning of the session so that queries are routed
	// to the correct cluster member (different databases may have different
	// leaders).
	ImpersonatedUser string
	// BookmarkManager defines a central point to externally supply bookmarks
	// and be notified of bookmark updates per database
	// Since 5.0
	// default: nil (no-op)
	BookmarkManager BookmarkManager
	// NotificationsMinSeverity defines the minimum severity level of notifications the server should send.
	// By default, the driver's settings are used.
	// Else, this option overrides the driver's settings.
	// Disabling severities allows the server to skip analysis for those, which can speed up query execution.
	NotificationsMinSeverity notifications.NotificationMinimumSeverityLevel
	// NotificationsDisabledCategories defines the categories of notifications the server should not send.
	// By default, the driver's settings are used.
	// Else, this option overrides the driver's settings.
	// Disabling categories allows the server to skip analysis for those, which can speed up query execution.
	NotificationsDisabledCategories notifications.NotificationDisabledCategories
	// Auth is used to overwrite the authentication information for the session.
	// This requires the server to support re-authentication on the protocol level.
	// `nil` will make the driver use the authentication information from the driver configuration.
	// The `neo4j` package provides factory functions for common authentication schemes:
	//   - `neo4j.NoAuth`
	//   - `neo4j.BasicAuth`
	//   - `neo4j.KerberosAuth`
	//   - `neo4j.BearerAuth`
	//   - `neo4j.CustomAuth`
	Auth *AuthToken

	forceReAuth bool
}

// FetchAll turns off fetching records in batches.
const FetchAll = -1

// FetchDefault lets the driver decide fetch size
const FetchDefault = 0

// Connection pool as seen by the session.
type sessionPool interface {
	Borrow(ctx context.Context, getServerNames func() []string, wait bool, boltLogger log.BoltLogger, livenessCheckTimeout time.Duration, auth *idb.ReAuthToken) (idb.Connection, error)
	Return(ctx context.Context, c idb.Connection)
	CleanUp(ctx context.Context)
}

type sessionWithContext struct {
	driverConfig  *Config
	defaultMode   idb.AccessMode
	bookmarks     *sessionBookmarks
	resolveHomeDb bool
	pool          sessionPool
	router        sessionRouter
	explicitTx    *explicitTransaction
	autocommitTx  *autocommitTransaction
	sleep         func(d time.Duration)
	logId         string
	log           log.Logger
	throttleTime  time.Duration
	fetchSize     int
	config        SessionConfig
	auth          *idb.ReAuthToken
}

func newSessionWithContext(
	config *Config,
	sessConfig SessionConfig,
	router sessionRouter,
	pool sessionPool,
	logger log.Logger,
	token *idb.ReAuthToken,
) *sessionWithContext {
	logId := log.NewId()
	logger.Debugf(log.Session, logId, "Created")

	fetchSize := config.FetchSize
	if sessConfig.FetchSize != FetchDefault {
		fetchSize = sessConfig.FetchSize
	}

	return &sessionWithContext{
		driverConfig:  config,
		router:        router,
		pool:          pool,
		defaultMode:   idb.AccessMode(sessConfig.AccessMode),
		bookmarks:     newSessionBookmarks(sessConfig.BookmarkManager, sessConfig.Bookmarks),
		config:        sessConfig,
		resolveHomeDb: sessConfig.DatabaseName == "",
		sleep:         time.Sleep,
		log:           logger,
		logId:         logId,
		throttleTime:  time.Second * 1,
		fetchSize:     fetchSize,
		auth:          token,
	}
}

func (s *sessionWithContext) lastBookmark() string {
	// Pick up bookmark from pending auto-commit if there is a bookmark on it
	// Note: the bookmark manager should not be notified here because:
	//  - the results of the autocommit transaction may have not been consumed
	// 	yet, in which case, the underlying connection may have an outdated
	//	cached bookmark value
	//  - moreover, the bookmark manager may already hold newer bookmarks
	// 	because other sessions for the same DB have completed some work in
	//	parallel
	if s.autocommitTx != nil {
		s.retrieveSessionBookmarks(s.autocommitTx.conn)
	}

	// Report bookmark from previously closed connection or from initial set
	return s.bookmarks.lastBookmark()
}

func (s *sessionWithContext) LastBookmarks() Bookmarks {
	// Pick up bookmark from pending auto-commit if there is a bookmark on it
	// Note: the bookmark manager should not be notified here because:
	//  - the results of the autocommit transaction may have not been consumed
	// 	yet, in which case, the underlying connection may have an outdated
	//	cached bookmark value
	//  - moreover, the bookmark manager may already hold newer bookmarks
	// 	because other sessions for the same DB have completed some work in
	//	parallel
	if s.autocommitTx != nil {
		s.retrieveSessionBookmarks(s.autocommitTx.conn)
	}

	// Report bookmarks from previously closed connection or from initial set
	return s.bookmarks.currentBookmarks()
}

func (s *sessionWithContext) BeginTransaction(ctx context.Context, configurers ...func(*TransactionConfig)) (ExplicitTransaction, error) {
	// Guard for more than one transaction per session
	if s.explicitTx != nil {
		err := &UsageError{Message: "Session already has a pending transaction"}
		s.log.Error(log.Session, s.logId, err)
		return nil, err
	}

	if s.autocommitTx != nil {
		s.autocommitTx.done(ctx)
	}

	// Apply configuration functions
	config := defaultTransactionConfig()
	for _, c := range configurers {
		c(&config)
	}
	if err := validateTransactionConfig(config); err != nil {
		return nil, err
	}

	// Get a connection from the pool. This could fail in clustered environment.
	conn, err := s.getConnection(ctx, s.defaultMode, s.driverConfig.ConnectionLivenessCheckTimeout)
	if err != nil {
		return nil, errorutil.WrapError(err)
	}

	if !s.driverConfig.TelemetryDisabled {
		conn.Telemetry(telemetry.UnmanagedTransaction, nil)
	}

	beginBookmarks, err := s.getBookmarks(ctx)
	if err != nil {
		s.pool.Return(ctx, conn)
		return nil, errorutil.WrapError(err)
	}
	txHandle, err := conn.TxBegin(ctx,
		idb.TxConfig{
			Mode:             s.defaultMode,
			Bookmarks:        beginBookmarks,
			Timeout:          config.Timeout,
			Meta:             config.Metadata,
			ImpersonatedUser: s.config.ImpersonatedUser,
			NotificationConfig: idb.NotificationConfig{
				MinSev:  s.config.NotificationsMinSeverity,
				DisCats: s.config.NotificationsDisabledCategories,
			},
		}, true)
	if err != nil {
		s.pool.Return(ctx, conn)
		return nil, errorutil.WrapError(err)
	}

	// Create transaction wrapper
	txState := &transactionState{}
	tx := &explicitTransaction{
		conn:      conn,
		fetchSize: s.fetchSize,
		txHandle:  txHandle,
		txState:   txState,
	}

	onClose := func() {
		if tx.conn == nil {
			return
		}
		// On run failure, transaction closed (rolled back or committed)
		bookmarkErr := s.retrieveBookmarks(ctx, tx.conn, beginBookmarks)
		s.pool.Return(ctx, tx.conn)
		tx.txState.err = errorutil.CombineAllErrors(tx.txState.err, bookmarkErr)
		tx.conn = nil
		s.explicitTx = nil
	}
	tx.onClosed = onClose
	txState.resultErrorHandlers = append(txState.resultErrorHandlers, func(error) { onClose() })

	s.explicitTx = tx

	return s.explicitTx, nil
}

func (s *sessionWithContext) ExecuteRead(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.ReadMode, work, true, telemetry.ManagedTransaction, configurers...)
}

func (s *sessionWithContext) ExecuteWrite(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.WriteMode, work, true, telemetry.ManagedTransaction, configurers...)
}

func (s *sessionWithContext) executeQueryRead(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.ReadMode, work, false, telemetry.ExecuteQuery, configurers...)
}

func (s *sessionWithContext) executeQueryWrite(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.WriteMode, work, false, telemetry.ExecuteQuery, configurers...)
}

func (s *sessionWithContext) runRetriable(
	ctx context.Context,
	mode idb.AccessMode,
	work ManagedTransactionWork,
	blockingTxBegin bool,
	api telemetry.API,
	configurers ...func(*TransactionConfig)) (any, error) {

	// Guard for more than one transaction per session
	if s.explicitTx != nil {
		err := &UsageError{Message: "Session already has a pending transaction"}
		return nil, err
	}

	if s.autocommitTx != nil {
		s.autocommitTx.done(ctx)
	}

	config := defaultTransactionConfig()
	for _, c := range configurers {
		c(&config)
	}
	if err := validateTransactionConfig(config); err != nil {
		return nil, err
	}

	state := retry.State{
		MaxTransactionRetryTime: s.driverConfig.MaxTransactionRetryTime,
		Log:                     s.log,
		LogName:                 log.Session,
		LogId:                   s.logId,
		Sleep:                   s.sleep,
		Throttle:                retry.Throttler(s.throttleTime),
		MaxDeadConnections:      s.driverConfig.MaxConnectionPoolSize,
		DatabaseName:            s.config.DatabaseName,
	}
	for state.Continue() {
		if hasCompleted, result := s.executeTransactionFunction(ctx, mode, config, &state, work, blockingTxBegin, api); hasCompleted {
			return result, nil
		}
	}

	err := state.ProduceError()
	s.log.Error(log.Session, s.logId, err)
	return nil, err
}

func (s *sessionWithContext) executeTransactionFunction(
	ctx context.Context,
	mode idb.AccessMode,
	config TransactionConfig,
	state *retry.State,
	work ManagedTransactionWork,
	blockingTxBegin bool,
	api telemetry.API) (bool, any) {

	conn, err := s.getConnection(ctx, mode, s.driverConfig.ConnectionLivenessCheckTimeout)
	if err != nil {
		state.OnFailure(ctx, err, conn, false)
		return false, nil
	}

	if !s.driverConfig.TelemetryDisabled && !state.TelemetrySent {
		conn.Telemetry(api, func() {
			state.TelemetrySent = true
		})
	}

	// handle transaction function panic as well
	defer func() {
		s.pool.Return(ctx, conn)
	}()

	beginBookmarks, err := s.getBookmarks(ctx)
	if err != nil {
		state.OnFailure(ctx, err, conn, false)
		return false, nil
	}
	txHandle, err := conn.TxBegin(ctx,
		idb.TxConfig{
			Mode:             mode,
			Bookmarks:        beginBookmarks,
			Timeout:          config.Timeout,
			Meta:             config.Metadata,
			ImpersonatedUser: s.config.ImpersonatedUser,
			NotificationConfig: idb.NotificationConfig{
				MinSev:  s.config.NotificationsMinSeverity,
				DisCats: s.config.NotificationsDisabledCategories,
			},
		},
		blockingTxBegin)
	if err != nil {
		state.OnFailure(ctx, err, conn, false)
		return false, nil
	}

	tx := managedTransaction{conn: conn, fetchSize: s.fetchSize, txHandle: txHandle, txState: &transactionState{}}
	x, err := work(&tx)
	if err != nil {
		// If the client returns a client specific error that means that
		// client wants to rollback. We don't do an explicit rollback here
		// but instead rely on the pool invoking reset on the connection,
		// that will do an implicit rollback.
		state.OnFailure(ctx, err, conn, false)
		return false, nil
	}

	err = conn.TxCommit(ctx, txHandle)
	if err != nil {
		state.OnFailure(ctx, err, conn, true)
		return false, nil
	}

	// transaction has been committed so let's ignore (ie just log) the error
	if err = s.retrieveBookmarks(ctx, conn, beginBookmarks); err != nil {
		s.log.Warnf(log.Session, s.logId, "could not retrieve bookmarks after successful commit: %s\n"+
			"the results of this transaction may not be visible to subsequent operations", err.Error())
	}
	return true, x
}

func (s *sessionWithContext) getOrUpdateServers(ctx context.Context, mode idb.AccessMode) ([]string, error) {
	if mode == idb.ReadMode {
		return s.router.GetOrUpdateReaders(ctx, s.getBookmarks, s.config.DatabaseName, s.auth, s.config.BoltLogger)
	} else {
		return s.router.GetOrUpdateWriters(ctx, s.getBookmarks, s.config.DatabaseName, s.auth, s.config.BoltLogger)
	}
}

func (s *sessionWithContext) getServers(mode idb.AccessMode) func() []string {
	return func() []string {
		if mode == idb.ReadMode {
			return s.router.Readers(s.config.DatabaseName)
		} else {
			return s.router.Writers(s.config.DatabaseName)
		}
	}
}

func (s *sessionWithContext) getConnection(ctx context.Context, mode idb.AccessMode, livenessCheckTimeout time.Duration) (idb.Connection, error) {
	timeout := s.driverConfig.ConnectionAcquisitionTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
		deadline, _ := ctx.Deadline()
		s.log.Debugf(log.Session, s.logId, "connection acquisition timeout is %s, resolved deadline is: %s", timeout, deadline)
	} else if deadline, ok := ctx.Deadline(); ok {
		s.log.Debugf(log.Session, s.logId, "connection acquisition user-provided deadline is: %s", deadline)
	}

	if err := s.resolveHomeDatabase(ctx); err != nil {
		return nil, errorutil.WrapError(err)
	}
	_, err := s.getOrUpdateServers(ctx, mode)
	if err != nil {
		return nil, errorutil.WrapError(err)
	}

	conn, err := s.pool.Borrow(
		ctx,
		s.getServers(mode),
		timeout != 0,
		s.config.BoltLogger,
		livenessCheckTimeout,
		s.auth)
	if err != nil {
		return nil, errorutil.WrapError(err)
	}

	// Select database on server
	if s.config.DatabaseName != idb.DefaultDatabase {
		dbSelector, ok := conn.(idb.DatabaseSelector)
		if !ok {
			s.pool.Return(ctx, conn)
			return nil, &UsageError{Message: "Database does not support multi-database"}
		}
		dbSelector.SelectDatabase(s.config.DatabaseName)
	}

	return conn, nil
}

func (s *sessionWithContext) retrieveBookmarks(ctx context.Context, conn idb.Connection, sentBookmarks Bookmarks) error {
	if conn == nil {
		return nil
	}
	return s.bookmarks.replaceBookmarks(ctx, sentBookmarks, conn.Bookmark())
}

func (s *sessionWithContext) retrieveSessionBookmarks(conn idb.Connection) {
	if conn == nil {
		return
	}
	s.bookmarks.replaceSessionBookmarks(conn.Bookmark())
}

func (s *sessionWithContext) Run(ctx context.Context,
	cypher string, params map[string]any, configurers ...func(*TransactionConfig)) (ResultWithContext, error) {

	if s.explicitTx != nil {
		err := &UsageError{Message: "Trying to run auto-commit transaction while in explicit transaction"}
		s.log.Error(log.Session, s.logId, err)
		return nil, err
	}

	if s.autocommitTx != nil {
		s.autocommitTx.done(ctx)
	}

	config := defaultTransactionConfig()
	for _, c := range configurers {
		c(&config)
	}
	if err := validateTransactionConfig(config); err != nil {
		return nil, err
	}

	conn, err := s.getConnection(ctx, s.defaultMode, s.driverConfig.ConnectionLivenessCheckTimeout)
	if err != nil {
		return nil, errorutil.WrapError(err)
	}

	if !s.driverConfig.TelemetryDisabled {
		conn.Telemetry(telemetry.AutoCommitTransaction, nil)
	}

	runBookmarks, err := s.getBookmarks(ctx)
	if err != nil {
		s.pool.Return(ctx, conn)
		return nil, errorutil.WrapError(err)
	}
	stream, err := conn.Run(
		ctx,
		idb.Command{
			Cypher:    cypher,
			Params:    params,
			FetchSize: s.fetchSize,
		},
		idb.TxConfig{
			Mode:             s.defaultMode,
			Bookmarks:        runBookmarks,
			Timeout:          config.Timeout,
			Meta:             config.Metadata,
			ImpersonatedUser: s.config.ImpersonatedUser,
			NotificationConfig: idb.NotificationConfig{
				MinSev:  s.config.NotificationsMinSeverity,
				DisCats: s.config.NotificationsDisabledCategories,
			},
		},
	)
	if err != nil {
		s.pool.Return(ctx, conn)
		return nil, errorutil.WrapError(err)
	}

	s.autocommitTx = &autocommitTransaction{
		conn: conn,
		res: newResultWithContext(conn, stream, cypher, params, &transactionState{}, func() {
			if err := s.retrieveBookmarks(ctx, conn, runBookmarks); err != nil {
				s.log.Warnf(log.Session, s.logId, "could not retrieve bookmarks after result consumption: %s\n"+
					"the result of the initiating auto-commit transaction may not be visible to subsequent operations", err.Error())
			}
		}),
		onClosed: func() {
			s.pool.Return(ctx, conn)
			s.autocommitTx = nil
		},
	}

	return s.autocommitTx.res, nil
}

func (s *sessionWithContext) Close(ctx context.Context) error {
	var txErr error
	if s.explicitTx != nil {
		txErr = s.explicitTx.Close(ctx)
	}

	if s.autocommitTx != nil {
		s.autocommitTx.discard(ctx)
	}

	defer s.log.Debugf(log.Session, s.logId, "Closed")
	poolCleanUpChan := make(chan struct{}, 1)
	routerCleanUpChan := make(chan struct{}, 1)
	go func() {
		s.pool.CleanUp(ctx)
		poolCleanUpChan <- struct{}{}
	}()
	go func() {
		s.router.CleanUp()
		routerCleanUpChan <- struct{}{}
	}()
	<-poolCleanUpChan
	<-routerCleanUpChan
	return txErr
}

func (s *sessionWithContext) legacy() Session {
	return &session{delegate: s}
}

func (s *sessionWithContext) getServerInfo(ctx context.Context) (ServerInfo, error) {
	if err := s.resolveHomeDatabase(ctx); err != nil {
		return nil, errorutil.WrapError(err)
	}
	_, err := s.getOrUpdateServers(ctx, idb.ReadMode)
	if err != nil {
		return nil, errorutil.WrapError(err)
	}
	conn, err := s.pool.Borrow(
		ctx,
		s.getServers(idb.ReadMode),
		s.driverConfig.ConnectionAcquisitionTimeout != 0,
		s.config.BoltLogger,
		0,
		s.auth)
	if err != nil {
		return nil, errorutil.WrapError(err)
	}
	defer s.pool.Return(ctx, conn)
	return &simpleServerInfo{
		address:         conn.ServerName(),
		agent:           conn.ServerVersion(),
		protocolVersion: conn.Version(),
	}, nil
}

func (s *sessionWithContext) verifyAuthentication(ctx context.Context) error {
	_, err := s.getOrUpdateServers(ctx, idb.ReadMode)
	if err != nil {
		return errorutil.WrapError(err)
	}
	conn, err := s.pool.Borrow(
		ctx,
		s.getServers(idb.ReadMode),
		s.driverConfig.ConnectionAcquisitionTimeout != 0,
		s.config.BoltLogger,
		0,
		s.auth)
	if err != nil {
		return errorutil.WrapError(err)
	}
	defer s.pool.Return(ctx, conn)
	return nil
}

func (s *sessionWithContext) resolveHomeDatabase(ctx context.Context) error {
	if !s.resolveHomeDb {
		return nil
	}

	bookmarks, err := s.getBookmarks(ctx)
	if err != nil {
		return err
	}
	defaultDb, err := s.router.GetNameOfDefaultDatabase(
		ctx,
		bookmarks,
		s.config.ImpersonatedUser,
		s.auth,
		s.config.BoltLogger)
	if err != nil {
		return err
	}
	s.log.Debugf(log.Session, s.logId, "Resolved home database, uses db '%s'", defaultDb)
	s.config.DatabaseName = defaultDb
	s.resolveHomeDb = false
	return nil
}

func (s *sessionWithContext) getBookmarks(ctx context.Context) (Bookmarks, error) {
	bookmarks, err := s.bookmarks.getBookmarks(ctx)
	if err != nil {
		return nil, err
	}
	result := collections.NewSet(bookmarks)
	result.AddAll(s.bookmarks.currentBookmarks())
	return result.Values(), nil
}

type erroredSessionWithContext struct {
	err error
}

func (s *erroredSessionWithContext) LastBookmarks() Bookmarks {
	return nil
}

func (s *erroredSessionWithContext) lastBookmark() string {
	return ""
}
func (s *erroredSessionWithContext) BeginTransaction(context.Context, ...func(*TransactionConfig)) (ExplicitTransaction, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) ExecuteRead(context.Context, ManagedTransactionWork, ...func(*TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) ExecuteWrite(context.Context, ManagedTransactionWork, ...func(*TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) executeQueryRead(context.Context, ManagedTransactionWork, ...func(*TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) executeQueryWrite(context.Context, ManagedTransactionWork, ...func(*TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) Run(context.Context, string, map[string]any, ...func(*TransactionConfig)) (ResultWithContext, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) Close(context.Context) error {
	return s.err
}
func (s *erroredSessionWithContext) legacy() Session {
	return &erroredSession{err: s.err}
}
func (s *erroredSessionWithContext) getServerInfo(context.Context) (ServerInfo, error) {
	return nil, s.err
}

func (s *erroredSessionWithContext) verifyAuthentication(context.Context) error {
	return s.err
}

func defaultTransactionConfig() TransactionConfig {
	return TransactionConfig{Timeout: math.MinInt, Metadata: nil}
}

func validateTransactionConfig(config TransactionConfig) error {
	if config.Timeout != math.MinInt && config.Timeout < 0 {
		err := fmt.Sprintf("Negative transaction timeouts are not allowed. Given: %d", config.Timeout)
		return &UsageError{Message: err}
	}
	return nil
}
