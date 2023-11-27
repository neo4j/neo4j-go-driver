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

	bm "github.com/neo4j/neo4j-go-driver/v5/neo4j/bookmarks"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collections"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/telemetry"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/retry"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
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
	LastBookmarks() bm.Bookmarks
	lastBookmark() string
	// BeginTransaction starts a new explicit transaction on this session
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	BeginTransaction(ctx context.Context, configurers ...func(*config.TransactionConfig)) (ExplicitTransaction, error)
	// ExecuteRead executes the given unit of work in a AccessModeRead transaction with
	// retry logic in place
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	ExecuteRead(ctx context.Context, work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error)
	// ExecuteWrite executes the given unit of work in a AccessModeWrite transaction with
	// retry logic in place
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	ExecuteWrite(ctx context.Context, work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error)
	// Run executes an auto-commit statement and returns a result
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Run(ctx context.Context, cypher string, params map[string]any, configurers ...func(*config.TransactionConfig)) (ResultWithContext, error)
	// Close closes any open resources and marks this session as unusable
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Close(ctx context.Context) error
	executeQueryRead(ctx context.Context, work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error)
	executeQueryWrite(ctx context.Context, work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error)
	legacy() Session
	getServerInfo(ctx context.Context) (ServerInfo, error)
	verifyAuthentication(ctx context.Context) error
}

// FetchAll turns off fetching records in batches.
const FetchAll = -1

// FetchDefault lets the driver decide fetch size
const FetchDefault = 0

// Connection pool as seen by the session.
type sessionPool interface {
	Borrow(ctx context.Context, getServerNames func() []string, wait bool, boltLogger log.BoltLogger, livenessCheckThreshold time.Duration, auth *idb.ReAuthToken) (idb.Connection, error)
	Return(ctx context.Context, c idb.Connection)
	CleanUp(ctx context.Context)
	Now() time.Time
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
	now           *func() time.Time
	logId         string
	log           log.Logger
	throttleTime  time.Duration
	fetchSize     int
	config        config.SessionConfig
	auth          *idb.ReAuthToken
}

func newSessionWithContext(
	config *Config,
	sessConfig config.SessionConfig,
	router sessionRouter,
	pool sessionPool,
	logger log.Logger,
	token *idb.ReAuthToken,
	now *func() time.Time,
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
		now:           now,
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

func (s *sessionWithContext) LastBookmarks() bm.Bookmarks {
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

func (s *sessionWithContext) BeginTransaction(ctx context.Context, configurers ...func(*config.TransactionConfig)) (ExplicitTransaction, error) {
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
	conn, err := s.getConnection(ctx, s.defaultMode, pool.DefaultLivenessCheckThreshold)
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
	work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.ReadMode, work, true, telemetry.ManagedTransaction, configurers...)
}

func (s *sessionWithContext) ExecuteWrite(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.WriteMode, work, true, telemetry.ManagedTransaction, configurers...)
}

func (s *sessionWithContext) executeQueryRead(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.ReadMode, work, false, telemetry.ExecuteQuery, configurers...)
}

func (s *sessionWithContext) executeQueryWrite(ctx context.Context,
	work ManagedTransactionWork, configurers ...func(*config.TransactionConfig)) (any, error) {

	return s.runRetriable(ctx, idb.WriteMode, work, false, telemetry.ExecuteQuery, configurers...)
}

func (s *sessionWithContext) runRetriable(
	ctx context.Context,
	mode idb.AccessMode,
	work ManagedTransactionWork,
	blockingTxBegin bool,
	api telemetry.API,
	configurers ...func(*config.TransactionConfig)) (any, error) {

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
		Now:                     s.now,
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
	config config.TransactionConfig,
	state *retry.State,
	work ManagedTransactionWork,
	blockingTxBegin bool,
	api telemetry.API) (bool, any) {

	conn, err := s.getConnection(ctx, mode, pool.DefaultLivenessCheckThreshold)
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

func (s *sessionWithContext) getConnection(ctx context.Context, mode idb.AccessMode, livenessCheckThreshold time.Duration) (idb.Connection, error) {
	timeout := s.driverConfig.ConnectionAcquisitionTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		if cancel != nil {
			defer cancel()
		}
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
		livenessCheckThreshold,
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

func (s *sessionWithContext) retrieveBookmarks(ctx context.Context, conn idb.Connection, sentBookmarks bm.Bookmarks) error {
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
	cypher string, params map[string]any, configurers ...func(*config.TransactionConfig)) (ResultWithContext, error) {

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

	conn, err := s.getConnection(ctx, s.defaultMode, pool.DefaultLivenessCheckThreshold)
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

func (s *sessionWithContext) getBookmarks(ctx context.Context) (bm.Bookmarks, error) {
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

func (s *erroredSessionWithContext) LastBookmarks() bm.Bookmarks {
	return nil
}

func (s *erroredSessionWithContext) lastBookmark() string {
	return ""
}
func (s *erroredSessionWithContext) BeginTransaction(context.Context, ...func(*config.TransactionConfig)) (ExplicitTransaction, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) ExecuteRead(context.Context, ManagedTransactionWork, ...func(*config.TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) ExecuteWrite(context.Context, ManagedTransactionWork, ...func(*config.TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) executeQueryRead(context.Context, ManagedTransactionWork, ...func(*config.TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) executeQueryWrite(context.Context, ManagedTransactionWork, ...func(*config.TransactionConfig)) (any, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) Run(context.Context, string, map[string]any, ...func(*config.TransactionConfig)) (ResultWithContext, error) {
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

func defaultTransactionConfig() config.TransactionConfig {
	return config.TransactionConfig{Timeout: math.MinInt, Metadata: nil}
}

func validateTransactionConfig(config config.TransactionConfig) error {
	if config.Timeout != math.MinInt && config.Timeout < 0 {
		err := fmt.Sprintf("Negative transaction timeouts are not allowed. Given: %d", config.Timeout)
		return &UsageError{Message: err}
	}
	return nil
}
