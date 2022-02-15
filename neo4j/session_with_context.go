/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/retry"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

// TransactionWork represents a unit of work that will be executed against the provided
// transaction
type TransactionWork func(tx Transaction) (interface{}, error)

// TransactionWorkWithContext represents a unit of work that will be executed against the provided
// transaction
type TransactionWorkWithContext func(tx TransactionWithContext) (interface{}, error)

// SessionWithContext represents a logical connection (which is not tied to a physical connection)
// to the server
type SessionWithContext interface {
	// LastBookmarks returns the bookmark received following the last successfully completed transaction.
	// If no bookmark was received or if this transaction was rolled back, the initial set of bookmarks will be
	// returned.
	LastBookmarks() Bookmarks
	lastBookmark() string
	// BeginTransaction starts a new explicit transaction on this session
	BeginTransaction(ctx context.Context, configurers ...func(*TransactionConfig)) (TransactionWithContext, error)
	// ReadTransaction executes the given unit of work in a AccessModeRead transaction with
	// retry logic in place
	ReadTransaction(ctx context.Context, work TransactionWorkWithContext, configurers ...func(*TransactionConfig)) (interface{}, error)
	// WriteTransaction executes the given unit of work in a AccessModeWrite transaction with
	// retry logic in place
	WriteTransaction(ctx context.Context, work TransactionWorkWithContext, configurers ...func(*TransactionConfig)) (interface{}, error)
	// Run executes an auto-commit statement and returns a result
	Run(ctx context.Context, cypher string, params map[string]interface{}, configurers ...func(*TransactionConfig)) (ResultWithContext, error)
	// Close closes any open resources and marks this session as unusable
	Close(ctx context.Context) error

	legacy() Session
}

// SessionConfig is used to configure a new session, its zero value uses safe defaults.
type SessionConfig struct {
	// AccessMode used when using Session.Run and explicit transactions. Used to route query to
	// to read or write servers when running in a cluster. Session.ReadTransaction and Session.WriteTransaction
	// does not rely on this mode.
	AccessMode AccessMode
	// Bookmarks are the initial bookmarks used to ensure that the executing server is at least up
	// to date to the point represented by the latest of the provided bookmarks. After running commands
	// on the session the bookmark can be retrieved with Session.LastBookmark. All commands executing
	// within the same session will automatically use the bookmark from the previous command in the
	// session.
	Bookmarks Bookmarks
	// DatabaseName contains the name of the database that the commands in the session will execute on.
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
	// ImpersonatedUser sets the Neo4j user that the session should be acting as. If not set the user used to create
	// the driver will be used. If user impersonation is used the the default database for the impersonated user
	// will be used. Beware that using impersonation and not explicitly setting the database name and routing enabled
	// will cause the driver to query the cluster for the name of the default database for the impersonated user to
	// be able to route to the correct instance.
	ImpersonatedUser string
}

// Turns off fetching records in batches.
const FetchAll = -1

// Lets the driver decide fetch size
const FetchDefault = 0

// Connection pool as seen by the session.
type sessionPool interface {
	Borrow(ctx context.Context, serverNames []string, wait bool, boltLogger log.BoltLogger) (db.Connection, error)
	Return(ctx context.Context, c db.Connection)
	CleanUp(ctx context.Context)
}

type sessionWithContext struct {
	config           *Config
	defaultMode      db.AccessMode
	bookmarks        []string
	databaseName     string
	impersonatedUser string
	getDefaultDbName bool
	pool             sessionPool
	router           sessionRouter
	txExplicit       *transactionWithContext
	txAuto           *autoTransactionWithContext
	sleep            func(d time.Duration)
	now              func() time.Time
	logId            string
	log              log.Logger
	throttleTime     time.Duration
	fetchSize        int
	boltLogger       log.BoltLogger
}

// Remove empty string bookmarks to check for "bad" callers
// To avoid allocating, first check if this is a problem
func cleanupBookmarks(bookmarks []string) []string {
	hasBad := false
	for _, b := range bookmarks {
		if len(b) == 0 {
			hasBad = true
			break
		}
	}

	if !hasBad {
		return bookmarks
	}

	cleaned := make([]string, 0, len(bookmarks)-1)
	for _, b := range bookmarks {
		if len(b) > 0 {
			cleaned = append(cleaned, b)
		}
	}
	return cleaned
}

func newSessionWithContext(config *Config, sessConfig SessionConfig, router sessionRouter, pool sessionPool, logger log.Logger) *sessionWithContext {
	logId := log.NewId()
	logger.Debugf(log.Session, logId, "Created with context")

	fetchSize := config.FetchSize
	if sessConfig.FetchSize != FetchDefault {
		fetchSize = sessConfig.FetchSize
	}

	return &sessionWithContext{
		config:           config,
		router:           router,
		pool:             pool,
		defaultMode:      db.AccessMode(sessConfig.AccessMode),
		bookmarks:        cleanupBookmarks(sessConfig.Bookmarks),
		databaseName:     sessConfig.DatabaseName,
		impersonatedUser: sessConfig.ImpersonatedUser,
		getDefaultDbName: sessConfig.DatabaseName == "",
		sleep:            time.Sleep,
		now:              time.Now,
		log:              logger,
		logId:            logId,
		throttleTime:     time.Second * 1,
		fetchSize:        fetchSize,
		boltLogger:       sessConfig.BoltLogger,
	}
}

func (s *sessionWithContext) LastBookmarks() Bookmarks {
	// Pick up bookmark from pending auto-commit if there is a bookmark on it
	if s.txAuto != nil {
		s.retrieveBookmarks(s.txAuto.conn)
	}

	// Report bookmarks from previously closed connection or from initial set
	return s.bookmarks
}

func (s *sessionWithContext) lastBookmark() string {
	// Pick up bookmark from pending auto-commit if there is a bookmark on it
	if s.txAuto != nil {
		s.retrieveBookmarks(s.txAuto.conn)
	}

	// Report bookmark from previously closed connection or from initial set
	if len(s.bookmarks) > 0 {
		return s.bookmarks[len(s.bookmarks)-1]
	}

	return ""
}

func (s *sessionWithContext) BeginTransaction(ctx context.Context, configurers ...func(*TransactionConfig)) (TransactionWithContext, error) {
	// Guard for more than one transaction per session
	if s.txExplicit != nil {
		err := &UsageError{Message: "Session already has a pending transaction"}
		s.log.Error(log.Session, s.logId, err)
		return nil, err
	}

	if s.txAuto != nil {
		s.txAuto.done(ctx)
	}

	// Apply configuration functions
	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	// Get a connection from the pool. This could fail in clustered environment.
	conn, err := s.getConnection(ctx, s.defaultMode)
	if err != nil {
		return nil, err
	}

	// Begin transaction
	txHandle, err := conn.TxBegin(ctx,
		db.TxConfig{
			Mode:             s.defaultMode,
			Bookmarks:        s.bookmarks,
			Timeout:          config.Timeout,
			Meta:             config.Metadata,
			ImpersonatedUser: s.impersonatedUser,
		})
	if err != nil {
		s.pool.Return(ctx, conn)
		return nil, wrapError(err)
	}

	// Create transaction wrapper
	s.txExplicit = &transactionWithContext{
		conn:      conn,
		fetchSize: s.fetchSize,
		txHandle:  txHandle,
		onClosed: func() {
			// On transaction closed (rolled back or committed)
			s.retrieveBookmarks(conn)
			s.pool.Return(ctx, conn)
			s.txExplicit = nil
		},
	}

	return s.txExplicit, nil
}

func (s *sessionWithContext) runRetriable(
	ctx context.Context,
	mode db.AccessMode,
	work TransactionWorkWithContext, configurers ...func(*TransactionConfig)) (interface{}, error) {

	// Guard for more than one transaction per session
	if s.txExplicit != nil {
		err := &UsageError{Message: "Session already has a pending transaction"}
		return nil, err
	}

	if s.txAuto != nil {
		s.txAuto.done(ctx)
	}

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	state := retry.State{
		MaxTransactionRetryTime: s.config.MaxTransactionRetryTime,
		Log:                     s.log,
		LogName:                 log.Session,
		LogId:                   s.logId,
		Now:                     s.now,
		Sleep:                   s.sleep,
		Throttle:                retry.Throttler(s.throttleTime),
		MaxDeadConnections:      s.config.MaxConnectionPoolSize,
		Router:                  s.router,
		DatabaseName:            s.databaseName,
	}
	for state.Continue() {
		// Establish new connection
		conn, err := s.getConnection(ctx, mode)
		if err != nil {
			state.OnFailure(conn, err, false)
			continue
		}

		// Begin transaction
		txHandle, err := conn.TxBegin(ctx,
			db.TxConfig{
				Mode:             mode,
				Bookmarks:        s.bookmarks,
				Timeout:          config.Timeout,
				Meta:             config.Metadata,
				ImpersonatedUser: s.impersonatedUser,
			})
		if err != nil {
			state.OnFailure(conn, err, false)
			s.pool.Return(ctx, conn)
			continue
		}

		// Construct a transaction like thing for client to execute stuff on
		// and invoke the client work function.
		tx := retryableTransactionWithContext{conn: conn, fetchSize: s.fetchSize, txHandle: txHandle}
		x, err := work(&tx)
		// Evaluate the returned error from all the work for retryable, this means
		// that client can mess up the error handling.
		if err != nil {
			// If the client returns a client specific error that means that
			// client wants to rollback. We don't do an explicit rollback here
			// but instead rely on the pool invoking reset on the connection,
			// that will do an implicit rollback.
			state.OnFailure(conn, err, false)
			s.pool.Return(ctx, conn)
			continue
		}

		// Commit transaction
		err = conn.TxCommit(ctx, txHandle)
		if err != nil {
			state.OnFailure(conn, err, true)
			s.pool.Return(ctx, conn)
			continue
		}

		// Collect bookmark and return connection to pool
		s.retrieveBookmarks(conn)
		s.pool.Return(ctx, conn)

		// All well
		return x, nil
	}

	// When retries has occurred wrap the error, the last error is always added but
	// cause is only set when the retry logic could detect something strange.
	if state.LastErrWasRetryable {
		err := newTransactionExecutionLimit(state.Errs, state.Causes)
		s.log.Error(log.Session, s.logId, err)
		return nil, err
	}
	// Wrap and log the error if it belongs to the driver
	err := wrapError(state.LastErr)
	switch err.(type) {
	case *UsageError, *ConnectivityError:
		s.log.Error(log.Session, s.logId, err)
	}
	return nil, err
}

func (s *sessionWithContext) ReadTransaction(ctx context.Context,
	work TransactionWorkWithContext, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(ctx, db.ReadMode, work, configurers...)
}

func (s *sessionWithContext) WriteTransaction(ctx context.Context,
	work TransactionWorkWithContext, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(ctx, db.WriteMode, work, configurers...)
}

func (s *sessionWithContext) getServers(ctx context.Context, mode db.AccessMode) ([]string, error) {
	if mode == db.ReadMode {
		return s.router.Readers(ctx, s.bookmarks, s.databaseName, s.boltLogger)
	} else {
		return s.router.Writers(ctx, s.bookmarks, s.databaseName, s.boltLogger)
	}
}

func (s *sessionWithContext) getConnection(ctx context.Context, mode db.AccessMode) (db.Connection, error) {
	if s.config.ConnectionAcquisitionTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.config.ConnectionAcquisitionTimeout)
		if cancel != nil {
			defer cancel()
		}
		s.log.Debugf(log.Session, s.logId, "connection acquisition timeout is: %s",
			s.config.ConnectionAcquisitionTimeout.String())
		if deadline, ok := ctx.Deadline(); ok {
			s.log.Debugf(log.Session, s.logId, "connection acquisition resolved deadline is: %s",
				deadline.String())
		}
	}
	// If client requested user impersonation but provided no database we need to retrieve
	// the name of the configured default database for that user before asking for a connection
	if s.getDefaultDbName {
		defaultDb, err := s.router.GetNameOfDefaultDatabase(ctx, s.bookmarks, s.impersonatedUser, s.boltLogger)
		if err != nil {
			return nil, wrapError(err)
		}
		s.log.Debugf(log.Session, s.logId, "Retrieved default database for impersonated user, uses db '%s'", defaultDb)
		s.databaseName = defaultDb
		s.getDefaultDbName = false
	}
	servers, err := s.getServers(ctx, mode)
	if err != nil {
		return nil, wrapError(err)
	}

	conn, err := s.pool.Borrow(ctx, servers, s.config.ConnectionAcquisitionTimeout != 0, s.boltLogger)
	if err != nil {
		return nil, wrapError(err)
	}

	// Select database on server
	if s.databaseName != db.DefaultDatabase {
		dbSelector, ok := conn.(db.DatabaseSelector)
		if !ok {
			s.pool.Return(ctx, conn)
			return nil, &UsageError{Message: "Database does not support multi-database"}
		}
		dbSelector.SelectDatabase(s.databaseName)
	}

	return conn, nil
}

func (s *sessionWithContext) retrieveBookmarks(conn db.Connection) {
	if conn == nil {
		return
	}
	bookmark := conn.Bookmark()
	if len(bookmark) > 0 {
		s.bookmarks = []string{bookmark}
	}
}

func (s *sessionWithContext) Run(ctx context.Context,
	cypher string, params map[string]interface{}, configurers ...func(*TransactionConfig)) (ResultWithContext, error) {

	if s.txExplicit != nil {
		err := &UsageError{Message: "Trying to run auto-commit transaction while in explicit transaction"}
		s.log.Error(log.Session, s.logId, err)
		return nil, err
	}

	if s.txAuto != nil {
		s.txAuto.done(ctx)
	}

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	var (
		conn db.Connection
		err  error
	)
	for {
		conn, err = s.getConnection(ctx, s.defaultMode)
		if err != nil {
			return nil, err
		}
		err = conn.ForceReset(ctx)
		if err == nil {
			break
		}
	}

	stream, err := conn.Run(
		ctx,
		db.Command{
			Cypher:    cypher,
			Params:    params,
			FetchSize: s.fetchSize,
		},
		db.TxConfig{
			Mode:             s.defaultMode,
			Bookmarks:        s.bookmarks,
			Timeout:          config.Timeout,
			Meta:             config.Metadata,
			ImpersonatedUser: s.impersonatedUser,
		})
	if err != nil {
		s.pool.Return(ctx, conn)
		return nil, wrapError(err)
	}

	s.txAuto = &autoTransactionWithContext{
		conn: conn,
		res:  newResultWithContext(conn, stream, cypher, params),
		onClosed: func() {
			s.retrieveBookmarks(conn)
			s.pool.Return(ctx, conn)
			s.txAuto = nil
		},
	}

	return s.txAuto.res, nil
}

func (s *sessionWithContext) Close(ctx context.Context) error {
	var err error

	if s.txExplicit != nil {
		err = s.txExplicit.Close(ctx)
	}

	if s.txAuto != nil {
		s.txAuto.discard(ctx)
	}

	s.log.Debugf(log.Session, s.logId, "Closed")

	// Schedule cleanups
	go func() {
		s.pool.CleanUp(ctx)
		s.router.CleanUp()
	}()
	return err
}

func (s *sessionWithContext) legacy() Session {
	return &session{delegate: s}
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
func (s *erroredSessionWithContext) BeginTransaction(context.Context, ...func(*TransactionConfig)) (TransactionWithContext, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) ReadTransaction(context.Context, TransactionWorkWithContext, ...func(*TransactionConfig)) (interface{}, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) WriteTransaction(context.Context, TransactionWorkWithContext, ...func(*TransactionConfig)) (interface{}, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) Run(context.Context, string, map[string]interface{}, ...func(*TransactionConfig)) (ResultWithContext, error) {
	return nil, s.err
}
func (s *erroredSessionWithContext) Close(context.Context) error {
	return s.err
}
func (s *erroredSessionWithContext) legacy() Session {
	return &erroredSession{err: s.err}
}
