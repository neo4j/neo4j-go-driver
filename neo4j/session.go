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
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/retry"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

// TransactionWork represents a unit of work that will be executed against the provided
// transaction
type TransactionWork func(tx Transaction) (interface{}, error)

// Session represents a logical connection (which is not tied to a physical connection)
// to the server
type Session interface {
	// LastBookmark returns the bookmark received following the last successfully completed transaction.
	// If no bookmark was received or if this transaction was rolled back, the bookmark value will not be changed.
	LastBookmark() string
	// BeginTransaction starts a new explicit transaction on this session
	BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error)
	// ReadTransaction executes the given unit of work in a AccessModeRead transaction with
	// retry logic in place
	ReadTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error)
	// WriteTransaction executes the given unit of work in a AccessModeWrite transaction with
	// retry logic in place
	WriteTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error)
	// Run executes an auto-commit statement and returns a result
	Run(cypher string, params map[string]interface{}, configurers ...func(*TransactionConfig)) (Result, error)
	// Close closes any open resources and marks this session as unusable
	Close() error
}

type SessionConfig struct {
	AccessMode   AccessMode
	Bookmarks    []string
	DatabaseName string
}

// Connection pool as seen by the session.
type sessionPool interface {
	Borrow(ctx context.Context, serverNames []string, wait bool) (db.Connection, error)
	Return(c db.Connection)
	CleanUp()
}

type session struct {
	config       *Config
	defaultMode  db.AccessMode
	bookmarks    []string
	databaseName string
	pool         sessionPool
	router       sessionRouter
	conn         db.Connection
	inTx         bool
	res          *result
	sleep        func(d time.Duration)
	now          func() time.Time
	logId        string
	log          log.Logger
	throttleTime time.Duration
}

var sessionid uint32
var sessionLogName = "session"

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

func newSession(config *Config, router sessionRouter, pool sessionPool,
	mode db.AccessMode, bookmarks []string, databaseName string, logger log.Logger) *session {

	id := atomic.AddUint32(&sessionid, 1)
	logId := strconv.FormatUint(uint64(id), 10)
	logger.Debugf(sessionLogName, logId, "Created")

	return &session{
		config:       config,
		router:       router,
		pool:         pool,
		defaultMode:  mode,
		bookmarks:    cleanupBookmarks(bookmarks),
		databaseName: databaseName,
		sleep:        time.Sleep,
		now:          time.Now,
		log:          logger,
		logId:        logId,
		throttleTime: time.Second * 1,
	}
}

func (s *session) LastBookmark() string {
	// Report bookmark on current connection if any
	if s.conn != nil {
		currentBookmark := s.conn.Bookmark()
		if len(currentBookmark) > 0 {
			return currentBookmark
		}
	}

	// Report bookmark from previously closed connection or from initial set
	if len(s.bookmarks) > 0 {
		return s.bookmarks[len(s.bookmarks)-1]
	}

	return ""
}

func (s *session) BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error) {
	// Guard for more than one transaction per session
	if s.inTx {
		err := errors.New("Already in transaction")
		s.log.Error(sessionLogName, s.logId, err)
		return nil, err
	}

	// Fetch all in current result if any
	err := s.fetchAllInCurrentResult()
	if err != nil {
		return nil, err
	}
	s.returnConn()

	// Apply configuration functions
	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	// Get a connection from the pool. This could fail in clustered environment.
	conn, err := s.getConnection(s.defaultMode)
	if err != nil {
		return nil, err
	}

	tx, err := beginTransaction(conn, s.defaultMode, s.bookmarks, config.Timeout, config.Metadata,
		// On transaction closed (rollbacked or committed)
		func(committed bool) {
			if committed {
				s.retrieveBookmarks(conn)
			}
			s.pool.Return(conn)
			s.inTx = false
		})
	if err != nil {
		return nil, err
	}
	s.inTx = true

	return tx, nil
}

func (s *session) runRetriable(
	mode db.AccessMode,
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	// Guard for more than one transaction per session
	if s.inTx {
		return nil, errors.New("Already in tx")
	}

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	// Fetch all in current result if any
	err := s.fetchAllInCurrentResult()
	if err != nil {
		return nil, err
	}
	s.returnConn()

	state := retry.State{
		MaxTransactionRetryTime: s.config.MaxTransactionRetryTime,
		Log:                     s.log,
		LogName:                 sessionLogName,
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
		conn, err := s.getConnection(mode)
		if err != nil {
			state.OnFailure(conn, err, false)
			s.pool.Return(conn)
			continue
		}

		// Begin transaction
		txHandle, err := conn.TxBegin(mode, s.bookmarks, config.Timeout, config.Metadata)
		if err != nil {
			state.OnFailure(conn, err, false)
			s.pool.Return(conn)
			continue
		}

		// Construct a transaction like thing for client to execute stuff on.
		// Evaluate the returned error from all the work for retryable, this means
		// that client can mess up the error handling.
		tx := retryableTransaction{conn: conn, txHandle: txHandle}
		x, err := work(&tx)
		if err != nil {
			state.OnFailure(conn, err, false)
			s.pool.Return(conn)
			continue
		}

		// Commit transaction
		err = conn.TxCommit(txHandle)
		if err != nil {
			state.OnFailure(conn, err, true)
			s.pool.Return(conn)
			continue
		}

		// Collect bookmark and return connection to pool
		s.retrieveBookmarks(conn)
		s.pool.Return(conn)

		return x, nil
	}
	return nil, state.Err
}

func (s *session) ReadTransaction(
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(db.ReadMode, work, configurers...)
}

func (s *session) WriteTransaction(
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(db.WriteMode, work, configurers...)
}

func (s *session) getServers(mode db.AccessMode) ([]string, error) {
	if mode == db.ReadMode {
		return s.router.Readers(s.databaseName)
	} else {
		return s.router.Writers(s.databaseName)
	}
}

func (s *session) getConnection(mode db.AccessMode) (db.Connection, error) {
	servers, err := s.getServers(mode)
	if err != nil {
		return nil, err
	}

	var ctx context.Context
	if s.config.ConnectionAcquisitionTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), s.config.ConnectionAcquisitionTimeout)
		if cancel != nil {
			defer cancel()
		}
	} else {
		ctx = context.Background()
	}
	conn, err := s.pool.Borrow(ctx, servers, s.config.ConnectionAcquisitionTimeout != 0)
	if err != nil {
		return nil, err
	}

	// Select database on server
	if s.databaseName != db.DefaultDatabase {
		dbSelector, ok := conn.(db.DatabaseSelector)
		if !ok {
			s.pool.Return(conn)
			return nil, errors.New("Database does not support multidatabase")
		}
		dbSelector.SelectDatabase(s.databaseName)
	}

	return conn, nil
}

func (s *session) borrowConn(mode db.AccessMode) error {
	conn, err := s.getConnection(mode)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

func (s *session) retrieveBookmarks(conn db.Connection) {
	if conn == nil {
		return
	}
	bookmark := conn.Bookmark()
	if len(bookmark) > 0 {
		s.bookmarks = []string{bookmark}
	}
}

func (s *session) returnConn() {
	if s.conn != nil {
		s.retrieveBookmarks(s.conn)
		s.pool.Return(s.conn)
		s.conn = nil
	}
}

func (s *session) fetchAllInCurrentResult() error {
	if s.res != nil {
		s.res.fetchAll()
		err := s.res.err
		s.res = nil
		return err
	}
	return nil
}

func (s *session) Run(
	cypher string, params map[string]interface{}, configurers ...func(*TransactionConfig)) (Result, error) {

	if s.inTx {
		err := errors.New("Trying to run auto-commit transaction while in explicit transaction")
		s.log.Error(sessionLogName, s.logId, err)
		return nil, err
	}

	err := s.fetchAllInCurrentResult()
	if err != nil {
		return nil, err
	}
	s.returnConn()

	err = s.borrowConn(s.defaultMode)
	if err != nil {
		return nil, err
	}

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	stream, err := s.conn.Run(cypher, params, s.defaultMode, s.bookmarks, config.Timeout, config.Metadata)
	if err != nil {
		s.returnConn()
		return nil, err
	}
	s.res = newResult(s.conn, stream, cypher, params)
	return s.res, nil
}

func (s *session) Close() error {
	s.fetchAllInCurrentResult()
	s.returnConn()
	s.log.Debugf(sessionLogName, s.logId, "Closed")
	// Schedule cleanups
	go func() {
		s.pool.CleanUp()
		s.router.CleanUp()
	}()
	return nil
}
