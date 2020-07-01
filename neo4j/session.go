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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/log"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
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
	Borrow(ctx context.Context, serverNames []string, wait bool) (pool.Connection, error)
	Return(c pool.Connection)
	CleanUp()
}

type session struct {
	config       *Config
	defaultMode  connection.AccessMode
	bookmarks    []string
	databaseName string
	pool         sessionPool
	router       sessionRouter
	conn         connection.Connection
	inTx         bool
	res          *result
	sleep        func(d time.Duration)
	now          func() time.Time
	logId        string
	log          log.Logger
	throttleTime time.Duration
}

var sessionid uint32

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
	mode connection.AccessMode, bookmarks []string, databaseName string, logger log.Logger) *session {

	id := atomic.AddUint32(&sessionid, 1)
	logId := fmt.Sprintf("sess %d", id)
	logger.Debugf(logId, "Created")

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
		s.log.Error(s.logId, err)
		return nil, err
	}

	// Consume current result if any
	err := s.consumeCurrent()
	if err != nil {
		return nil, err
	}

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	s.returnConn()

	return s.beginTransaction(s.defaultMode, &config)
}

func (s *session) beginTransaction(mode connection.AccessMode, config *TransactionConfig) (Transaction, error) {
	// Ensure that the session has a connection
	err := s.borrowConn(mode)
	if err != nil {
		return nil, err
	}
	conn := s.conn

	// Start a transaction on the connection
	txHandle, err := s.conn.TxBegin(mode, s.bookmarks, config.Timeout, config.Metadata)
	if err != nil {
		return nil, err
	}
	s.inTx = true

	// Wrap the returned transaction in something directly connected to this instance to
	// make sure state is synced. Only one active transaction per session is allowed.
	// This wrapped transaction is bound to the local txHandle so if a transaction is
	// kept around after the connection has been returned to the pool it will not mess
	// with the connection since the handle is invalid. Also use local connection variable
	// to avoid relying on state.
	return &transaction{
		run: func(cypher string, params map[string]interface{}) (Result, error) {
			// The last result should receive all records
			err := s.consumeCurrent()
			if err != nil {
				return nil, err
			}

			streamHandle, err := conn.RunTx(txHandle, cypher, params)
			if err != nil {
				// To be backwards compatible we delay the error here if it is a database error.
				// The old implementation just sent all the commands and didn't wait for an answer
				// until starting to consume or iterate.
				if _, isDbErr := err.(*connection.DatabaseError); isDbErr {
					return &delayedErrorResult{err: err}, nil
				}
				return nil, err
			}
			s.res = newResult(conn, streamHandle, cypher, params)
			return s.res, nil
		},
		commit: func() error {
			// The last result should receive all records
			if s.res != nil {
				s.res.fetchAll()
				s.res = nil
			}

			err := conn.TxCommit(txHandle)
			s.inTx = false
			return err
		},
		rollback: func() error {
			err := conn.TxRollback(txHandle)
			s.inTx = false
			return err
		},
	}, nil
}

func (s *session) runOneTry(mode connection.AccessMode, work TransactionWork, config *TransactionConfig) (interface{}, error) {
	tx, err := s.beginTransaction(mode, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		tx.Close()
	}()

	x, err := work(tx)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return x, nil
}

func (s *session) runRetriable(
	mode connection.AccessMode,
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	// Guard for more than one transaction per session
	if s.inTx {
		return nil, errors.New("Already in tx")
	}

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	// Consume current result if any
	err := s.consumeCurrent()
	if err != nil {
		return nil, err
	}

	var (
		maxDeadErrors    = s.config.MaxConnectionPoolSize / 2
		maxClusterErrors = 1
		throttle         = throttler(s.throttleTime)
		start            time.Time
	)
	for {
		// Always return the current connection before trying (again)
		s.returnConn()
		s.res = nil

		x, err := s.runOneTry(mode, work, &config)
		if err == nil {
			return x, nil
		}

		s.log.Debugf(s.logId, "Retriable transaction evaluating error: %s", err)

		// If we failed due to connect problem, just give up since the pool tries really hard
		if s.conn == nil {
			s.log.Errorf(s.logId, "Retriable transaction failed due to no available connection: %s", err)
			return nil, err
		}

		// Check retry timeout
		if start.IsZero() {
			start = s.now()
		}
		if time.Since(start) > s.config.MaxTransactionRetryTime {
			s.log.Errorf(s.logId, "Retriable transaction failed due to reaching MaxTransactionRetryTime: %s", s.config.MaxTransactionRetryTime.String())
			return nil, err
		}

		// Failed, check cause and determine next action

		// If the connection is dead just return the connection, get another and try again, no sleep
		if !s.conn.IsAlive() {
			maxDeadErrors--
			if maxDeadErrors < 0 {
				s.log.Errorf(s.logId, "Retriable transaction failed due to too many dead connections")
				return nil, err
			}
			s.log.Debugf(s.logId, "Retrying transaction due to dead connection")
			continue
		}

		// Check type of error, this assumes a well behaved transaction func, could be better
		// to keep last error in the connection in the future.
		switch e := err.(type) {
		case *connection.DatabaseError:
			switch {
			case e.IsRetriableCluster():
				// Force routing tables to be updated before trying again
				s.router.Invalidate(s.databaseName)
				maxClusterErrors--
				if maxClusterErrors < 0 {
					s.log.Errorf(s.logId, "Retriable transaction failed due to encountering too many cluster errors")
					return nil, err
				}
				s.log.Debugf(s.logId, "Retrying transaction due to cluster error")
			case e.IsRetriableTransient():
				throttle = throttle.next()
				d := throttle.delay()
				s.log.Debugf(s.logId, "Retrying transaction due to transient error after sleeping for %s", d.String())
				s.sleep(d)
			default:
				return nil, err
			}
		default:
			return nil, err
		}
	}
}

func (s *session) ReadTransaction(
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(connection.ReadMode, work, configurers...)
}

func (s *session) WriteTransaction(
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(connection.WriteMode, work, configurers...)
}

func (s *session) borrowConn(mode connection.AccessMode) error {
	var servers []string
	var err error
	if mode == connection.ReadMode {
		servers, err = s.router.Readers(s.databaseName)
	} else {
		servers, err = s.router.Writers(s.databaseName)
	}
	if err != nil {
		return err
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if s.config.ConnectionAcquisitionTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), s.config.ConnectionAcquisitionTimeout)
	} else {
		ctx = context.Background()
	}
	if cancel != nil {
		defer cancel()
	}
	conn, err := s.pool.Borrow(ctx, servers, s.config.ConnectionAcquisitionTimeout != 0)
	if err != nil {
		return err
	}
	s.conn = conn.(connection.Connection)

	// Select database on server
	if s.databaseName != connection.DefaultDatabase {
		dbSelector, ok := s.conn.(connection.DatabaseSelector)
		if !ok {
			return errors.New("Database does not support multidatabase")
		}
		dbSelector.SelectDatabase(s.databaseName)
	}
	return nil
}

func (s *session) returnConn() {
	if s.conn != nil {
		// Retrieve bookmark before returning connection, useful as input to next transaction
		// on another connection.
		bookmark := s.conn.Bookmark()
		if len(bookmark) > 0 {
			s.bookmarks = []string{bookmark}
		}

		s.pool.Return(s.conn)
		s.conn = nil
	}
}

func (s *session) consumeCurrent() error {
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
		s.log.Error(s.logId, err)
		return nil, err
	}

	err := s.consumeCurrent()
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
		// To be backwards compatible we delay the error here if it is a database error.
		// The old implementation just sent all the commands and didn't wait for an answer
		// until starting to consume or iterate.
		if _, isDbErr := err.(*connection.DatabaseError); isDbErr {
			return &delayedErrorResult{err: err}, nil
		}
		return nil, err
	}
	s.res = newResult(s.conn, stream, cypher, params)
	return s.res, nil
}

func (s *session) Close() error {
	s.consumeCurrent()
	s.returnConn()
	s.log.Debugf(s.logId, "Closed")
	// Schedule cleanups
	go func() {
		s.pool.CleanUp()
		s.router.CleanUp()
	}()
	return nil
}
