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

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
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
	//LastBookmark() string
	// BeginTransaction starts a new explicit transaction on this session
	BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error)
	// ReadTransaction executes the given unit of work in a AccessModeRead transaction with
	// retry logic in place
	ReadTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error)
	// WriteTransaction executes the given unit of work in a AccessModeWrite transaction with
	// retry logic in place
	WriteTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error)
	// Run executes an auto-commit statement and returns a result
	Run(cypher string, params map[string]interface{}) (Result, error) //, configurers ...func(*TransactionConfig)) (Result, error)
	// Close closes any open resources and marks this session as unusable
	Close() error
}

type session struct {
	config    *Config
	mode      conn.AccessMode
	bookmarks []string
	pool      *pool.Pool
	router    func() []string
	conn      conn.Connection
	inTx      bool
	res       *result
}

func newSession(
	config *Config, router func() []string, pool *pool.Pool,
	mode conn.AccessMode, bookmarks []string) *session {

	return &session{
		config:    config,
		router:    router,
		pool:      pool,
		mode:      mode,
		bookmarks: bookmarks,
	}
}

func (s *session) BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error) {
	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}
	return s.beginTransaction(s.mode, &config)
}

func (s *session) beginTransaction(mode conn.AccessMode, config *TransactionConfig) (Transaction, error) {
	// Guard for more than one transaction per session
	if s.inTx {
		return nil, errors.New("Already in tx")
	}

	// Consume current result if any
	if s.res != nil {
		s.res.fetchAll()
		s.res = nil
	}

	// Ensure that the session has a connection
	err := s.borrowConn()
	if err != nil {
		return nil, err
	}
	conn := s.conn

	// Start a transaction on the connection
	txHandle, err := s.conn.TxBegin(s.mode, s.bookmarks, config.Timeout, config.Metadata)
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
			if s.res != nil {
				s.res.fetchAll()
				s.res = nil
			}

			streamHandle, err := conn.RunTx(txHandle, cypher, patchInSliceX(params))
			if err != nil {
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
			if err == nil {
				s.inTx = false
			}
			return err
		},
		rollback: func() error {
			err := conn.TxRollback(txHandle)
			if err == nil {
				s.inTx = false
			}
			return err
		},
	}, nil
}

func (s *session) runRetriable(
	mode conn.AccessMode,
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	// TODO: Retry
	tx, err := s.beginTransaction(mode, &config)
	if err != nil {
		return nil, err
	}
	defer func() {
		tx.Close()
	}()

	x, err := work(tx)
	if err != nil {
		// Rely on rollback in tx.Close
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return x, nil
}

func (s *session) ReadTransaction(
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(conn.ReadMode, work, configurers...)
}

func (s *session) WriteTransaction(
	work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {

	return s.runRetriable(conn.WriteMode, work, configurers...)
}

func (s *session) borrowConn() (err error) {
	s.returnConn()
	servers := s.router()

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ConnectionAcquisitionTimeout)
	defer cancel()
	s.conn, err = s.pool.Borrow(ctx, servers)
	return
}

func (s *session) returnConn() {
	if s.conn != nil {
		s.pool.Return(s.conn)
		s.conn = nil
	}
}

func (s *session) Run(
	cypher string, params map[string]interface{}) (Result, error) {

	if s.inTx {
		return nil, errors.New("Trying run in tx")
	}
	if s.res != nil {
		s.res.fetchAll()
		s.res = nil
	}

	err := s.borrowConn()
	if err != nil {
		return nil, err
	}

	stream, err := s.conn.Run(cypher, patchInSliceX(params))
	if err != nil {
		s.returnConn()
		return nil, err
	}
	res, err := newResult(s.conn, stream, cypher, params), nil
	if err != nil {
		return nil, err
	}
	s.res = res
	return res, nil
}

func (s *session) Close() error {
	s.returnConn()
	return nil
}
