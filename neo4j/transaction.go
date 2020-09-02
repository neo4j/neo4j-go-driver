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
	"errors"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

// Transaction represents a transaction in the Neo4j database
type Transaction interface {
	// Run executes a statement on this transaction and returns a result
	Run(cypher string, params map[string]interface{}) (Result, error)
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
	// Close rolls back the actual transaction if it's not already committed/rolled back
	// and closes all resources associated with this transaction
	Close() error
}

// Transaction implementation when explicit transaction started
type transaction struct {
	conn     db.Connection
	txHandle db.Handle
	res      *result
	done     bool
	err      error
	onClosed func(bool)
}

type onClosedCallback func(committed bool)

func beginTransaction(
	conn db.Connection, mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{},
	onClosed onClosedCallback) (*transaction, error) {

	txHandle, err := conn.TxBegin(mode, bookmarks, timeout, meta)
	if err != nil {
		return nil, err
	}

	return &transaction{
		conn:     conn,
		txHandle: txHandle,
		onClosed: onClosed,
	}, nil
}

func (tx *transaction) Run(cypher string, params map[string]interface{}) (Result, error) {
	err := fetchAllInResult(&tx.res)
	if err != nil {
		return nil, err
	}

	stream, err := tx.conn.RunTx(tx.txHandle, cypher, params)
	if err != nil {
		return nil, err
	}
	tx.res = newResult(tx.conn, stream, cypher, params)
	return tx.res, nil
}

func (tx *transaction) Commit() error {
	if tx.done {
		return tx.err
	}
	tx.err = tx.conn.TxCommit(tx.txHandle)
	tx.done = true
	tx.onClosed(tx.err == nil)
	return tx.err
}

func (tx *transaction) Rollback() error {
	if tx.done {
		return tx.err
	}
	tx.err = tx.conn.TxRollback(tx.txHandle)
	tx.done = true
	tx.onClosed(false)
	return tx.err
}

func (tx *transaction) Close() error {
	return tx.Rollback()
}

func fetchAllInResult(respp **result) error {
	res := *respp
	if res == nil {
		return nil
	}
	res.fetchAll()
	*respp = nil
	return res.err
}

// Transaction implementation used as parameter to transactional functions
type retryableTransaction struct {
	conn     db.Connection
	txHandle db.Handle
	res      *result
}

func (tx *retryableTransaction) Run(cypher string, params map[string]interface{}) (Result, error) {
	// Fetch all in previous result
	err := fetchAllInResult(&tx.res)
	if err != nil {
		return nil, err
	}

	stream, err := tx.conn.RunTx(tx.txHandle, cypher, params)
	if err != nil {
		return nil, err
	}
	tx.res = newResult(tx.conn, stream, cypher, params)
	return tx.res, nil
}

func (tx *retryableTransaction) Commit() error {
	return errors.New("Commit not allowed on retryable transaction")
}

func (tx *retryableTransaction) Rollback() error {
	return errors.New("Rollback not allowed on retryable transaction")
}

func (tx *retryableTransaction) Close() error {
	return errors.New("Close not allowed on retryable transaction")
}
