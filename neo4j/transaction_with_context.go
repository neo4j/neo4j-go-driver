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
)

// TransactionWithContext represents a transaction in the Neo4j database
type TransactionWithContext interface {
	// Run executes a statement on this transaction and returns a result
	Run(ctx context.Context, cypher string, params map[string]interface{}) (ResultWithContext, error)
	// Commit commits the transaction
	Commit(ctx context.Context) error
	// Rollback rolls back the transaction
	Rollback(ctx context.Context) error
	// Close rolls back the actual transaction if it's not already committed/rolled back
	// and closes all resources associated with this transaction
	Close(ctx context.Context) error

	// legacy returns the non-cancelling, legacy variant of this TransactionWithContext type
	// This is used so that legacy transaction functions can delegate work to their newer, context-aware variants
	legacy() Transaction
}

// Transaction implementation when explicit transaction started
type transactionWithContext struct {
	conn      db.Connection
	fetchSize int
	txHandle  db.TxHandle
	done      bool
	err       error
	onClosed  func()
}

func (tx *transactionWithContext) Run(ctx context.Context, cypher string,
	params map[string]interface{}) (ResultWithContext, error) {
	stream, err := tx.conn.RunTx(ctx, tx.txHandle, db.Command{Cypher: cypher, Params: params, FetchSize: tx.fetchSize})
	if err != nil {
		return nil, wrapError(err)
	}
	return newResultWithContext(tx.conn, stream, cypher, params), nil
}

func (tx *transactionWithContext) Commit(ctx context.Context) error {
	if tx.done {
		return tx.err
	}
	tx.err = tx.conn.TxCommit(ctx, tx.txHandle)
	tx.done = true
	tx.onClosed()
	return wrapError(tx.err)
}

func (tx *transactionWithContext) Close(ctx context.Context) error {
	return tx.Rollback(ctx)
}

func (tx *transactionWithContext) Rollback(ctx context.Context) error {
	if tx.done {
		return tx.err
	}
	if !tx.conn.IsAlive() || tx.conn.HasFailed() {
		// tx implicitly rolled back by having failed
		tx.err = nil
	} else {
		tx.err = tx.conn.TxRollback(ctx, tx.txHandle)
	}
	tx.done = true
	tx.onClosed()
	return wrapError(tx.err)
}

func (tx *transactionWithContext) legacy() Transaction {
	return &transaction{
		delegate: tx,
	}
}

// TransactionWithContext implementation used as parameter to transactional functions
type retryableTransactionWithContext struct {
	conn      db.Connection
	fetchSize int
	txHandle  db.TxHandle
}

func (tx *retryableTransactionWithContext) Run(ctx context.Context, cypher string, params map[string]interface{}) (ResultWithContext, error) {
	stream, err := tx.conn.RunTx(ctx, tx.txHandle, db.Command{Cypher: cypher, Params: params, FetchSize: tx.fetchSize})
	if err != nil {
		return nil, wrapError(err)
	}
	return newResultWithContext(tx.conn, stream, cypher, params), nil
}

func (tx *retryableTransactionWithContext) Commit(context.Context) error {
	return &UsageError{Message: "Commit not allowed on retryable transaction"}
}

func (tx *retryableTransactionWithContext) Rollback(context.Context) error {
	return &UsageError{Message: "Rollback not allowed on retryable transaction"}
}

func (tx *retryableTransactionWithContext) Close(context.Context) error {
	return &UsageError{Message: "Close not allowed on retryable transaction"}
}

func (tx *retryableTransactionWithContext) legacy() Transaction {
	return &transaction{
		delegate: tx,
	}
}

// Represents an auto commit transaction.
// Does not implement the TransactionWithContext interface.
type autoTransactionWithContext struct {
	conn     db.Connection
	res      *resultWithContext
	closed   bool
	onClosed func()
}

func (tx *autoTransactionWithContext) done(ctx context.Context) {
	if !tx.closed {
		tx.res.buffer(ctx)
		tx.closed = true
		tx.onClosed()
	}
}

func (tx *autoTransactionWithContext) discard(ctx context.Context) {
	if !tx.closed {
		tx.res.Consume(ctx)
		tx.closed = true
		tx.onClosed()
	}
}
