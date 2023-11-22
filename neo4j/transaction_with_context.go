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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
)

// ManagedTransaction represents a transaction managed by the driver and operated on by the user, via transaction functions
type ManagedTransaction interface {
	// Run executes a statement on this transaction and returns a result
	Run(ctx context.Context, cypher string, params map[string]any) (ResultWithContext, error)

	legacy() Transaction
}

// ExplicitTransaction represents a transaction in the Neo4j database
type ExplicitTransaction interface {
	// Run executes a statement on this transaction and returns a result
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Run(ctx context.Context, cypher string, params map[string]any) (ResultWithContext, error)
	// Commit commits the transaction
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Commit(ctx context.Context) error
	// Rollback rolls back the transaction
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Rollback(ctx context.Context) error
	// Close rolls back the actual transaction if it's not already committed/rolled back
	// and closes all resources associated with this transaction
	// Contexts terminating too early negatively affect connection pooling and degrade the driver performance.
	Close(ctx context.Context) error

	// legacy returns the non-cancelling, legacy variant of this ExplicitTransaction type
	// This is used so that legacy transaction functions can delegate work to their newer, context-aware variants
	legacy() Transaction
}

type transactionState struct {
	err                 error
	resultErrorHandlers []func(error)
}

func (t *transactionState) onError(err error) {
	t.err = err
	for _, resultErrorHandler := range t.resultErrorHandlers {
		resultErrorHandler(err)
	}
}

// Transaction implementation when explicit transaction started
type explicitTransaction struct {
	conn      db.Connection
	fetchSize int
	txHandle  db.TxHandle
	txState   *transactionState
	onClosed  func()
}

func (tx *explicitTransaction) Run(ctx context.Context, cypher string, params map[string]any) (ResultWithContext, error) {
	if tx.conn == nil {
		return nil, transactionAlreadyCompletedError()
	}
	stream, err := tx.conn.RunTx(ctx, tx.txHandle, db.Command{Cypher: cypher, Params: params, FetchSize: tx.fetchSize})
	if err != nil {
		tx.txState.onError(err)
		return nil, errorutil.WrapError(tx.txState.err)
	}
	// no result consumption hook here since bookmarks are sent after commit, not after pulling results
	result := newResultWithContext(tx.conn, stream, cypher, params, tx.txState, nil)
	tx.txState.resultErrorHandlers = append(tx.txState.resultErrorHandlers, result.errorHandler)
	return result, nil
}

func (tx *explicitTransaction) Commit(ctx context.Context) error {
	if tx.txState.err != nil {
		return transactionAlreadyCompletedError()
	}
	if tx.conn == nil {
		return transactionAlreadyCompletedError()
	}
	tx.txState.err = tx.conn.TxCommit(ctx, tx.txHandle)
	tx.onClosed()
	return errorutil.WrapError(tx.txState.err)
}

func (tx *explicitTransaction) Close(ctx context.Context) error {
	if tx.conn == nil {
		// repeated calls to Close => NOOP
		return nil
	}
	return tx.Rollback(ctx)
}

func (tx *explicitTransaction) Rollback(ctx context.Context) error {
	if tx.txState.err != nil {
		return nil
	}
	if tx.conn == nil {
		return transactionAlreadyCompletedError()
	}
	if !tx.conn.IsAlive() || tx.conn.HasFailed() {
		// tx implicitly rolled back by having failed
		tx.txState.err = nil
	} else {
		tx.txState.err = tx.conn.TxRollback(ctx, tx.txHandle)
	}
	tx.onClosed()
	return errorutil.WrapError(tx.txState.err)
}

func (tx *explicitTransaction) legacy() Transaction {
	return &transaction{
		delegate: tx,
	}
}

// ManagedTransaction implementation used as parameter to transactional functions
type managedTransaction struct {
	conn      db.Connection
	fetchSize int
	txHandle  db.TxHandle
	txState   *transactionState
}

func (tx *managedTransaction) Run(ctx context.Context, cypher string, params map[string]any) (ResultWithContext, error) {
	stream, err := tx.conn.RunTx(ctx, tx.txHandle, db.Command{Cypher: cypher, Params: params, FetchSize: tx.fetchSize})
	if err != nil {
		return nil, errorutil.WrapError(err)
	}
	// no result consumption hook here since bookmarks are sent after commit, not after pulling results
	return newResultWithContext(tx.conn, stream, cypher, params, tx.txState, nil), nil
}

// legacy interop only - remove in 6.0
func (tx *managedTransaction) Commit(context.Context) error {
	return &UsageError{Message: "Commit not allowed on retryable transaction"}
}

// legacy interop only - remove in 6.0
func (tx *managedTransaction) Rollback(context.Context) error {
	return &UsageError{Message: "Rollback not allowed on retryable transaction"}
}

// legacy interop only - remove in 6.0
func (tx *managedTransaction) Close(context.Context) error {
	return &UsageError{Message: "Close not allowed on retryable transaction"}
}

// legacy interop only - remove in 6.0
func (tx *managedTransaction) legacy() Transaction {
	return &transaction{
		delegate: tx,
	}
}

// Represents an auto commit transaction.
// Does not implement the ExplicitTransaction nor the ManagedTransaction interface.
type autocommitTransaction struct {
	conn     db.Connection
	res      ResultWithContext
	closed   bool
	onClosed func()
}

func (tx *autocommitTransaction) done(ctx context.Context) {
	if !tx.closed {
		tx.res.buffer(ctx)
		tx.closed = true
		tx.onClosed()
	}
}

func (tx *autocommitTransaction) discard(ctx context.Context) {
	if !tx.closed {
		tx.res.Consume(ctx)
		tx.closed = true
		tx.onClosed()
	}
}

func transactionAlreadyCompletedError() *UsageError {
	return &UsageError{Message: "cannot use this transaction, because it has been committed or rolled back either because of an error or explicit termination"}
}
