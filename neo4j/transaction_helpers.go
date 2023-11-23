/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
)

type ManagedTransactionWorkT[T any] func(tx ManagedTransaction) (T, error)

// ExecuteRead executes the given unit of work in a read transaction with
// retry logic in place, via the provided session.
//
// This is the generic variant of SessionWithContext.ExecuteRead.
//
// If an error occurs, the zero value of T is returned.
func ExecuteRead[T any](ctx context.Context, session SessionWithContext,
	work ManagedTransactionWorkT[T],
	configurers ...func(config *TransactionConfig)) (T, error) {

	return castGeneric[T](session.ExecuteRead(ctx, transactionWorkAdapter(work), configurers...))
}

// ExecuteWrite executes the given unit of work in a write transaction with
// retry logic in place, via the provided session.
//
// This is the generic variant of SessionWithContext.ExecuteWrite.
//
// If an error occurs, the zero value of T is returned.
func ExecuteWrite[T any](ctx context.Context, session SessionWithContext,
	work ManagedTransactionWorkT[T],
	configurers ...func(config *TransactionConfig)) (T, error) {

	return castGeneric[T](session.ExecuteWrite(ctx, transactionWorkAdapter(work), configurers...))
}

func transactionWorkAdapter[T any](work ManagedTransactionWorkT[T]) ManagedTransactionWork {
	return func(tx ManagedTransaction) (any, error) {
		return work(tx)
	}
}

// castGeneric performs a type assertion on the given `result` to the generic type T, unless an error has occurred.
//
// Implementation note: the function currently assumes that `result` is compatible with T and does not perform a soft
// assertion.
//
// For instance, the following code would currently panic instead of returning an error:
//
//	str, err := castGeneric[string](42, nil)
func castGeneric[T any](result any, err error) (T, error) {
	if err != nil {
		return *new(T), err
	}
	return result.(T), nil
}
