/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
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

import "context"

type ManagedTransactionWorkT[T any] func(tx ManagedTransaction) (T, error)

func ExecuteRead[T any](ctx context.Context, session SessionWithContext,
	work ManagedTransactionWorkT[T],
	configurers ...func(config *TransactionConfig)) (T, error) {

	return castGeneric[T](session.ExecuteRead(ctx, transactionWorkAdapter(work), configurers...))
}

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

func castGeneric[T any](result any, err error) (T, error) {
	if err != nil {
		return *new(T), err
	}
	return result.(T), nil
}
