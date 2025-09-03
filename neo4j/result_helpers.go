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
)

// SingleT maps the single record left to an instance of T with the provided mapper function.
// It relies on Result.Single and propagate its error, if any.
//
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func SingleT[T any](ctx context.Context, result Result, mapper func(*Record) (T, error)) (T, error) {
	single, err := result.Single(ctx)
	if err != nil {
		return *new(T), err
	}
	return mapper(single)
}

// SingleTWithContext is an alias for SingleT to maintain backward compatibility
// for users who migrated from v5 to v6 using the WithContext APIs.
// In v6, SingleT is the primary function and is context-aware.
//
// Deprecated: please use SingleT instead. This alias will be removed in 7.0.
func SingleTWithContext[T any](ctx context.Context, result Result, mapper func(*Record) (T, error)) (T, error) {
	return SingleT(ctx, result, mapper)
}

// Single returns one and only one record from the result stream. Any error passed in
// or reported while navigating the result stream is returned without any conversion.
// If the result stream contains zero or more than one records error is returned.
//
//	result, err := session.Run(ctx, "...", nil)
//	record, err := neo4j.Single(ctx, result, err)
//
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func Single(ctx context.Context, result Result, err error) (*Record, error) {
	if err != nil {
		return nil, err
	}
	return result.Single(ctx)
}

// SingleWithContext is an alias for Single to maintain backward compatibility
// for users who migrated from v5 to v6 using the WithContext APIs.
// In v6, Single is the primary function and is context-aware.
//
// Deprecated: please use Single instead. This alias will be removed in 7.0.
func SingleWithContext(ctx context.Context, result Result, err error) (*Record, error) {
	return Single(ctx, result, err)
}

// CollectT maps the records to a slice of T with the provided mapper function.
// It relies on Result.Collect and propagate its error, if any.
//
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func CollectT[T any](ctx context.Context, result Result, mapper func(*Record) (T, error)) ([]T, error) {
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return mapAll(records, mapper)
}

// CollectTWithContext is an alias for CollectT to maintain backward compatibility
// for users who migrated from v5 to v6 using the WithContext APIs.
// In v6, CollectT is the primary function and is context-aware.
//
// Deprecated: please use CollectT instead. This alias will be removed in 7.0.
func CollectTWithContext[T any](ctx context.Context, result Result, mapper func(*Record) (T, error)) ([]T, error) {
	return CollectT(ctx, result, mapper)
}

// Collect aggregates the records into a slice.
// It relies on Result.Collect and propagate its error, if any.
//
//	result, err := session.Run(...)
//	records, err := neo4j.Collect(ctx, result, err)
//
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func Collect(ctx context.Context, result Result, err error) ([]*Record, error) {
	if err != nil {
		return nil, err
	}
	return result.Collect(ctx)
}

// CollectWithContext is an alias for Collect to maintain backward compatibility
// for users who migrated from v5 to v6 using the WithContext APIs.
// In v6, Collect is the primary function and is context-aware.
//
// Deprecated: please use Collect instead. This alias will be removed in 7.0.
func CollectWithContext(ctx context.Context, result Result, err error) ([]*Record, error) {
	return Collect(ctx, result, err)
}

// AsRecords passes any existing error or casts from to a slice of records.
// Use in combination with Collect and transactional functions:
//
//	records, err := neo4j.AsRecords(
//		session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
//			result, err := tx.Run(ctx, "...", nil)
//			return neo4j.Collect(ctx, result, err)
//		}),
//	)
func AsRecords(from any, err error) ([]*Record, error) {
	if err != nil {
		return nil, err
	}
	recs, ok := from.([]*Record)
	if !ok {
		return nil, &UsageError{
			Message: fmt.Sprintf("Expected type []*Record, not %T", from),
		}
	}
	return recs, nil
}

// AsRecord passes any existing error or casts from to a record.
// Use in combination with Single and transactional functions:
//
//	record, err := neo4j.AsRecord(session.ExecuteRead(func (tx neo4j.Transaction) {
//	    return neo4j.Single(tx.Run(...))
//	}))
func AsRecord(from any, err error) (*Record, error) {
	if err != nil {
		return nil, err
	}
	rec, ok := from.(*Record)
	if !ok {
		return nil, &UsageError{
			Message: fmt.Sprintf("Expected type *Record, not %T", from),
		}
	}
	return rec, nil
}

func mapAll[T any](records []*Record, mapper func(*Record) (T, error)) ([]T, error) {
	results := make([]T, len(records))
	for i, record := range records {
		mappedRecord, err := mapper(record)
		if err != nil {
			return nil, err
		}
		results[i] = mappedRecord
	}
	return results, nil
}
