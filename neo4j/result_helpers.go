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
	"fmt"
)

// SingleTWithContext maps the single record left to an instance of T with the provided mapper function.
// It relies on ResultWithContext.Single and propagate its error, if any.
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func SingleTWithContext[T any](ctx context.Context, result ResultWithContext, mapper func(*Record) (T, error)) (T, error) {
	single, err := result.Single(ctx)
	if err != nil {
		return *new(T), err
	}
	return mapper(single)
}

// SingleT maps the single record left to an instance of T with the provided mapper function.
// It relies on Result.Single and propagate its error, if any.
//
// Deprecated: use SingleTWithContext instead (the entry point of context-aware
// APIs is NewDriverWithContext)
func SingleT[T any](result Result, mapper func(*Record) (T, error)) (T, error) {
	single, err := result.Single()
	if err != nil {
		return *new(T), err
	}
	return mapper(single)
}

// CollectTWithContext maps the records to a slice of T with the provided mapper function.
// It relies on ResultWithContext.Collect and propagate its error, if any.
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func CollectTWithContext[T any](ctx context.Context, result ResultWithContext, mapper func(*Record) (T, error)) ([]T, error) {
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return mapAll(records, mapper)
}

// CollectT maps the records to a slice of T with the provided mapper function.
// It relies on Result.Collect and propagate its error, if any.
//
// Deprecated: use CollectTWithContext instead (the entry point of context-aware
// APIs is NewDriverWithContext)
func CollectT[T any](result Result, mapper func(*Record) (T, error)) ([]T, error) {
	records, err := result.Collect()
	if err != nil {
		return nil, err
	}
	return mapAll(records, mapper)
}

// Single returns one and only one record from the result stream. Any error passed in
// or reported while navigating the result stream is returned without any conversion.
// If the result stream contains zero or more than one records error is returned.
//
//	record, err := neo4j.Single(session.Run(...))
func Single(result Result, err error) (*Record, error) {
	if err != nil {
		return nil, err
	}
	return result.Single()
}

// Collect aggregates the records into a slice.
// It relies on Result.Collect and propagate its error, if any.
//
//	records, err := neo4j.Collect(session.Run(...))
//
// Deprecated: use CollectWithContext instead (the entry point of context-aware
// APIs is NewDriverWithContext)
func Collect(result Result, err error) ([]*Record, error) {
	if err != nil {
		return nil, err
	}
	return result.Collect()
}

// CollectWithContext aggregates the records into a slice.
// It relies on ResultWithContext.Collect and propagate its error, if any.
//
//	result, err := session.Run(...)
//	records, err := neo4j.CollectWithContext(ctx, result, err)
//
// It accepts a context.Context, which may be canceled or carry a deadline, to control the overall record fetching
// execution time.
func CollectWithContext(ctx context.Context, result ResultWithContext, err error) ([]*Record, error) {
	if err != nil {
		return nil, err
	}
	return result.Collect(ctx)
}

// AsRecords passes any existing error or casts from to a slice of records.
// Use in combination with Collect and transactional functions:
//
//	records, err := neo4j.AsRecords(session.ExecuteRead(func (tx neo4j.Transaction) {
//	    return neo4j.Collect(tx.Run(...))
//	}))
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
