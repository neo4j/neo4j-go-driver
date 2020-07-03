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
	"container/list"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/connection"
)

type Result interface {
	// Keys returns the keys available on the result set.
	Keys() ([]string, error)
	// Next returns true only if there is a record to be processed.
	Next() bool
	// Err returns the latest error that caused this Next to return false.
	Err() error
	// Record returns the current record.
	Record() *Record
	// Summary returns the summary information about the statement execution.
	Summary() (ResultSummary, error)
	// Consume consumes the entire result and returns the summary information
	// about the statement execution.
	Consume() (ResultSummary, error)
}

type iterator interface {
	Next(s connection.Handle) (*Record, *connection.Summary, error)
}

type result struct {
	err         error
	iter        iterator
	stream      *connection.Stream
	cypher      string
	params      map[string]interface{}
	allReceived bool
	unconsumed  list.List
	record      *Record
	summary     *connection.Summary
}

func newResult(iter iterator, str *connection.Stream, cypher string, params map[string]interface{}) *result {
	return &result{
		iter:   iter,
		stream: str,
		cypher: cypher,
		params: params,
	}
}

// Receive another record.
func (r *result) doFetch() *Record {
	var rec *Record
	var sum *connection.Summary
	rec, sum, r.err = r.iter.Next(r.stream.Handle)
	r.allReceived = r.err != nil || rec == nil
	r.summary = sum
	return rec
}

func (r *result) Keys() ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.stream.Keys, nil
}

func (r *result) Next() bool {
	e := r.unconsumed.Front()
	if e == nil {
		// All has been received and consumed
		if r.allReceived {
			r.record = nil
			return false
		}

		// Receive another record
		r.record = r.doFetch()
		return r.record != nil
	}

	// Remove the record from list of unconsumed and return it
	r.unconsumed.Remove(e)
	r.record = e.Value.(*Record)
	return true
}

func (r *result) Record() *Record {
	// Unbox for better client experience
	if r.record == nil {
		return nil
	}
	return r.record
}

func (r *result) Err() error {
	return r.err
}

func (r *result) Summary() (ResultSummary, error) {
	r.fetchAll()
	// Unbox for better client experience
	if r.summary == nil || r.err != nil {
		return nil, r.err
	}
	return &resultSummary{
		sum:    r.summary,
		cypher: r.cypher,
		params: r.params,
	}, nil
}

// Used internally to fetch all records from stream and put them in unconsumed list.
func (r *result) fetchAll() {
	for !r.allReceived {
		rec := r.doFetch()
		if rec != nil {
			r.unconsumed.PushBack(rec)
		}
	}
}

func (r *result) Consume() (ResultSummary, error) {
	for !r.allReceived {
		r.doFetch()
	}
	if r.summary == nil || r.err != nil {
		return nil, r.err
	}
	return &resultSummary{
		sum:    r.summary,
		cypher: r.cypher,
		params: r.params,
	}, nil
}

// Used for backwards compatibility, error is not returned on session.Run but on iteration
// or consumption. This implementation of result interface fakes that behaviour.
type delayedErrorResult struct {
	err error
}

func (d *delayedErrorResult) Keys() ([]string, error) {
	return nil, d.err
}

func (d *delayedErrorResult) Next() bool {
	return false
}

func (d *delayedErrorResult) Err() error {
	return d.err
}

func (d *delayedErrorResult) Record() *Record {
	return nil
}

func (d *delayedErrorResult) Summary() (ResultSummary, error) {
	return nil, d.err
}

func (d *delayedErrorResult) Consume() (ResultSummary, error) {
	return nil, d.err
}
