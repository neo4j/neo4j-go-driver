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

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type Result struct {
	// Keys available on the result set.
	Keys []string
	// Current record. Initially nil, set by a succesfull call to Next()
	Record *db.Record

	err         error
	iter        iterator
	stream      *db.Stream
	cypher      string
	params      map[string]interface{}
	allReceived bool
	unconsumed  list.List
	summary     *db.Summary
}

type iterator interface {
	Next(s db.Handle) (*Record, *db.Summary, error)
}

func newResult(iter iterator, stream *db.Stream, cypher string, params map[string]interface{}) *Result {
	return &Result{
		Keys:   stream.Keys,
		iter:   iter,
		stream: stream,
		cypher: cypher,
		params: params,
	}
}

// Receive another record.
func (r *Result) doFetch() *Record {
	var rec *Record
	var sum *db.Summary
	rec, sum, r.err = r.iter.Next(r.stream.Handle)
	r.allReceived = r.err != nil || rec == nil
	r.summary = sum
	return rec
}

// Next returns true only if there is a record to be processed.
func (r *Result) Next() bool {
	e := r.unconsumed.Front()
	if e == nil {
		// All has been received and consumed
		if r.allReceived {
			r.Record = nil
			return false
		}

		// Receive another record
		r.Record = r.doFetch()
		return r.Record != nil
	}

	// Remove the record from list of unconsumed and return it
	r.unconsumed.Remove(e)
	r.Record = e.Value.(*Record)
	return true
}

// Err returns the latest error that caused Next to return false or Consume to fail.
func (r *Result) Err() error {
	return r.err
}

// Used internally to fetch all records from stream and put them in unconsumed list.
func (r *Result) fetchAll() {
	for !r.allReceived {
		rec := r.doFetch()
		if rec != nil {
			r.unconsumed.PushBack(rec)
		}
	}
}

// Consume discards all remaining records and returns the summary information
// about the statement execution.
// Should be called when records of resultset if of no interest to indicate
// to the driver that records don't need to be buffered in memory.
func (r *Result) Consume() (ResultSummary, error) {
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

// Collect loops through the result, collects records into a slice and returns the
// resulting slice.
func (r *Result) Collect() ([]*Record, error) {
	list := make([]*Record, 0, 100)
	for r.Next() {
		list = append(list, r.Record)
	}
	if r.err != nil {
		return nil, r.err
	}
	return list, nil
}

// Single returns one and only one record from the result stream.
// If the result contains zero or more than one record error is returned.
func (r *Result) Single() (*Record, error) {
	r.Next()
	if r.err != nil {
		return nil, r.err
	}
	record := r.Record
	if record == nil {
		return nil, newDriverError("result contains no records")
	}
	if r.Next() {
		return nil, newDriverError("result contains more than one record")
	}
	return record, nil
}
