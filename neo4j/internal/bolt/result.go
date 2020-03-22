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

package bolt

import (
	"container/list"

	"github.com/neo4j/neo4j-go-driver/neo4j/api"
)

type fetch func() (*record, *summary, error)

type result struct {
	keys        []string
	err         error
	fetch       fetch
	allReceived bool
	unconsumed  list.List
	record      *record
	summary     *summary
}

func newResult(keys []string, fetch fetch) *result {
	return &result{
		keys:  keys,
		fetch: fetch,
	}
}

// Receive another record.
func (r *result) doFetch() *record {
	var rec *record
	var sum *summary
	rec, sum, r.err = r.fetch()
	r.allReceived = r.err != nil || rec == nil
	r.summary = sum
	return rec
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
	r.record = e.Value.(*record)
	return true
}

func (r *result) Record() api.Record {
	// Unbox for better client experience
	if r.record == nil {
		return nil
	}
	return r.record
}

func (r *result) Err() error {
	return r.err
}

func (r *result) Summary() (api.ResultSummary, error) {
	r.FetchAll()
	// Unbox for better client experience
	if r.summary == nil {
		return nil, r.err
	}
	return r.summary, r.err
}

// Used internally to fetch all records from stream and put them in unconsumed list.
func (r *result) FetchAll() {
	for !r.allReceived {
		rec := r.doFetch()
		if rec != nil {
			r.unconsumed.PushBack(rec)
		}
	}
}

// Used internally to drain all records from stream.
func (r *result) ConsumeAll() {
	for !r.allReceived {
		r.doFetch()
	}
}
