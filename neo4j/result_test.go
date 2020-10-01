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
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
)

type iter struct {
	expectNext   bool
	expectRec    *db.Record
	expectSum    *db.Summary
	expectSumErr error
	expectErr    error
}

func TestResult(ot *testing.T) {
	streamHandle := db.StreamHandle(0)
	cypher := ""
	params := map[string]interface{}{}
	recs := []*db.Record{
		&db.Record{},
		&db.Record{},
		&db.Record{},
	}
	sums := []*db.Summary{
		&db.Summary{},
	}
	errs := []error{
		errors.New("Whatever"),
	}

	// Initialization
	ot.Run("Initialization", func(t *testing.T) {
		conn := &ConnFake{}
		res := newResult(conn, streamHandle, cypher, params)
		rec := res.Record()
		if rec != nil {
			t.Errorf("Should be no record")
		}
		err := res.Err()
		if err != nil {
			t.Errorf("Should be no error")
		}
		if err != nil {
			t.Errorf("Shouldn't be an error to call summary too early")
		}
	})

	// Next
	// TODO: Reduce testing of this, simpler after moving logic to bolt
	iterCases := []struct {
		name   string
		stream []Next
		iters  []iter
		sum    db.Summary
	}{
		{
			name: "happy",
			stream: []Next{
				Next{Record: recs[0]},
				Next{Record: recs[1]},
				Next{Summary: sums[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: true, expectRec: recs[1]},
				iter{expectNext: false, expectSum: sums[0]},
			},
		},
		{
			name: "error after one record",
			stream: []Next{
				Next{Record: recs[0]},
				Next{Err: errs[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: false, expectErr: errs[0]},
			},
		},
		{
			name: "proceed after error",
			stream: []Next{
				Next{Record: recs[0]},
				Next{Err: errs[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: false, expectErr: errs[0]},
				iter{expectNext: false, expectErr: errs[0]},
			},
		},
	}
	for _, c := range iterCases {
		ot.Run(fmt.Sprintf("Next %s", c.name), func(t *testing.T) {
			conn := &ConnFake{Nexts: c.stream}
			res := newResult(conn, streamHandle, cypher, params)
			for i, call := range c.iters {
				gotNext := res.Next()
				if gotNext != call.expectNext {
					t.Fatalf("Next at iter %d returned %t but expected to return %t", i, gotNext, call.expectNext)
				}
				gotRec := res.Record()
				if (gotRec == nil) != (call.expectRec == nil) {
					if gotRec == nil {
						t.Fatalf("Expected to get record but didn't at iter %d", i)
					} else {
						t.Fatalf("Expected to NOT get a record but did at iter %d", i)
					}
				}
				gotErr := res.Err()
				if (gotErr == nil) != (call.expectErr == nil) {
					if gotErr == nil {
						t.Fatalf("Expected to get an error but didn't at iter %d", i)
					} else {
						t.Fatalf("Expected to NOT get an error but did at iter %d", i)
					}
				}
			}
		})
	}

	// Consume
	ot.Run("Consume with summary", func(t *testing.T) {
		conn := &ConnFake{
			ConsumeSum: sums[0],
			ConsumeErr: nil,
			Nexts:      []Next{Next{Record: recs[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		// Get one record to make sure that Record() is cleared
		res.Next()
		AssertNotNil(t, res.Record())
		sum, err := res.Consume()
		AssertNotNil(t, sum)
		AssertNil(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})
	ot.Run("Consume with error", func(t *testing.T) {
		conn := &ConnFake{
			ConsumeSum: nil,
			ConsumeErr: errs[0],
			Nexts:      []Next{Next{Record: recs[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		// Get one record to make sure that Record() is cleared
		res.Next()
		AssertNotNil(t, res.Record())
		sum, err := res.Consume()
		AssertNil(t, sum)
		AssertNotNil(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})

	// Single
	ot.Run("Single with one record", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Record: recs[0]}, Next{Summary: sums[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		rec, err := res.Single()
		AssertNotNil(t, rec)
		AssertNoError(t, err)
		// The getters should be updated
		AssertNotNil(t, res.Record())
		AssertNil(t, res.Err())
	})
	ot.Run("Single with no record", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Summary: sums[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		rec, err := res.Single()
		AssertNil(t, rec)
		assertUsageError(t, err)
		AssertError(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		assertUsageError(t, res.Err())
	})
	ot.Run("Single with two records", func(t *testing.T) {
		calledConsume := false
		conn := &ConnFake{
			Nexts: []Next{Next{Record: recs[0]}, Next{Record: recs[1]}, Next{Summary: sums[0]}},
			ConsumeHook: func() {
				calledConsume = true
			},
			ConsumeSum: sums[0],
		}
		res := newResult(conn, streamHandle, cypher, params)
		rec, err := res.Single()
		AssertNil(t, rec)
		assertUsageError(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		assertUsageError(t, res.Err())
		// It should have called Consume on the connection to get rid of all the records,
		// the result isn't useful after this.
		AssertTrue(t, calledConsume)
		// Calling Consume should preserve the usage error
		sum, err := res.Consume()
		AssertNil(t, sum)
		assertUsageError(t, err)
		assertUsageError(t, res.Err())
	})
	ot.Run("Single with error", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Err: errs[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		rec, err := res.Single()
		AssertNil(t, rec)
		AssertError(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})

	// Collect
	ot.Run("Collect n records", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Record: recs[0]}, Next{Record: recs[1]}, Next{Summary: sums[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		coll, err := res.Collect()
		AssertNoError(t, err)
		AssertLen(t, coll, 2)
		if recs[0] != coll[0] || recs[1] != coll[1] {
			t.Error("Collected records do not match")
		}
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})
	ot.Run("Collect n records after Next", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Record: recs[0]}, Next{Record: recs[1]}, Next{Record: recs[2]}, Next{Summary: sums[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		res.Next()
		AssertNotNil(t, res.Record())
		coll, err := res.Collect()
		AssertNoError(t, err)
		AssertLen(t, coll, 2)
		if recs[1] != coll[0] || recs[2] != coll[1] {
			t.Error("Collected records do not match")
		}
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})
	ot.Run("Collect empty", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Summary: sums[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		coll, err := res.Collect()
		AssertNoError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})
	ot.Run("Collect emptied", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Summary: sums[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		res.Next()
		AssertNil(t, res.Record())
		coll, err := res.Collect()
		AssertNoError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})
	ot.Run("Collect error", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Err: errs[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		coll, err := res.Collect()
		AssertError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})
	ot.Run("Collect stream error", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{Next{Record: recs[0]}, Next{Err: errs[0]}},
		}
		res := newResult(conn, streamHandle, cypher, params)
		coll, err := res.Collect()
		AssertError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})
}
