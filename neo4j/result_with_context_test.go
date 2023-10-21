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

import (
	"context"
	"errors"
	"fmt"
	idb "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/db"
	"testing"

	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/db"
	. "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
)

type iter struct {
	expectNext bool
	expectRec  *db.Record
	expectSum  *db.Summary
	expectErr  error
}

func TestResult(outer *testing.T) {
	ctx := context.Background()
	streamHandle := idb.StreamHandle(0)
	cypher := ""
	params := map[string]any{}
	recs := []*db.Record{
		{Keys: []string{"n"}, Values: []any{42}},
		{Keys: []string{"n"}, Values: []any{43}},
		{Keys: []string{"n"}, Values: []any{44}},
	}
	record1 := recs[0]
	record2 := recs[1]
	sums := []*db.Summary{{}}
	errs := []error{
		errors.New("whatever"),
	}

	// Initialization
	outer.Run("Initialization", func(t *testing.T) {
		conn := &ConnFake{}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
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
		rounds []iter
		sum    db.Summary
	}{
		{
			name: "happy",
			stream: []Next{
				{Record: recs[0]},
				{Record: recs[1]},
				{Summary: sums[0]},
			},
			rounds: []iter{
				{expectNext: true, expectRec: recs[0]},
				{expectNext: true, expectRec: recs[1]},
				{expectNext: false, expectSum: sums[0]},
			},
		},
		{
			name: "error after one record",
			stream: []Next{
				{Record: recs[0]},
				{Err: errs[0]},
			},
			rounds: []iter{
				{expectNext: true, expectRec: recs[0]},
				{expectNext: false, expectErr: errs[0]},
			},
		},
		{
			name: "proceed after error",
			stream: []Next{
				{Record: recs[0]},
				{Err: errs[0]},
			},
			rounds: []iter{
				{expectNext: true, expectRec: recs[0]},
				{expectNext: false, expectErr: errs[0]},
				{expectNext: false, expectErr: errs[0]},
			},
		},
	}
	for _, c := range iterCases {
		outer.Run(fmt.Sprintf("Next %s", c.name), func(t *testing.T) {
			conn := &ConnFake{Nexts: c.stream}
			res := newResultWithContext(conn, streamHandle, cypher, params, nil)
			for i, call := range c.rounds {
				gotNext := res.Next(context.Background())
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

	// PeekRecord
	outer.Run("Peeks records and allocates", func(t *testing.T) {
		var peekedFirst *Record
		var peekedSecond *Record
		var nextFirst *Record
		var peekedAfterNextFirst *Record
		var nextSecond *Record
		conn := &ConnFake{Nexts: []Next{{Record: recs[0]}}}

		result := newResultWithContext(conn, streamHandle, cypher, params, nil)

		AssertTrue(t, result.PeekRecord(ctx, &peekedFirst))
		AssertTrue(t, result.PeekRecord(ctx, &peekedSecond))
		AssertTrue(t, result.NextRecord(ctx, &nextFirst))
		AssertDeepEquals(t, recs[0], peekedFirst, peekedSecond, nextFirst)
		AssertFalse(t, result.PeekRecord(ctx, &peekedAfterNextFirst))
		AssertNil(t, peekedAfterNextFirst)
		AssertFalse(t, result.NextRecord(ctx, &nextSecond))
		AssertNil(t, nextSecond)
	})

	// Peek
	outer.Run("Peeks records", func(inner *testing.T) {

		inner.Run("peeks single record", func(t *testing.T) {
			conn := &ConnFake{Nexts: []Next{{Record: record1}}}

			result := newResultWithContext(conn, streamHandle, cypher, params, nil)

			AssertTrue(t, result.Peek(ctx))
			AssertDeepEquals(t, record1, result.Record())
			AssertTrue(t, result.Next(ctx))
			AssertDeepEquals(t, record1, result.Record())
			AssertFalse(t, result.Peek(ctx))
			AssertDeepEquals(t, record1, result.Record())
			AssertFalse(t, result.Next(ctx))
			AssertNil(t, result.Record())
		})

		inner.Run("peeks once and fetches subsequent records", func(t *testing.T) {
			conn := &ConnFake{Nexts: []Next{{Record: record1}, {Record: record2}}}

			result := newResultWithContext(conn, streamHandle, cypher, params, nil)

			AssertTrue(t, result.Peek(ctx))
			AssertDeepEquals(t, record1, result.Record())
			AssertTrue(t, result.Next(ctx))
			AssertDeepEquals(t, record1, result.Record())
			AssertTrue(t, result.Next(ctx))
			AssertDeepEquals(t, record2, result.Record())
			AssertFalse(t, result.Peek(ctx))
			AssertFalse(t, result.Next(ctx))
		})

	})

	// Consume
	outer.Run("Consume with summary", func(t *testing.T) {
		conn := &ConnFake{
			ConsumeSum: sums[0],
			ConsumeErr: nil,
			Nexts:      []Next{{Record: recs[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		// Get one record to make sure that Record() is cleared
		res.Next(ctx)
		AssertNotNil(t, res.Record())
		sum, err := res.Consume(ctx)
		AssertNotNil(t, sum)
		AssertNil(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})

	outer.Run("Consume with error", func(t *testing.T) {
		conn := &ConnFake{
			ConsumeSum: nil,
			ConsumeErr: errs[0],
			Nexts:      []Next{{Record: recs[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		// Get one record to make sure that Record() is cleared
		res.Next(ctx)
		AssertNotNil(t, res.Record())
		sum, err := res.Consume(ctx)
		AssertNil(t, sum)
		AssertNotNil(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})

	// Single
	outer.Run("Single with one record", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		rec, err := res.Single(ctx)
		AssertNotNil(t, rec)
		AssertNoError(t, err)
		// The getters should be updated
		AssertNotNil(t, res.Record())
		AssertNil(t, res.Err())
	})

	outer.Run("Single with no record", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		rec, err := res.Single(ctx)
		AssertNil(t, rec)
		assertUsageError(t, err)
		AssertError(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		assertUsageError(t, res.Err())
	})

	outer.Run("Single with two records", func(t *testing.T) {
		calledConsume := false
		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Record: recs[1]}, {Summary: sums[0]}},
			ConsumeHook: func() {
				calledConsume = true
			},
			ConsumeSum: sums[0],
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		rec, err := res.Single(ctx)
		AssertNil(t, rec)
		assertUsageError(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		assertUsageError(t, res.Err())
		// It should have called Consume on the connection to get rid of all the records,
		// the result isn't useful after this.
		AssertTrue(t, calledConsume)
		// Calling Consume should preserve the usage error
		sum, err := res.Consume(ctx)
		AssertNil(t, sum)
		assertUsageError(t, err)
		assertUsageError(t, res.Err())
	})

	outer.Run("Single with error", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Err: errs[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		rec, err := res.Single(ctx)
		AssertNil(t, rec)
		AssertError(t, err)
		// The getters should be updated
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})

	// Collect
	outer.Run("Collect n records", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Record: recs[1]}, {Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		coll, err := res.Collect(ctx)
		AssertNoError(t, err)
		AssertLen(t, coll, 2)
		if recs[0] != coll[0] || recs[1] != coll[1] {
			t.Error("Collected records do not match")
		}
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})

	outer.Run("Collect n records after Next", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Record: recs[1]}, {Record: recs[2]}, {Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		res.Next(ctx)
		AssertNotNil(t, res.Record())
		coll, err := res.Collect(ctx)
		AssertNoError(t, err)
		AssertLen(t, coll, 2)
		if recs[1] != coll[0] || recs[2] != coll[1] {
			t.Error("Collected records do not match")
		}
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})

	outer.Run("Collect empty", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		coll, err := res.Collect(ctx)
		AssertNoError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})

	outer.Run("Collect emptied", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		res.Next(ctx)
		AssertNil(t, res.Record())
		coll, err := res.Collect(ctx)
		AssertNoError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNil(t, res.Err())
	})

	outer.Run("Collect error", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Err: errs[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		coll, err := res.Collect(ctx)
		AssertError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})

	outer.Run("Collect stream error", func(t *testing.T) {
		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Err: errs[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, nil)
		coll, err := res.Collect(ctx)
		AssertError(t, err)
		AssertLen(t, coll, 0)
		AssertNil(t, res.Record())
		AssertNotNil(t, res.Err())
	})

	outer.Run("IsOpen", func(t *testing.T) {
		openResult := &resultWithContext{summary: nil}
		closedResult := &resultWithContext{summary: &db.Summary{}}

		AssertTrue(t, openResult.IsOpen())
		AssertFalse(t, closedResult.IsOpen())
	})

	outer.Run("Consuming closed result fails", func(inner *testing.T) {
		testCases := []struct {
			scenario string
			callback func(*testing.T, *resultWithContext) error
		}{
			{
				scenario: "with Next and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					AssertFalse(t, result.Next(ctx))
					return result.Err()
				},
			},
			{
				scenario: "with several Next calls and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					AssertFalse(t, result.Next(ctx))
					AssertFalse(t, result.Next(ctx))
					return result.Err()
				},
			},
			{
				scenario: "with Peek and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					AssertFalse(t, result.Peek(ctx))
					return result.Err()
				},
			},
			{
				scenario: "with several Peek calls and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					AssertFalse(t, result.Peek(ctx))
					AssertFalse(t, result.Peek(ctx))
					return result.Err()
				},
			},
			{
				scenario: "with NextRecord and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					var record *Record
					AssertFalse(t, result.NextRecord(ctx, &record))
					return result.Err()
				},
			},
			{
				scenario: "with several NextRecord calls and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					var record *Record
					AssertFalse(t, result.NextRecord(ctx, &record))
					AssertFalse(t, result.NextRecord(ctx, &record))
					return result.Err()
				},
			},
			{
				scenario: "with PeekRecord and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					var record *Record
					AssertFalse(t, result.PeekRecord(ctx, &record))
					return result.Err()
				},
			},
			{
				scenario: "with several PeekRecord calls and Err",
				callback: func(t *testing.T, result *resultWithContext) error {
					var record *Record
					AssertFalse(t, result.PeekRecord(ctx, &record))
					AssertFalse(t, result.PeekRecord(ctx, &record))
					return result.Err()
				},
			},
		}

		for _, testCase := range testCases {
			inner.Run(testCase.scenario, func(t *testing.T) {
				result := &resultWithContext{summary: &db.Summary{}}

				err := testCase.callback(t, result)

				assertUsageError(t, err)
				AssertStringEqual(t, err.Error(), consumedResultError)
				AssertNil(t, result.Record())
			})
		}
	})

	outer.Run("Calling the consumption hook", func(inner *testing.T) {
		inner.Parallel()

		type consumptionTestCases struct {
			description string
			callback    func(*resultWithContext) error
		}

		testCases := []consumptionTestCases{
			{"after Single", func(r *resultWithContext) error {
				_, err := r.Single(ctx)
				return err
			}},
			{"only once after more than Single call", func(r *resultWithContext) error {
				_, _ = r.Single(ctx)
				_, _ = r.Single(ctx) // ignore "result already consumed" error
				return nil
			}},
			{"after Consume", func(r *resultWithContext) error {
				_, err := r.Consume(ctx)
				return err
			}},
			{"only once after more than Consume call", func(r *resultWithContext) error {
				_, _ = r.Consume(ctx)
				_, err := r.Consume(ctx)
				return err
			}},
			{"after Collect", func(r *resultWithContext) error {
				_, err := r.Collect(ctx)
				return err
			}},
			{"only once after more than Collect call", func(r *resultWithContext) error {
				_, _ = r.Collect(ctx)
				_, err := r.Collect(ctx)
				return err
			}},
			{"after buffering", func(r *resultWithContext) error {
				r.buffer(ctx)
				return nil
			}},
			{"only once after more than one buffering call", func(r *resultWithContext) error {
				r.buffer(ctx)
				r.buffer(ctx)
				return nil
			}},
		}

		for _, testCase := range testCases {
			inner.Run(testCase.description, func(t *testing.T) {
				count := 0
				result := &resultWithContext{
					conn: &ConnFake{
						Nexts: []Next{{Record: record1}, {Summary: sums[0]}},
					},
					afterConsumptionHook: func() {
						count++
					}}

				err := testCase.callback(result)

				AssertNil(t, err)
				AssertIntEqual(t, count, 1)
			})
		}
	})
}
