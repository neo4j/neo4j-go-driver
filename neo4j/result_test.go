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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"testing"
)

type iter struct {
	expectNext   bool
	expectRec    *db.Record
	expectSum    *db.Summary
	expectSumErr error
	expectErr    error
	consume      bool
	panicOnFetch bool
}

type fetchRet struct {
	rec *db.Record
	sum *db.Summary
	err error
}

type testFetcher struct {
	rets         []fetchRet
	panicOnFetch bool
}

func (f *testFetcher) Next(s db.Handle) (*db.Record, *db.Summary, error) {
	if len(f.rets) == 0 || f.panicOnFetch {
		// If signalling is made correctly in test case this shouldn't happen and if it does
		// it is an error in test setup.
		panic("boom")
	}
	ret := f.rets[0]
	f.rets = f.rets[1:]
	return ret.rec, ret.sum, ret.err
}

func TestResult(ot *testing.T) {
	stream := &db.Stream{
		Keys: []string{"key1", "key2"},
	}
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
		fetcher := &testFetcher{}
		res := newResult(fetcher, stream, cypher, params)
		rec := res.Record
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

	// Iterate without any unconsumed (no push from connection)
	iterCases := []struct {
		name   string
		stream []fetchRet
		iters  []iter
		sum    db.Summary
	}{
		{
			name: "happy",
			stream: []fetchRet{
				fetchRet{rec: recs[0]},
				fetchRet{rec: recs[1]},
				fetchRet{sum: sums[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: true, expectRec: recs[1]},
				iter{expectNext: false},
				iter{expectNext: false, consume: true, expectSum: sums[0]},
			},
		},
		{
			name: "error after one record",
			stream: []fetchRet{
				fetchRet{rec: recs[0]},
				fetchRet{err: errs[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: false, expectErr: errs[0]},
				iter{expectNext: false, expectErr: errs[0], consume: true, expectSumErr: errs[0]},
			},
		},
		{
			name: "proceed after error",
			stream: []fetchRet{
				fetchRet{rec: recs[0]},
				fetchRet{err: errs[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: false, expectErr: errs[0]},
				iter{expectNext: false, expectErr: errs[0]},
			},
		},
		{
			name: "consume all",
			stream: []fetchRet{
				fetchRet{rec: recs[0]},
				fetchRet{rec: recs[1]},
				fetchRet{sum: sums[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{consume: true, expectNext: false},
				iter{panicOnFetch: true, expectNext: false},
			},
		},
	}
	for _, c := range iterCases {
		ot.Run(fmt.Sprintf("Iteration-%s", c.name), func(t *testing.T) {
			fetcher := &testFetcher{rets: c.stream}
			res := newResult(fetcher, stream, cypher, params)
			for i, call := range c.iters {
				fetcher.panicOnFetch = call.panicOnFetch
				if call.consume {
					res.Consume()
					if len(fetcher.rets) > 0 {
						t.Fatalf("Consume should have emptied stream")
					}
				}
				gotNext := res.Next()
				if gotNext != call.expectNext {
					t.Fatalf("Next at iter %d returned %t but expected to return %t", i, gotNext, call.expectNext)
				}
				gotRec := res.Record
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
}
