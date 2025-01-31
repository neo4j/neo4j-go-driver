//go:build go1.23

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
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func TestResultGo1_23(outer *testing.T) {
	outer.Parallel()

	ctx := context.Background()
	streamHandle := idb.StreamHandle(0)
	cypher := ""
	params := map[string]any{}
	recs := []*db.Record{
		{Keys: []string{"n"}, Values: []any{42}},
		{Keys: []string{"n"}, Values: []any{43}},
		{Keys: []string{"n"}, Values: []any{44}},
	}
	sums := []*db.Summary{{}}
	errs := []error{
		errors.New("whatever"),
	}

	outer.Run("range iter success", func(t *testing.T) {
		hookCalled := false
		afterConsumptionHook := func() {
			hookCalled = true
		}

		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Record: recs[1]}, {Record: recs[2]}, {Summary: sums[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, &transactionState{}, afterConsumptionHook)
		i := 0
		for record, err := range res.Records(ctx) {
			AssertBoolEqual(t, hookCalled, false)
			AssertNoError(t, err)
			AssertDeepEquals(t, record, recs[i])
			i += 1
		}
		AssertIntEqual(t, i, 3)
		AssertNil(t, res.Err())
		AssertBoolEqual(t, hookCalled, true)
		summary, err := res.Consume(ctx)
		AssertNoError(t, err)
		AssertNotNil(t, summary)
	})

	outer.Run("range iter error", func(t *testing.T) {
		hookCalled := false
		afterConsumptionHook := func() {
			hookCalled = true
		}

		conn := &ConnFake{
			Nexts: []Next{{Record: recs[0]}, {Record: recs[1]}, {Err: errs[0]}},
		}
		res := newResultWithContext(conn, streamHandle, cypher, params, &transactionState{}, afterConsumptionHook)
		i := 0
		for record, err := range res.Records(ctx) {
			if i < 2 {
				AssertNoError(t, err)
				AssertDeepEquals(t, record, recs[i])
			} else {
				AssertError(t, err)
				AssertNil(t, record)
			}
			i += 1
		}
		AssertIntEqual(t, i, 3)
		AssertError(t, res.Err())
		summary, err := res.Consume(ctx)
		AssertError(t, err)
		AssertNil(t, summary)
		AssertBoolEqual(t, hookCalled, false)
	})

	outer.Run("range iter break", func(inner *testing.T) {
		inner.Parallel()

		type iterBreakTestCase struct {
			description string
			reuseIter   bool
		}

		iterBreakTestCases := []iterBreakTestCase{
			{"reusing iter", true},
			{"new iter", false},
		}

		for _, testCase := range iterBreakTestCases {
			inner.Run(testCase.description, func(t *testing.T) {
				hookCalled := false
				afterConsumptionHook := func() {
					hookCalled = true
				}

				conn := &ConnFake{
					Nexts: []Next{{Record: recs[0]}, {Record: recs[1]}, {Record: recs[2]}, {Summary: sums[0]}},
				}
				res := newResultWithContext(conn, streamHandle, cypher, params, &transactionState{}, afterConsumptionHook)
				i := 0
				recordsIter := res.Records(ctx)
				for record, err := range recordsIter {
					AssertBoolEqual(t, hookCalled, false)
					AssertNoError(t, err)
					AssertDeepEquals(t, record, recs[i])
					i += 1
					if i == 2 {
						break
					}
				}
				AssertNil(t, res.Err())
				AssertBoolEqual(t, hookCalled, false)

				if !testCase.reuseIter {
					recordsIter = res.Records(ctx)
				}
				for record, err := range recordsIter {
					AssertBoolEqual(t, hookCalled, false)
					AssertNoError(t, err)
					AssertDeepEquals(t, record, recs[i])
					i += 1
				}

				AssertIntEqual(t, i, 3)
				AssertNil(t, res.Err())
				AssertBoolEqual(t, hookCalled, true)
				summary, err := res.Consume(ctx)
				AssertNoError(t, err)
				AssertNotNil(t, summary)
			})
		}
	})

	outer.Run("range iter on closed", func(inner *testing.T) {
		inner.Parallel()

		type iterBreakTestCase struct {
			description      string
			closer           func(ResultWithContext) error
			usePreIter       bool
			pullPreIterFirst bool
		}

		iterBreakTestCases := []iterBreakTestCase{
			{
				description: "by single",
				closer: func(res ResultWithContext) error {
					_, err := res.Single(ctx)
					return err
				},
			},
			{
				description: "by consume",
				closer: func(res ResultWithContext) error {
					_, err := res.Consume(ctx)
					return err
				},
			},
			{
				description: "by collect",
				closer: func(res ResultWithContext) error {
					_, err := res.Collect(ctx)
					return err
				},
			},
			{
				description: "by iter",
				closer: func(res ResultWithContext) error {
					for _, err := range res.Records(ctx) {
						if err != nil {
							return err
						}
					}
					return nil
				},
			},
		}

		extraBreakTestCases := make([]iterBreakTestCase, 0, len(iterBreakTestCases)*2)
		for _, testCase := range iterBreakTestCases {
			extraBreakTestCase := testCase
			extraBreakTestCase.usePreIter = true
			extraBreakTestCase.description = fmt.Sprintf("%s with pre iter", testCase.description)
			extraBreakTestCases = append(extraBreakTestCases, extraBreakTestCase)
			extraBreakTestCase.pullPreIterFirst = true
			extraBreakTestCase.description = fmt.Sprintf("%s with pre iter failing first", testCase.description)
			extraBreakTestCases = append(extraBreakTestCases, extraBreakTestCase)
		}
		iterBreakTestCases = append(iterBreakTestCases, extraBreakTestCases...)

		for _, testCase := range iterBreakTestCases {
			inner.Run(testCase.description, func(t *testing.T) {
				nexts := []Next{}
				if testCase.usePreIter {
					nexts = append(nexts, Next{Record: recs[0]})
				}
				nexts = append(nexts, Next{Record: recs[1]}, Next{Summary: sums[0]})
				conn := &ConnFake{Nexts: nexts, ConsumeSum: sums[0]}
				res := newResultWithContext(conn, streamHandle, cypher, params, &transactionState{}, nil)

				iter1 := res.Records(ctx)
				if testCase.usePreIter {
					for _, err := range iter1 {
						AssertNoError(t, err)
						break
					}
				}

				AssertNoError(t, testCase.closer(res))

				iter2 := res.Records(ctx)

				if testCase.pullPreIterFirst {
					i := 0
					for _, err := range iter1 {
						AssertError(t, err)
						i += 1
					}
					AssertIntEqual(t, i, 1)
				}

				i := 0
				for _, err := range iter2 {
					AssertError(t, err)
					i += 1
				}
				AssertIntEqual(t, i, 1)

				if !testCase.pullPreIterFirst {
					i = 0
					for _, err := range iter1 {
						AssertError(t, err)
						i += 1
					}
					AssertIntEqual(t, i, 1)
				}
			})
		}
	})
}
