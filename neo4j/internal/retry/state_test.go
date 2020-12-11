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

package retry

import (
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

type TStateInvocation struct {
	conn                      db.Connection
	err                       error
	isCommitting              bool
	now                       time.Time
	expectContinued           bool
	expectRouterInvalidated   bool
	expectRouterInvalidatedDb string
	expectLastErrWasRetryable bool
	expectLastErrType         error
}

func TestState(tt *testing.T) {
	var (
		baseTime       = time.Now()
		maxRetryTime   = time.Second * 10
		overTime       = baseTime.Add(maxRetryTime).Add(1 * time.Second)
		halfTime       = baseTime.Add(maxRetryTime / 2)
		maxDead        = 2
		dbName         = "thedb"
		clusterErr     = &db.Neo4jError{Code: "Neo.ClientError.Cluster.NotALeader"}
		dbTransientErr = &db.Neo4jError{Code: "Neo.TransientError.Some.Some"}
	)

	cases := map[string][]TStateInvocation{
		"Retry connect": []TStateInvocation{
			{conn: nil, err: &pool.PoolTimeout{}, expectContinued: true,
				expectLastErrWasRetryable: true, expectLastErrType: &pool.PoolTimeout{}},
		},
		"Retry connect timeout": []TStateInvocation{
			{conn: nil, err: errors.New("connect error 1"), expectContinued: true, now: baseTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: errors.New("connect error 2"), expectContinued: true, now: halfTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: errors.New("connect error 3"), expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Retry dead connection": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error"), expectContinued: true,
				expectLastErrWasRetryable: true},
		},
		"Retry dead connection timeout": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error 1"), expectContinued: true, now: baseTime,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error 2"), expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Retry dead connection max": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error 1"), expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error 2"), expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error 3"), expectContinued: false,
				expectLastErrWasRetryable: true},
		},
		"Cluster error": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: true,
				expectRouterInvalidated: true, expectRouterInvalidatedDb: dbName, expectLastErrWasRetryable: true},
		},
		"Cluster error timeout": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: true,
				expectRouterInvalidated: true, expectRouterInvalidatedDb: dbName, expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Database transient error": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
		},
		"Database transient error timeout": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"User defined error": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: true}, err: errors.New("client error"), expectContinued: false,
				expectLastErrWasRetryable: false},
		},
		"Fail during commit": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: false}, err: io.EOF, isCommitting: true, expectContinued: false,
				expectLastErrWasRetryable: false, expectLastErrType: &CommitFailedDeadError{}},
		},
		"Fail during commit after retry": []TStateInvocation{
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: false}, err: io.EOF, isCommitting: true, expectContinued: false,
				expectLastErrWasRetryable: false, expectLastErrType: &CommitFailedDeadError{}},
		},
	}

	for n, c := range cases {
		tt.Run(n, func(t *testing.T) {
			now := baseTime
			state := State{
				Now:                     func() time.Time { return now },
				Log:                     &log.Console{Errors: true, Debugs: true},
				LogName:                 "TEST",
				LogId:                   "State",
				Sleep:                   func(time.Duration) {},
				MaxTransactionRetryTime: maxRetryTime,
				MaxDeadConnections:      maxDead,
				DatabaseName:            dbName,
			}
			for _, i := range c {
				// Update now if a value has been provided
				if !i.now.IsZero() {
					now = i.now
				}
				router := &testutil.RouterFake{}
				state.Router = router

				state.OnFailure(i.conn, i.err, i.isCommitting)
				continued := state.Continue()
				if continued != i.expectContinued {
					t.Errorf("Expected continue to return %v but returned %v", i.expectContinued, continued)
				}
				if i.expectRouterInvalidated != router.Invalidated ||
					i.expectRouterInvalidatedDb != router.InvalidatedDb {
					t.Errorf("Expected router invalidated: (%v/%s) vs (%v/%s)",
						i.expectRouterInvalidated, i.expectRouterInvalidatedDb,
						router.Invalidated, router.InvalidatedDb)
				}
				if state.LastErr == nil {
					t.Errorf("LastErr should be set")
				}
				if state.LastErrWasRetryable != i.expectLastErrWasRetryable {
					t.Errorf("LastErrWasRetryable mismatch")
				}
				if i.expectLastErrType != nil {
					t1 := reflect.TypeOf(state.LastErr)
					t2 := reflect.TypeOf(i.expectLastErrType)
					if t1 != t2 {
						t.Errorf("LastErr type mismatch: %s vs %s", t1, t2)
					}
				}
			}
		})
	}
}
