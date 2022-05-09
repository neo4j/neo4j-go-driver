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

package retry

import (
	"errors"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type TStateInvocation struct {
	conn                          idb.Connection
	err                           error
	isCommitting                  bool
	now                           time.Time
	expectContinued               bool
	expectRouterInvalidated       bool
	expectRouterInvalidatedDb     string
	expectRouterInvalidatedServer string
	expectLastErrWasRetryable     bool
	expectLastErrType             error
}

func TestState(outer *testing.T) {
	var (
		baseTime     = time.Now()
		maxRetryTime = time.Second * 10
		overTime     = baseTime.Add(maxRetryTime).Add(1 * time.Second)
		halfTime     = baseTime.Add(maxRetryTime / 2)
		maxDead      = 2
		dbName       = "thedb"
		// a single server can be reused here since the router is a fake impl
		serverName     = "somehost:9999"
		authErr        = &db.Neo4jError{Code: "Neo.ClientError.Security.Unauthorized"}
		clusterErr     = &db.Neo4jError{Code: "Neo.ClientError.Cluster.NotALeader"}
		dbTransientErr = &db.Neo4jError{Code: "Neo.TransientError.Some.Some"}
	)

	testCases := map[string][]TStateInvocation{
		"Retry connect": {
			{conn: nil, err: &pool.PoolTimeout{}, expectContinued: true,
				expectLastErrWasRetryable: true, expectLastErrType: &pool.PoolTimeout{}},
		},
		"Retry connect timeout": {
			{conn: nil, err: errors.New("connect error 1"), expectContinued: true, now: baseTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: errors.New("connect error 2"), expectContinued: true, now: halfTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: errors.New("connect error 3"), expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Retry dead connection": {
			{conn: &testutil.ConnFake{Name: serverName, Alive: false},
				err: errors.New("some error"), expectContinued: true,
				expectLastErrWasRetryable: true, expectRouterInvalidated: true,
				expectRouterInvalidatedDb: dbName, expectRouterInvalidatedServer: serverName},
		},
		"Retry dead connection timeout": {
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: errors.New("some error 1"), expectContinued: true, now: baseTime,
				expectLastErrWasRetryable: true, expectRouterInvalidated: true, expectRouterInvalidatedDb: dbName, expectRouterInvalidatedServer: serverName},
			{conn: &testutil.ConnFake{Alive: false}, err: errors.New("some error 2"), expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Retry dead connection max": {
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: errors.New("some error 1"), expectContinued: true,
				expectLastErrWasRetryable: true, expectRouterInvalidated: true,
				expectRouterInvalidatedDb: dbName, expectRouterInvalidatedServer: serverName},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: errors.New("some error 2"), expectContinued: true,
				expectLastErrWasRetryable: true, expectRouterInvalidated: true,
				expectRouterInvalidatedDb: dbName, expectRouterInvalidatedServer: serverName},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: errors.New("some error 3"), expectContinued: false,
				expectLastErrWasRetryable: true, expectRouterInvalidated: true,
				expectRouterInvalidatedDb: dbName, expectRouterInvalidatedServer: serverName},
		},
		"Cluster error": {
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: true,
				expectRouterInvalidated: true, expectRouterInvalidatedDb: dbName, expectLastErrWasRetryable: true},
		},
		"Cluster error timeout": {
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: true,
				expectRouterInvalidated: true, expectRouterInvalidatedDb: dbName, expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Database transient error": {
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
		},
		"Database transient error timeout": {
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"User defined error": {
			{conn: &testutil.ConnFake{Alive: true}, err: errors.New("client error"), expectContinued: false,
				expectLastErrWasRetryable: false},
		},
		"Fail during commit": {
			{conn: &testutil.ConnFake{Alive: false}, err: io.EOF, isCommitting: true, expectContinued: false,
				expectLastErrWasRetryable: false, expectLastErrType: &CommitFailedDeadError{}},
		},
		"Fail during commit after retry": {
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Alive: false}, err: io.EOF, isCommitting: true, expectContinued: false,
				expectLastErrWasRetryable: false, expectLastErrType: &CommitFailedDeadError{}},
		},
		"Does not retry on auth errors": {
			{conn: nil, err: authErr, expectContinued: false,
				expectLastErrWasRetryable: false},
		},
	}

	for i, testCase := range testCases {
		outer.Run(i, func(t *testing.T) {
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
			for _, invocation := range testCase {
				// Update now if a value has been provided
				if !invocation.now.IsZero() {
					now = invocation.now
				}
				router := &testutil.RouterFake{}
				state.Router = router
				state.OnDeadConnection = func(server string) {
					router.InvalidateReader(dbName, server)
				}

				state.OnFailure(invocation.conn, invocation.err, invocation.isCommitting)
				continued := state.Continue()
				if continued != invocation.expectContinued {
					t.Errorf("Expected continue to return %v but returned %v", invocation.expectContinued, continued)
				}
				if invocation.expectRouterInvalidated != router.Invalidated ||
					invocation.expectRouterInvalidatedDb != router.InvalidatedDb ||
					invocation.expectRouterInvalidatedServer != router.InvalidatedServer {
					t.Errorf("Expected router invalidated: expected (%v/%s/%s) vs. actual (%v/%s/%s)",
						invocation.expectRouterInvalidated, invocation.expectRouterInvalidatedDb, invocation.expectRouterInvalidatedServer,
						router.Invalidated, router.InvalidatedDb, router.InvalidatedServer)
				}
				if state.LastErr == nil {
					t.Errorf("LastErr should be set")
				}
				if state.LastErrWasRetryable != invocation.expectLastErrWasRetryable {
					t.Errorf("LastErrWasRetryable mismatch")
				}
				if invocation.expectLastErrType != nil {
					t1 := reflect.TypeOf(state.LastErr)
					t2 := reflect.TypeOf(invocation.expectLastErrType)
					if t1 != t2 {
						t.Errorf("LastErr type mismatch: %s vs %s", t1, t2)
					}
				}
			}
		})
	}
}
