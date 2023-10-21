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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package retry

import (
	"context"
	"errors"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type TStateInvocation struct {
	conn                      idb.Connection
	err                       error
	isCommitting              bool
	now                       time.Time
	expectContinued           bool
	expectLastErrWasRetryable bool
	expectLastErrType         error
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
			{conn: nil, err: &errorutil.PoolTimeout{}, expectContinued: true,
				expectLastErrWasRetryable: true, expectLastErrType: &errorutil.PoolTimeout{}},
		},
		"Retry connect timeout": {
			{conn: nil, err: dbTransientErr, expectContinued: true, now: baseTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: dbTransientErr, expectContinued: true, now: halfTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: dbTransientErr, expectContinued: false, now: overTime,
				expectLastErrWasRetryable: true},
		},
		"Retry dead connection": {
			{conn: &testutil.ConnFake{Name: serverName, Alive: false},
				err: errors.New("some error"), expectContinued: false,
				expectLastErrWasRetryable: false},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false},
				err:             dbTransientErr,
				expectContinued: true, expectLastErrWasRetryable: true},
		},
		"Retry dead connection timeout": {
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: dbTransientErr,
				expectContinued: true, now: baseTime, expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: errors.New("some error 2"),
				expectContinued: false, now: overTime,
				expectLastErrWasRetryable: false},
		},
		"Retry dead connection max": {
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: dbTransientErr, expectContinued: false,
				expectLastErrWasRetryable: true},
		},
		"Cluster error": {
			{conn: &testutil.ConnFake{Alive: true}, err: clusterErr, expectContinued: true,
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
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: io.EOF, isCommitting: true, expectContinued: false,
				expectLastErrWasRetryable: false, expectLastErrType: &errorutil.CommitFailedDeadError{}},
		},
		"Fail during commit after retry": {
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: true,
				expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: io.EOF, isCommitting: true,
				expectContinued: false, expectLastErrWasRetryable: false,
				expectLastErrType: &errorutil.CommitFailedDeadError{}},
		},
		"Does not retry on auth errors": {
			{conn: nil, err: authErr, expectContinued: false,
				expectLastErrWasRetryable: false},
		},
		"Does not retry on protocol errors": {
			{
				conn: &testutil.ConnFake{Alive: true},
				err: &db.ProtocolError{
					MessageType: "dateTimeNamedZone",
					Field:       "location",
					Err:         "unknown time zone wat/wat",
				},
				expectContinued: false, expectLastErrWasRetryable: false},
		},
	}

	ctx := context.Background()
	for i, testCase := range testCases {
		outer.Run(i, func(t *testing.T) {
			now := baseTime
			timer := func() time.Time { return now }
			state := State{
				Now:                     &timer,
				Log:                     &log.Void{},
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

				state.OnFailure(ctx, invocation.err, invocation.conn, invocation.isCommitting)
				continued := state.Continue()
				if continued != invocation.expectContinued {
					t.Errorf("Expected continue to return %v but returned %v", invocation.expectContinued, continued)
				}
				var lastError error
				if err, ok := state.Errs[0].(*errorutil.TransactionExecutionLimit); ok {
					errs := err.Errors
					lastError = errs[len(errs)-1]
				} else {
					lastError = state.Errs[len(state.Errs)-1]
				}

				if IsRetryable(lastError) != invocation.expectLastErrWasRetryable {
					t.Errorf("LastErrWasRetryable mismatch")
				}
				if invocation.expectLastErrType != nil {
					t1 := reflect.TypeOf(lastError)
					t2 := reflect.TypeOf(invocation.expectLastErrType)
					if t1 != t2 {
						t.Errorf("LastErr type mismatch: %s vs %s", t1, t2)
					}
				}
			}
		})
	}
}
