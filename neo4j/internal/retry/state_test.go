//go:build internal_time_mock

/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type TStateInvocation struct {
	conn                      idb.Connection
	err                       error
	isCommitting              bool
	freezeTime                bool
	tick                      time.Duration
	expectContinued           bool
	expectLastErrWasRetryable bool
	expectLastErrType         error
}

func TestState(outer *testing.T) {
	var (
		maxRetryTime = time.Second * 10
		halfTime     = maxRetryTime / 2
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
			{conn: nil, err: dbTransientErr, expectContinued: true, freezeTime: true,
				expectLastErrWasRetryable: true},
			{conn: nil, err: dbTransientErr, expectContinued: true, tick: halfTime,
				expectLastErrWasRetryable: true},
			{conn: nil, err: dbTransientErr, expectContinued: false, tick: halfTime + 1*time.Second,
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
				expectContinued: true, freezeTime: true, expectLastErrWasRetryable: true},
			{conn: &testutil.ConnFake{Name: serverName, Alive: false}, err: errors.New("some error 2"),
				expectContinued: false, tick: 2*halfTime + 1*time.Second,
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
				expectLastErrWasRetryable: true, freezeTime: true},
			{conn: &testutil.ConnFake{Alive: true}, err: dbTransientErr, expectContinued: false, tick: 2*halfTime + 1*time.Second,
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
			state := State{
				Log:                     log.ToVoid(),
				LogName:                 "TEST",
				LogId:                   "State",
				MaxTransactionRetryTime: maxRetryTime,
				MaxDeadConnections:      maxDead,
				DatabaseName:            dbName,
			}
			for _, invocation := range testCase {
				if invocation.freezeTime {
					itime.ForceFreezeTime()
					defer itime.ForceUnfreezeTime()
				}
				if invocation.tick > 0 {
					itime.ForceTickTime(invocation.tick)
				}

				state.OnFailure(ctx, invocation.err, invocation.conn, invocation.isCommitting)
				continued := state.Continue(ctx)
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

func TestContextCancel(t *testing.T) {
	state := State{
		Log:                     log.ToVoid(),
		LogName:                 "TEST",
		LogId:                   "State",
		MaxTransactionRetryTime: time.Second * 10,
		Throttle:                Throttler(time.Second * 10),
		Errs: []error{&errorutil.PoolTimeout{
			Err:     errors.New("dummy error"),
			Servers: nil,
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())

	waitCh := make(chan struct{})
	go func() {
		state.Continue(ctx)
		close(waitCh)
	}()

	<-time.After(time.Second * 1)
	cancel()

	select {
	case <-time.After(time.Second * 1):
		t.Fatal("continue did not exit after context was canceled")
	case <-waitCh:
	}
}
