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

// Package retry handles retry operations.
package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type State struct {
	Errs                    []error
	MaxTransactionRetryTime time.Duration
	Log                     log.Logger
	LogName                 string
	LogId                   string
	Sleep                   func(time.Duration)
	Throttle                Throttler
	MaxDeadConnections      int
	DatabaseName            string
	TelemetrySent           bool

	start      time.Time
	cause      string
	deadErrors int
	skipSleep  bool
}

func (s *State) OnFailure(_ context.Context, err error, conn idb.Connection, isCommitting bool) {
	if conn != nil && !conn.IsAlive() {
		if isCommitting {
			// FIXME: CommitFailedDeadError should be returned even when not using transaction functions
			s.Errs = append(s.Errs, &errorutil.CommitFailedDeadError{Inner: err})
		} else {
			s.Errs = append(s.Errs, err)
		}
		s.deadErrors += 1
		s.skipSleep = true
		return
	}

	s.Errs = append(s.Errs, err)
	s.skipSleep = false
}

func (s *State) Continue() bool {
	if s.start.IsZero() {
		s.start = itime.Now()
	}

	if len(s.Errs) == 0 {
		return true
	}

	lastErr := s.Errs[len(s.Errs)-1]
	if !IsRetryable(errorutil.WrapError(lastErr)) {
		return false
	}

	if itime.Since(s.start) > s.MaxTransactionRetryTime {
		s.Errs = []error{&errorutil.TransactionExecutionLimit{
			Cause:  fmt.Sprintf("timeout (exceeded max retry time: %s)", s.MaxTransactionRetryTime.String()),
			Errors: s.Errs,
		}}
		return false
	}

	if s.deadErrors > s.MaxDeadConnections {
		s.Errs = []error{&errorutil.TransactionExecutionLimit{
			Cause:  fmt.Sprintf("too many failed connection attempts (allowed max %d)", s.MaxDeadConnections),
			Errors: s.Errs,
		}}
		return false
	}

	if s.skipSleep {
		s.Log.Debugf(s.LogName, s.LogId, "Retrying transaction (%s): %s", s.cause, lastErr)
	} else {
		s.Throttle = s.Throttle.next()
		sleepTime := s.Throttle.delay()
		s.Log.Debugf(s.LogName, s.LogId,
			"Retrying transaction (%s): %s [after %s]", s.cause, lastErr, sleepTime)
		s.Sleep(sleepTime)
	}
	return true
}

func (s *State) ProduceError() error {
	lastErr := s.Errs[len(s.Errs)-1]
	if limitReachedErr, ok := lastErr.(*errorutil.TransactionExecutionLimit); ok {
		return limitReachedErr
	}
	return errorutil.WrapError(lastErr)
}

func IsRetryable(err error) bool {
	if connectivityErr, ok := err.(*errorutil.ConnectivityError); ok {
		if _, ok := connectivityErr.Inner.(*errorutil.CommitFailedDeadError); ok {
			return false
		}
		return true
	}
	if _, ok := err.(*errorutil.PoolTimeout); ok {
		return true
	}
	var dbError *db.Neo4jError
	if !errors.As(err, &dbError) {
		return false
	}
	return dbError.IsRetriable()
}
