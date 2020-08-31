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
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

type Router interface {
	Invalidate(database string)
}

type State struct {
	IsRetryable             bool
	Err                     error
	MaxTransactionRetryTime time.Duration
	Log                     log.Logger
	LogName                 string
	LogId                   string
	Now                     func() time.Time
	Sleep                   func(time.Duration)
	Throttle                Throttler
	MaxDeadConnections      int
	Router                  Router
	DatabaseName            string

	start      time.Time
	cause      string
	deadErrors int
	skipSleep  bool
}

func (s *State) OnFailure(conn db.Connection, err error, isCommitting bool) {
	s.Err = err
	s.IsRetryable = false
	s.cause = ""
	s.skipSleep = false

	// Check timeout
	if s.start.IsZero() {
		s.start = s.Now()
	}
	if s.Now().Sub(s.start) > s.MaxTransactionRetryTime {
		s.cause = "Timeout"
		return
	}

	// Failed to connect
	if conn == nil {
		s.IsRetryable = true
		s.cause = "No available connection"
		return
	}

	// Check if the connection died, if it died during commit it is not safe to retry.
	if !conn.IsAlive() {
		if isCommitting {
			s.cause = "Connection lost during commit"
			return
		}

		s.deadErrors += 1
		s.IsRetryable = s.deadErrors <= s.MaxDeadConnections
		s.cause = "Connection lost"
		s.skipSleep = true
		return
	}

	if dbErr, isDbErr := err.(*db.DatabaseError); isDbErr {
		if dbErr.IsRetriableCluster() {
			// Force routing tables to be updated before trying again
			s.Router.Invalidate(s.DatabaseName)
			s.cause = "Cluster error"
			s.IsRetryable = true
			return
		}

		if dbErr.IsRetriableTransient() {
			s.cause = "Transient error"
			s.IsRetryable = true
			return
		}
	}
}

func (s *State) Continue() bool {
	if s.Err == nil {
		return true
	}

	if !s.IsRetryable {
		// When there is a cause we failed due to a reason we should be able to handle, therefore
		// log it as an error. The transaction could fail due to many other reasons, not sure if
		// these are errors that the driver should log.
		if s.cause != "" {
			s.Log.Errorf(s.LogName, s.LogId, "Transaction failed (%s): %s", s.cause, s.Err)
		}
		return false
	}

	if s.skipSleep {
		s.Log.Debugf(s.LogName, s.LogId, "Retrying transaction (%s): %s", s.cause, s.Err)
	} else {
		s.Throttle = s.Throttle.next()
		sleepTime := s.Throttle.delay()
		s.Log.Debugf(s.LogName, s.LogId, "Retrying transaction (%s): %s [after %s]", s.cause, s.Err, sleepTime)
		s.Sleep(sleepTime)
	}

	// Reset
	s.Err = nil
	s.IsRetryable = false
	return true
}
