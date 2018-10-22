/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://seabolt.com]
 *
 * This file is part of seabolt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"github.com/neo4j-drivers/gobolt"
)

type statementRunner struct {
	driver         *goboltDriver
	connection     gobolt.Connection
	autoClose      bool
	accessMode     AccessMode
	lastBookmark   string
	pendingResults []*neoResult
}

func newRunner(driver *goboltDriver, accessMode AccessMode, autoClose bool) *statementRunner {
	return &statementRunner{driver: driver, accessMode: accessMode, autoClose: autoClose}
}

func (runner *statementRunner) lastSeenBookmark() string {
	if runner.connection != nil {
		return runner.connection.LastBookmark()
	}

	return runner.lastBookmark
}

func (runner *statementRunner) assertConnection() error {
	if runner.connection == nil {
		return newDriverError("unexpected state: expected an active connection bound to this runner")
	}

	return nil
}

func (runner *statementRunner) assertNoConnection() error {
	if runner.connection != nil {
		return newDriverError("unexpected state: expected no connection bound to this runner")
	}

	return nil
}

// This ensures that we've a connection to run statements against
func (runner *statementRunner) ensureConnection() error {
	if runner.connection == nil {
		connection, err := runner.driver.acquire(runner.accessMode)
		if err != nil {
			return err
		}

		runner.connection = connection
	}

	return nil
}

// This closes any active connection that's bound to this session and
// updates bookmark before actual closure
func (runner *statementRunner) closeConnection() error {
	if runner.connection != nil {
		runner.lastBookmark = runner.connection.LastBookmark()

		err := runner.connection.Close()
		runner.connection = nil
		return err
	}

	return nil
}

func (runner *statementRunner) receiveAll() error {
	for len(runner.pendingResults) > 0 {
		// we don't care for errors here, because the errors are saved
		// into individual result objects
		runner.receive()
	}

	return nil
}

func handleRunPhase(runner *statementRunner, activeResult *neoResult) error {
	if !activeResult.runCompleted {
		received, err := runner.connection.Fetch(activeResult.runHandle)
		if err != nil {
			return err
		}

		if received != gobolt.FetchTypeMetadata {
			return newDriverError("unexpected response received while waiting for a METADATA")
		}

		fields, err := runner.connection.Fields()
		if err != nil {
			return err
		}
		activeResult.keys = fields

		metadata, err := runner.connection.Metadata()
		if err != nil {
			return err
		}

		activeResult.collectMetadata(metadata)
		activeResult.runCompleted = true
	}

	return nil
}

func handleRecordsPhase(runner *statementRunner, activeResult *neoResult) error {
	if !activeResult.resultCompleted {
		received, err := runner.connection.Fetch(activeResult.resultHandle)
		if err != nil {
			return err
		}

		switch received {
		case gobolt.FetchTypeMetadata:
			metadata, err := runner.connection.Metadata()
			if err != nil {
				return err
			}

			activeResult.collectMetadata(metadata)
			activeResult.resultCompleted = true
		case gobolt.FetchTypeRecord:
			fields, err := runner.connection.Data()
			if err != nil {
				return err
			}

			activeResult.collectRecord(fields)
		case gobolt.FetchTypeError:
			return newDriverError("unable to fetch from connection")
		}
	}

	return nil
}

func transformError(runner *statementRunner, err error) error {
	if gobolt.IsWriteError(err) {
		if runner.accessMode == AccessModeRead {
			return newDriverError("write queries cannot be performed in read access mode")
		}

		return newSessionExpiredError("server at %s no longer accepts writes", runner.connection.RemoteAddress())
	}

	return err
}

func (runner *statementRunner) receive() (Result, error) {
	if len(runner.pendingResults) <= 0 {
		return nil, newDriverError("unexpected state: no pending results registered on session")
	}

	if runner.connection == nil {
		return nil, newDriverError("unexpected state: no open connection to perform receive")
	}

	activeResult := runner.pendingResults[0]

	if err := handleRunPhase(runner, activeResult); err != nil {
		// record error on the result and return error
		activeResult.err = transformError(runner, err)
		activeResult.runCompleted = true

		return nil, activeResult.err
	}

	if err := handleRecordsPhase(runner, activeResult); err != nil {
		// just record the error on the result
		activeResult.err = transformError(runner, err)
		activeResult.resultCompleted = true
	}

	if activeResult.resultCompleted {
		runner.pendingResults = runner.pendingResults[1:]
	}

	if len(runner.pendingResults) == 0 && runner.autoClose {
		runner.closeConnection()
	}

	return activeResult, nil
}

func (runner *statementRunner) runStatement(statement *neoStatement, bookmarks []string, txConfig TransactionConfig) (Result, error) {
	if err := runner.ensureConnection(); err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	runHandle, err := runner.connection.Run(statement.text, statement.params, bookmarks, txConfig.Timeout, txConfig.Metadata)
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}
	pullAllHandle, err := runner.connection.PullAll()
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	err = runner.connection.Flush()
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	result := &neoResult{
		runner:       runner,
		runHandle:    runHandle,
		resultHandle: pullAllHandle,
		summary: &neoResultSummary{
			statement: statement,
			server: &neoServerInfo{
				address: runner.connection.RemoteAddress(),
				version: runner.connection.Server(),
			},
			counters: &neoCounters{},
		},
	}

	runner.pendingResults = append(runner.pendingResults, result)

	return result, nil
}

func (runner *statementRunner) beginTransaction(bookmarks []string, txConfig TransactionConfig) (Result, error) {
	if err := runner.assertNoConnection(); err != nil {
		return nil, err
	}

	if err := runner.ensureConnection(); err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	beginHandle, err := runner.connection.Begin(bookmarks, txConfig.Timeout, txConfig.Metadata)
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	err = runner.connection.Flush()
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	beginResult := &neoResult{runner: runner, runCompleted: true, resultHandle: beginHandle}

	runner.pendingResults = append(runner.pendingResults, beginResult)

	return beginResult, nil
}

func (runner *statementRunner) commitTransaction() (Result, error) {
	if err := runner.assertConnection(); err != nil {
		return nil, err
	}

	commitHandle, err := runner.connection.Commit()
	if err != nil {
		return nil, err
	}

	err = runner.connection.Flush()
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	rollbackResult := &neoResult{runner: runner, runCompleted: true, resultHandle: commitHandle}

	runner.pendingResults = append(runner.pendingResults, rollbackResult)

	return rollbackResult, nil
}

func (runner *statementRunner) rollbackTransaction() (Result, error) {
	if err := runner.assertConnection(); err != nil {
		return nil, err
	}

	rollbackHandle, err := runner.connection.Rollback()
	if err != nil {
		return nil, err
	}

	if err = runner.connection.Flush(); err != nil {
		return nil, err
	}

	rollbackResult := &neoResult{runner: runner, runCompleted: true, resultHandle: rollbackHandle}

	runner.pendingResults = append(runner.pendingResults, rollbackResult)

	return rollbackResult, nil
}
