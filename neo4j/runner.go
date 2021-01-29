/*
 * Copyright (c) "Neo4j"
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

type runnerHandler func(*statementRunner) error
type phaseHandler func(*statementRunner, *neoResult) error
type resultHandler func(*statementRunner) (*neoResult, error)

type statementRunner struct {
	driver         *goboltDriver
	connection     gobolt.Connection
	autoClose      bool
	accessMode     AccessMode
	lastBookmark   string
	pendingResults []*neoResult

	closeHandler              runnerHandler
	receiveHandler            resultHandler
	receiveAllHandler         runnerHandler
	receiveAllAndCloseHandler runnerHandler
	runPhaseHandler           phaseHandler
	recordsPhaseHandler       phaseHandler
}

func newRunner(driver *goboltDriver, accessMode AccessMode, autoClose bool) *statementRunner {
	return &statementRunner{
		driver:     driver,
		accessMode: accessMode,
		autoClose:  autoClose,
	}
}

func (runner *statementRunner) lastSeenBookmark() (string, error) {
	if runner.connection != nil {
		var err error
		var bookmark string

		if bookmark, err = runner.connection.LastBookmark(); err != nil {
			runner.driver.config.Log.Errorf("LastBookmark call on connection failed: %v", err)
		}

		return bookmark, err
	}

	return runner.lastBookmark, nil
}

func (runner *statementRunner) id() string {
	var id = "unknown"
	var err error

	if runner.connection != nil {
		if id, err = runner.connection.Id(); err != nil {
			runner.driver.config.Log.Errorf("Id call on connection failed: %v", err)
			id = "unknown[failed to get id]"
		}
	}
	return id
}

func (runner *statementRunner) remoteAddress() string {
	var remoteAddress = "unknown"
	var err error

	if runner.connection != nil {
		if remoteAddress, err = runner.connection.RemoteAddress(); err != nil {
			runner.driver.config.Log.Errorf("RemoteAddress call on connection failed: %v", err)
			remoteAddress = "unknown[failed to get remote address]"
		}
	}
	return remoteAddress
}

func (runner *statementRunner) version() string {
	var version = "unknown"
	var err error

	if runner.connection != nil {
		if version, err = runner.connection.Server(); err != nil {
			runner.driver.config.Log.Errorf("Server call on connection failed: %v", err)
			version = "unknown[failed to get version text]"
		}
	}
	return version
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

// This receives all pending results and closes the connection
func (runner *statementRunner) receiveAllAndClose() error {
	if runner.receiveAllAndCloseHandler != nil {
		return runner.receiveAllAndCloseHandler(runner)
	}

	return receiveAllAndCloseHandler(runner)
}

func receiveAllAndCloseHandler(runner *statementRunner) error {
	receiveAllErr := runner.receiveAll()
	closeErr := runner.close()
	if receiveAllErr != nil {
		return receiveAllErr
	}
	return closeErr
}

// This closes current active connection that's bound to this runner and
// updates bookmark before actual closure
func (runner *statementRunner) close() error {
	if runner.closeHandler != nil {
		return runner.closeHandler(runner)
	}

	return closeHandler(runner)
}

func closeHandler(runner *statementRunner) error {
	if runner.connection != nil {
		var bookmark string
		var err error

		if bookmark, err = runner.connection.LastBookmark(); err != nil {
			runner.driver.config.Log.Errorf("LastBookmark call on connection failed: %v", err)
		} else {
			runner.lastBookmark = bookmark
		}

		if err = runner.connection.Close(); err != nil {
			return err
		}

		runner.connection = nil
	}

	return nil
}

func (runner *statementRunner) receiveAll() error {
	if runner.receiveAllHandler != nil {
		return runner.receiveAllHandler(runner)
	}

	return receiveAll(runner)
}

func receiveAll(runner *statementRunner) error {
	var errToReturn error = nil

	for len(runner.pendingResults) > 0 {
		// we don't care for errors here, because the errors are saved
		// into individual result objects
		if _, err := runner.receive(); err != nil {
			errToReturn = err
		}
	}

	return errToReturn
}

func (runner *statementRunner) handleRunPhase(activeResult *neoResult) error {
	if runner.runPhaseHandler != nil {
		return runner.runPhaseHandler(runner, activeResult)
	}

	return handleRunPhase(runner, activeResult)
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

		collectMetadata(activeResult, metadata)
		activeResult.runCompleted = true
	}

	return nil
}

func (runner *statementRunner) handleRecordsPhase(activeResult *neoResult) error {
	if runner.recordsPhaseHandler != nil {
		return runner.recordsPhaseHandler(runner, activeResult)
	}

	return handleRecordsPhase(runner, activeResult)
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

			collectMetadata(activeResult, metadata)
			activeResult.resultCompleted = true
		case gobolt.FetchTypeRecord:
			fields, err := runner.connection.Data()
			if err != nil {
				return err
			}

			collectRecord(activeResult, fields)
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

		return newSessionExpiredError("server at %s no longer accepts writes", runner.remoteAddress())
	}

	return err
}

func (runner *statementRunner) receive() (Result, error) {
	if runner.receiveHandler != nil {
		return runner.receiveHandler(runner)
	}

	return receive(runner)
}

func receive(runner *statementRunner) (Result, error) {
	var err error

	if len(runner.pendingResults) <= 0 {
		return nil, newDriverError("unexpected state: no pending results registered on session")
	}

	if runner.connection == nil {
		return nil, newDriverError("unexpected state: no open connection to perform receive")
	}

	activeResult := runner.pendingResults[0]

	if err = runner.handleRunPhase(activeResult); err != nil {
		// record error on the result and return error
		activeResult.err = transformError(runner, err)
		activeResult.runCompleted = true

		return nil, activeResult.err
	}

	if err = runner.handleRecordsPhase(activeResult); err != nil {
		// just record the error on the result
		activeResult.err = transformError(runner, err)
		activeResult.resultCompleted = true
	}

	if activeResult.resultCompleted {
		runner.pendingResults = runner.pendingResults[1:]
	}

	if len(runner.pendingResults) == 0 && runner.autoClose {
		if err = runner.close(); err != nil {
			return nil, err
		}
	}

	if activeResult.err != nil {
		return nil, activeResult.err
	}

	return activeResult, nil
}

func (runner *statementRunner) runStatement(statement *neoStatement, bookmarks []string, txConfig TransactionConfig) (*neoResult, error) {
	var runHandle, pullAllHandle gobolt.RequestHandle
	var err error

	if err = runner.ensureConnection(); err != nil {
		return nil, err
	}

	if runHandle, err = runner.connection.Run(statement.text, statement.params, bookmarks, txConfig.Timeout, txConfig.Metadata); err != nil {
		return nil, err
	}

	if pullAllHandle, err = runner.connection.PullAll(); err != nil {
		return nil, err
	}

	if err = runner.connection.Flush(); err != nil {
		return nil, err
	}

	result := &neoResult{
		runner:       runner,
		runHandle:    runHandle,
		resultHandle: pullAllHandle,
		summary: &neoResultSummary{
			statement: statement,
			server: &neoServerInfo{
				address: runner.remoteAddress(),
				version: runner.version(),
			},
			counters: &neoCounters{},
		},
	}

	runner.pendingResults = append(runner.pendingResults, result)

	return result, nil
}

func (runner *statementRunner) beginTransaction(bookmarks []string, txConfig TransactionConfig) (*neoResult, error) {
	var beginHandle gobolt.RequestHandle
	var err error

	if err = runner.assertNoConnection(); err != nil {
		return nil, err
	}

	if err = runner.ensureConnection(); err != nil {
		_ = runner.close()

		return nil, err
	}

	if beginHandle, err = runner.connection.Begin(bookmarks, txConfig.Timeout, txConfig.Metadata); err != nil {
		_ = runner.close()

		return nil, err
	}

	if err = runner.connection.Flush(); err != nil {
		_ = runner.close()

		return nil, err
	}

	beginResult := &neoResult{runner: runner, runCompleted: true, resultHandle: beginHandle}

	runner.pendingResults = append(runner.pendingResults, beginResult)

	return beginResult, nil
}

func (runner *statementRunner) commitTransaction() (*neoResult, error) {
	var commitHandle gobolt.RequestHandle
	var err error

	if err = runner.assertConnection(); err != nil {
		return nil, err
	}

	if commitHandle, err = runner.connection.Commit(); err != nil {
		return nil, err
	}

	if err = runner.connection.Flush(); err != nil {
		return nil, err
	}

	rollbackResult := &neoResult{runner: runner, runCompleted: true, resultHandle: commitHandle}

	runner.pendingResults = append(runner.pendingResults, rollbackResult)

	return rollbackResult, nil
}

func (runner *statementRunner) rollbackTransaction() (*neoResult, error) {
	var rollbackHandle gobolt.RequestHandle
	var err error

	if err = runner.assertConnection(); err != nil {
		return nil, err
	}

	if rollbackHandle, err = runner.connection.Rollback(); err != nil {
		return nil, err
	}

	if err = runner.connection.Flush(); err != nil {
		return nil, err
	}

	rollbackResult := &neoResult{runner: runner, runCompleted: true, resultHandle: rollbackHandle}

	runner.pendingResults = append(runner.pendingResults, rollbackResult)

	return rollbackResult, nil
}
