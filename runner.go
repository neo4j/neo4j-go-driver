/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

package neo4j_go_driver

import (
    "errors"
    neo4j "neo4j-go-connector"
)

type statementRunner struct {
	driver         Driver
	connection     neo4j.Connection
	autoClose      bool
	accessMode     AccessMode
	lastBookmark   string
	pendingResults []*Result
}

func newRunner(driver Driver, accessMode AccessMode, autoClose bool) *statementRunner {
	return &statementRunner{driver: driver, accessMode: accessMode, autoClose: autoClose}
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
		if _, err := runner.receive(); err != nil {
			return err
		}
	}

	return nil
}

func (runner *statementRunner) receive() (*Result, error) {
    if len(runner.pendingResults) <= 0 {
		return nil, errors.New("unexpected state: no pending results registered on session")
	}

	if runner.connection == nil {
	    return nil, errors.New("unexpected state: no open connection to perform receive")
    }

	activeResult := runner.pendingResults[0]
	if !activeResult.runCompleted {
		received, err := runner.connection.Fetch(activeResult.runHandle)
		if err != nil {
			return nil, err
		}

		if received != neo4j.METADATA {
			return nil, errors.New("unexpected response received while waiting for a METADATA")
		}

		metadata, err := runner.connection.Metadata()
		if err != nil {
			return nil, err
		}

		activeResult.collectMetadata(metadata)
		activeResult.runCompleted = true

		return activeResult, nil
	}

	if !activeResult.resultCompleted {
		received, err := runner.connection.Fetch(activeResult.resultHandle)
		if err != nil {
			return nil, err
		}

		switch received {
		case neo4j.METADATA:
			metadata, err := runner.connection.Metadata()
			if err != nil {
				return nil, err
			}

			activeResult.collectMetadata(metadata)
			activeResult.resultCompleted = true
		case neo4j.RECORD:
			fields, err := runner.connection.Data()
			if err != nil {
				return nil, err
			}

			activeResult.collectRecord(fields)
		case neo4j.ERROR:
			return nil, errors.New("unable to fetch from connection")
		}
	}

	if activeResult.resultCompleted {
		runner.pendingResults = runner.pendingResults[1:]
	}

	if len(runner.pendingResults) == 0 && runner.autoClose {
		runner.closeConnection()
	}

	return activeResult, nil
}

func (runner *statementRunner) runStatement(statement Statement) (*Result, error) {
	if err := runner.ensureConnection(); err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	runHandle, err := runner.connection.Run(statement.cypher, statement.params)
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

	result := &Result{
		runner:       runner,
		runHandle:    runHandle,
		resultHandle: pullAllHandle,
	}

	runner.pendingResults = append(runner.pendingResults, result)

	return result, nil
}

func (runner *statementRunner) beginTransaction(bookmarks []string) (*Result, error) {
	// TODO: assert no connection

	if err := runner.ensureConnection(); err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	beginHandle, err := runner.connection.Begin(bookmarks)
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	err = runner.connection.Flush()
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	beginResult := &Result{runner: runner, runCompleted: true, resultHandle: beginHandle}

	runner.pendingResults = append(runner.pendingResults, beginResult)

	return beginResult, nil
}

func (runner *statementRunner) commitTransaction() (*Result, error) {
	// TODO: assert connection

	commitHandle, err := runner.connection.Commit()
	if err != nil {
		return nil, err
	}

	err = runner.connection.Flush()
	if err != nil {
		defer runner.closeConnection()

		return nil, err
	}

	rollbackResult := &Result{runner: runner, runCompleted: true, resultHandle: commitHandle}

	runner.pendingResults = append(runner.pendingResults, rollbackResult)

	return rollbackResult, nil
}

func (runner *statementRunner) rollbackTransaction() (*Result, error) {
	// TODO: assert connection

	rollbackHandle, err := runner.connection.Rollback()
	if err != nil {
		return nil, err
	}

	if err = runner.connection.Flush(); err != nil {
		return nil, err
	}

	rollbackResult := &Result{runner: runner, runCompleted: true, resultHandle: rollbackHandle}

	runner.pendingResults = append(runner.pendingResults, rollbackResult)

	return rollbackResult, nil
}
