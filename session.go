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

package neo4j

import (
	"errors"
)

// Session represents a logical connection (which is not tied to a physical connection)
// to the server
type Session struct {
	driver     Driver
	accessMode AccessMode
	bookmarks  []string

	lastBookmark string

	open   bool
	tx     *Transaction
	runner *statementRunner
}

func newSession(driver Driver, accessMode AccessMode, bookmarks []string) *Session {
	// filter out bookmarks with empty string
	bookmarks = filter(bookmarks, func(s string) bool {
		return len(s) > 0
	})

	return &Session{
		driver:       driver,
		accessMode:   accessMode,
		bookmarks:    bookmarks,
		lastBookmark: "",
		open:         true,
		tx:           nil,
		runner:       nil,
	}
}

// This ensures that we're in a good state to run statements on this
// session
func ensureReady(session *Session) error {
	if !session.open {
		return errors.New("session is already closed")
	}

	if session.tx != nil {
		return errors.New("there's already an open transaction on this session")
	}

	if session.runner != nil {
		if err := session.runner.receiveAll(); err != nil {
			return err
		}
	}

	return nil
}

// This ensures that we've a connection to run statements against
func ensureRunner(session *Session, mode AccessMode, autoClose bool) error {
	if session.runner != nil && (session.runner.autoClose != autoClose || session.runner.accessMode != mode) {
		closeRunner(session)
	}

	if session.runner == nil {
		session.runner = newRunner(session.driver, mode, autoClose)
	}

	return nil
}

// This closes any active connection that's bound to this session and
// updates bookmark before actual closure
func closeRunner(session *Session) error {
	if session.runner != nil {
		err := session.runner.closeConnection()

		session.lastBookmark = session.runner.lastSeenBookmark()

		session.runner = nil
		return err
	}

	return nil
}

func (session *Session) LastBookmark() string {
	if session.runner != nil {
		return session.runner.lastSeenBookmark()
	}

	return session.lastBookmark
}

// BeginTransaction starts a new explicit transaction on this session
func (session *Session) BeginTransaction() (*Transaction, error) {
	return beginTransactionInternal(session, session.accessMode)
}

// ReadTransaction executes the given unit of work in a AccessModeRead transaction with
// retry logic in place
func (session *Session) ReadTransaction(work TransactionWork) (interface{}, error) {
	return runTransaction(session, AccessModeRead, work)
}

// WriteTransaction executes the given unit of work in a AccessModeWrite transaction with
// retry logic in place
func (session *Session) WriteTransaction(work TransactionWork) (interface{}, error) {
	return runTransaction(session, AccessModeWrite, work)
}

// Run executes an auto-commit statement and returns a result
func (session *Session) Run(cypher string, params *map[string]interface{}) (*Result, error) {
	return runStatement(session, &Statement{cypher: cypher, params: params})
}

// Close closes any open resources and marks this session as unusable
func (session *Session) Close() error {
	if err := closeRunner(session); err != nil {
		return err
	}

	session.open = false

	return nil
}

func beginTransactionInternal(session *Session, mode AccessMode) (*Transaction, error) {
	if err := ensureReady(session); err != nil {
		return nil, err
	}

	// TODO: ensure no active results

	if err := ensureRunner(session, mode, false); err != nil {
		return nil, err
	}

	beginResult, err := session.runner.beginTransaction(session.bookmarks)
	if err != nil {
		return nil, err
	}

	if _, err := beginResult.Consume(); err != nil {
		defer closeRunner(session)

		return nil, err
	}

	transaction := &Transaction{session: session, beginResult: beginResult}
	session.tx = transaction
	return transaction, nil
}

func runStatement(session *Session, statement *Statement) (*Result, error) {
	if err := statement.validate(); err != nil {
		return nil, err
	}

	if err := ensureReady(session); err != nil {
		return nil, err
	}

	if err := ensureRunner(session, session.accessMode, true); err != nil {
		return nil, err
	}

	result, err := session.runner.runStatement(*statement)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func runTransaction(session *Session, mode AccessMode, work TransactionWork) (interface{}, error) {
	retry := newRetryLogic(session.driver.configuration())

	result, err := retry.retry(func() (interface{}, error) {
		tx, errWork := beginTransactionInternal(session, mode)
		if errWork != nil {
			return nil, errWork
		}
		defer tx.Close()

		resultWork, errWork := work(tx)
		if errWork != nil {
			return nil, errWork
		}

		errWork = tx.Commit()
		if errWork != nil {
			return nil, errWork
		}

		return resultWork, nil
	})

	return result, err
}
