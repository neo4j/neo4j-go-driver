/*
 * Copyright (c) "Neo4j"
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
	"sync/atomic"
)

type neoSession struct {
	driver     *goboltDriver
	accessMode AccessMode
	bookmarks  []string

	lastBookmark string

	open   int32
	tx     *neoTransaction
	runner *statementRunner
}

func newSession(driver *goboltDriver, accessMode AccessMode, bookmarks []string) Session {
	// filter out bookmarks with empty string
	bookmarks = filter(bookmarks, func(s string) bool {
		return len(s) > 0
	})

	return &neoSession{
		driver:       driver,
		accessMode:   accessMode,
		bookmarks:    bookmarks,
		lastBookmark: "",
		open:         1,
		tx:           nil,
		runner:       nil,
	}
}

func assertSessionOpen(session *neoSession) error {
	if atomic.LoadInt32(&session.open) == 0 {
		return newDriverError("session is already closed")
	}

	return nil
}

// This ensures that we're in a good state to run statements on this
// session
func ensureReady(session *neoSession) error {
	if err := assertSessionOpen(session); err != nil {
		return nil
	}

	if session.tx != nil {
		return newDriverError("there's already an open transaction on this session")
	}

	if session.runner != nil {
		if err := session.runner.receiveAll(); err != nil {
			return err
		}
	}

	return nil
}

// This ensures that we've a connection to run statements against
func ensureRunner(session *neoSession, mode AccessMode, autoClose bool) error {
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
func closeRunner(session *neoSession) error {
	if session.runner != nil {
		var err error
		var bookmark string

		err = session.runner.receiveAllAndClose()

		if bookmark, err = session.runner.lastSeenBookmark(); err == nil {
			session.lastBookmark = bookmark
		}

		session.runner = nil
		return err
	}

	return nil
}

func (session *neoSession) id() string {
	id := "unknown"
	if session.runner != nil {
		id = session.runner.id()
	}
	return id
}

func (session *neoSession) LastBookmark() string {
	if session.runner != nil {
		var err error
		var bookmark string

		if bookmark, err = session.runner.lastSeenBookmark(); err == nil {
			return bookmark
		}
	}

	return session.lastBookmark
}

func (session *neoSession) BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error) {
	return beginTransactionInternal(session, session.accessMode, configurers...)
}

func (session *neoSession) ReadTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {
	return runTransaction(session, AccessModeRead, work, configurers...)
}

func (session *neoSession) WriteTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {
	return runTransaction(session, AccessModeWrite, work, configurers...)
}

func (session *neoSession) Run(cypher string, params map[string]interface{}, configurers ...func(*TransactionConfig)) (Result, error) {
	return runStatementOnSession(session, &neoStatement{text: cypher, params: params}, configurers...)
}

func (session *neoSession) Close() error {
	if atomic.CompareAndSwapInt32(&session.open, 1, 0) {
		if err := closeRunner(session); err != nil {
			return err
		}
	}

	return nil
}

func computeTransactionConfig(configurers ...func(config *TransactionConfig)) TransactionConfig {
	config := TransactionConfig{Timeout: 0, Metadata: nil}

	for _, configurer := range configurers {
		configurer(&config)
	}

	return config
}

func computeBookmarks(session *neoSession) []string {
	var computedBookmarks []string

	computedBookmarks = append(computedBookmarks, session.bookmarks...)
	if session.lastBookmark != "" {
		computedBookmarks = append(computedBookmarks, session.lastBookmark)
	}

	return computedBookmarks
}

func beginTransactionInternal(session *neoSession, mode AccessMode, configurers ...func(*TransactionConfig)) (Transaction, error) {
	if err := ensureReady(session); err != nil {
		return nil, err
	}

	if err := ensureRunner(session, mode, false); err != nil {
		return nil, err
	}

	beginResult, err := session.runner.beginTransaction(computeBookmarks(session), computeTransactionConfig(configurers...))
	if err != nil {
		return nil, err
	}

	if _, err := beginResult.Consume(); err != nil {
		defer closeRunner(session)

		return nil, err
	}

	transaction := &neoTransaction{session: session, beginResult: beginResult}
	session.tx = transaction
	return transaction, nil
}

func runStatementOnSession(session *neoSession, statement *neoStatement, configurers ...func(*TransactionConfig)) (Result, error) {
	if err := statement.validate(); err != nil {
		return nil, err
	}

	if err := ensureReady(session); err != nil {
		return nil, err
	}

	if err := ensureRunner(session, session.accessMode, true); err != nil {
		return nil, err
	}

	result, err := session.runner.runStatement(statement, computeBookmarks(session), computeTransactionConfig(configurers...))
	if err != nil {
		return nil, err
	}

	return result, nil
}

func runTransaction(session *neoSession, mode AccessMode, work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {
	retry := newRetryLogic(session.driver.configuration())

	result, err := retry.retry(func() (interface{}, string, error) {
		tx, errWork := beginTransactionInternal(session, mode, configurers...)
		if errWork != nil {
			return nil, session.id(), errWork
		}
		defer tx.Close()

		resultWork, errWork := work(tx)
		if errWork != nil {
			return nil, session.id(), errWork
		}

		errWork = tx.Commit()
		if errWork != nil {
			return nil, session.id(), errWork
		}

		return resultWork, "", nil
	})

	return result, err
}
