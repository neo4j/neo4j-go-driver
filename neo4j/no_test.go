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

package neo4j

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

func assertErrorEq(t *testing.T, err1, err2 error) {
	if !reflect.DeepEqual(err1, err2) {
		t.Errorf("Wrong type of error, '%s' != '%s'", err1, err2)
	}
}

// Fake implementation of router
type testRouter struct {
	readers []string
	writers []string
	err     error
}

func (r *testRouter) Readers(database string) ([]string, error) {
	return r.readers, r.err
}

func (r *testRouter) Writers(database string) ([]string, error) {
	return r.writers, r.err
}

func (r *testRouter) Invalidate(database string) {
}

func (r *testRouter) CleanUp() {
}

// Fake implementation of connection pool
type testPool struct {
	borrowConn pool.Connection
	err        error
	returnHook func()
}

func (p *testPool) Borrow(ctx context.Context, serverNames []string, wait bool) (pool.Connection, error) {
	return p.borrowConn, p.err
}

func (p *testPool) Return(c pool.Connection) {
	if p.returnHook != nil {
		p.returnHook()
	}
}

func (p *testPool) CleanUp() {
}

// Fake implementation of db connection
type testConn struct {
	err           error
	txBeginHandle db.Handle
	runStream     *db.Stream
	runTxStream   *db.Stream
	nextRecord    *db.Record
	nextSummary   *db.Summary
	bookmark      string
	serverName    string
	serverVersion string
	isAlive       bool
	birthDate     time.Time
	txCommitErr   error
	txRollbackErr error
	resetHook     func()
}

func (c *testConn) TxBegin(mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (db.Handle, error) {
	return c.txBeginHandle, c.err
}

func (c *testConn) TxRollback(tx db.Handle) error {
	return c.txRollbackErr
}

func (c *testConn) TxCommit(tx db.Handle) error {
	return c.txCommitErr
}

func (c *testConn) Run(cypher string, params map[string]interface{}, mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (*db.Stream, error) {
	return c.runStream, c.err
}

func (c *testConn) RunTx(tx db.Handle, cypher string, params map[string]interface{}) (*db.Stream, error) {
	return c.runTxStream, c.err
}

func (c *testConn) Next(streamHandle db.Handle) (*db.Record, *db.Summary, error) {
	return c.nextRecord, c.nextSummary, c.err
}

func (c *testConn) Bookmark() string {
	return c.bookmark
}

func (c *testConn) ServerName() string {
	return c.serverName
}

func (c *testConn) ServerVersion() string {
	return c.serverVersion
}

func (c *testConn) IsAlive() bool {
	return c.isAlive
}

func (c *testConn) Birthdate() time.Time {
	return c.birthDate
}

func (c *testConn) Reset() {
	if c.resetHook != nil {
		c.resetHook()
	}
}

func (c *testConn) Close() {
}
