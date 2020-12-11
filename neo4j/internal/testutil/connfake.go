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

package testutil

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type Next struct {
	Record  *db.Record
	Summary *db.Summary
	Err     error
}

type RecordedTx struct {
	Origin    string
	Mode      db.AccessMode
	Bookmarks []string
	Timeout   time.Duration
	Meta      map[string]interface{}
}

type ConnFake struct {
	Name          string
	Version       string
	Alive         bool
	Birth         time.Time
	Table         *db.RoutingTable
	Err           error
	Id            int
	TxBeginHandle db.TxHandle
	RunStream     db.StreamHandle
	RunTxStream   db.StreamHandle
	Nexts         []Next
	Bookm         string
	TxCommitErr   error
	TxCommitHook  func()
	TxRollbackErr error
	ResetHook     func()
	ConsumeSum    *db.Summary
	ConsumeErr    error
	ConsumeHook   func()
	RecordedTxs   []RecordedTx // Appended to by Run/TxBegin
	BufferErr     error
	BufferHook    func()
}

func (c *ConnFake) ServerName() string {
	return c.Name
}

func (c *ConnFake) IsAlive() bool {
	return c.Alive
}

func (c *ConnFake) Reset() {
}

func (c *ConnFake) Close() {
}

func (c *ConnFake) Birthdate() time.Time {
	return c.Birth
}

func (c *ConnFake) Bookmark() string {
	return c.Bookm
}

func (c *ConnFake) ServerVersion() string {
	return "serverVersion"
}

func (c *ConnFake) Buffer(streamHandle db.StreamHandle) error {
	if c.BufferHook != nil {
		c.BufferHook()
	}
	return c.BufferErr
}

func (c *ConnFake) Consume(streamHandle db.StreamHandle) (*db.Summary, error) {
	if c.ConsumeHook != nil {
		c.ConsumeHook()
	}
	return c.ConsumeSum, c.ConsumeErr
}

func (c *ConnFake) GetRoutingTable(database string, context map[string]string) (*db.RoutingTable, error) {
	return c.Table, c.Err
}

func (c *ConnFake) TxBegin(txConfig db.TxConfig) (db.TxHandle, error) {
	c.RecordedTxs = append(c.RecordedTxs, RecordedTx{Origin: "TxBegin", Mode: txConfig.Mode, Bookmarks: txConfig.Bookmarks, Timeout: txConfig.Timeout, Meta: txConfig.Meta})
	return c.TxBeginHandle, c.Err
}

func (c *ConnFake) TxRollback(tx db.TxHandle) error {
	return c.TxRollbackErr
}

func (c *ConnFake) TxCommit(tx db.TxHandle) error {
	if c.TxCommitHook != nil {
		c.TxCommitHook()
	}
	return c.TxCommitErr
}

func (c *ConnFake) Run(runCommand db.Command, txConfig db.TxConfig) (db.StreamHandle, error) {

	c.RecordedTxs = append(c.RecordedTxs, RecordedTx{Origin: "Run", Mode: txConfig.Mode, Bookmarks: txConfig.Bookmarks, Timeout: txConfig.Timeout, Meta: txConfig.Meta})
	return c.RunStream, c.Err
}

func (c *ConnFake) RunTx(tx db.TxHandle, runCommand db.Command) (db.StreamHandle, error) {
	return c.RunTxStream, c.Err
}

func (c *ConnFake) Keys(streamHandle db.StreamHandle) ([]string, error) {
	return nil, nil
}

func (c *ConnFake) Next(streamHandle db.StreamHandle) (*db.Record, *db.Summary, error) {
	next := c.Nexts[0]
	if len(c.Nexts) > 1 {
		c.Nexts = c.Nexts[1:]
	}
	return next.Record, next.Summary, next.Err
}
