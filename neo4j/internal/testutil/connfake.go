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

type ConnFake struct {
	Name          string
	Version       string
	Alive         bool
	Birth         time.Time
	Table         *db.RoutingTable
	Err           error
	Id            int
	TxBeginHandle db.Handle
	RunStream     *db.Stream
	RunTxStream   *db.Stream
	NextRecord    *db.Record
	NextSummary   *db.Summary
	Bookm         string
	TxCommitErr   error
	TxRollbackErr error
	ResetHook     func()
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

func (c *ConnFake) GetRoutingTable(database string, context map[string]string) (*db.RoutingTable, error) {
	return c.Table, c.Err
}

func (c *ConnFake) TxBegin(mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (db.Handle, error) {
	return c.TxBeginHandle, c.Err
}

func (c *ConnFake) TxRollback(tx db.Handle) error {
	return c.TxRollbackErr
}

func (c *ConnFake) TxCommit(tx db.Handle) error {
	return c.TxCommitErr
}

func (c *ConnFake) Run(cypher string, params map[string]interface{}, mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (*db.Stream, error) {
	return c.RunStream, c.Err
}

func (c *ConnFake) RunTx(tx db.Handle, cypher string, params map[string]interface{}) (*db.Stream, error) {
	return c.RunTxStream, c.Err
}

func (c *ConnFake) Next(streamHandle db.Handle) (*db.Record, *db.Summary, error) {
	return c.NextRecord, c.NextSummary, c.Err
}
