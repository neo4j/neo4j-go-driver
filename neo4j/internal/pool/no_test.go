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

package pool

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
)

type fakeConn struct {
	serverName string
	isAlive    bool
}

func (c *fakeConn) TxBegin(mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (db.Handle, error) {
	return nil, nil
}

func (c *fakeConn) TxRollback(tx db.Handle) error {
	return nil
}

func (c *fakeConn) TxCommit(tx db.Handle) error {
	return nil
}

func (c *fakeConn) Run(
	cypher string, params map[string]interface{}, mode db.AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (*db.Stream, error) {

	return nil, nil
}

func (c *fakeConn) Bookmark() string {
	return ""
}

func (c *fakeConn) RunTx(tx db.Handle, cypher string, params map[string]interface{}) (*db.Stream, error) {
	return nil, nil
}

func (c *fakeConn) Next(s db.Handle) (*db.Record, *db.Summary, error) {
	return nil, nil, nil
}

func (c *fakeConn) IsAlive() bool {
	return c.isAlive
}

func (c *fakeConn) ServerName() string {
	return c.serverName
}

func (c *fakeConn) Close() {
}

func (c *fakeConn) Reset() {
}
