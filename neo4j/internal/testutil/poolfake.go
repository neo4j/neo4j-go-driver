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
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type PoolFake struct {
	BorrowConn  db.Connection
	BorrowErr   error
	ReturnHook  func()
	CleanUpHook func()
}

func (p *PoolFake) Borrow(ctx context.Context, serverNames []string, wait bool, _ log.BoltLogger) (db.Connection, error) {
	return p.BorrowConn, p.BorrowErr
}

func (p *PoolFake) Return(ctx context.Context, c db.Connection) {
	if p.ReturnHook != nil {
		p.ReturnHook()
	}
}

func (p *PoolFake) CleanUp(ctx context.Context) {
	if p.CleanUpHook != nil {
		p.CleanUpHook()
	}
}
