/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testutil

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"time"
)

type PoolFake struct {
	BorrowConn  db.Connection
	BorrowErr   error
	ReturnHook  func()
	CleanUpHook func()
	BorrowHook  func() (db.Connection, error)
}

func (p *PoolFake) Borrow(ctx context.Context, getServerNames func(context.Context) ([]string, error), _ bool, _ log.BoltLogger, _ time.Duration, _ *db.ReAuthToken) (db.Connection, error) {
	if p.BorrowHook != nil && (p.BorrowConn != nil || p.BorrowErr != nil) {
		panic("either use the hook or the desired return values, but not both")
	}
	if p.BorrowHook != nil {
		return p.BorrowHook()
	}
	_, err := getServerNames(ctx)
	if err != nil {
		return nil, err
	}
	return p.BorrowConn, p.BorrowErr
}

func (p *PoolFake) Return(context.Context, db.Connection) {
	if p.ReturnHook != nil {
		p.ReturnHook()
	}
}

func (p *PoolFake) CleanUp(context.Context) {
	if p.CleanUpHook != nil {
		p.CleanUpHook()
	}
}

func (p *PoolFake) Now() time.Time {
	return time.Now()
}
