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

package router

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"time"
)

type poolFake struct {
	borrow   func(names []string, cancel context.CancelFunc, logger log.BoltLogger) (db.Connection, error)
	returned []db.Connection
	cancel   context.CancelFunc
}

func (p *poolFake) Borrow(ctx context.Context, getServers func(context.Context) ([]string, error), _ bool, logger log.BoltLogger, _ time.Duration, _ *db.ReAuthToken) (db.Connection, error) {
	servers, err := getServers(ctx)
	if err != nil {
		return nil, err
	}
	return p.borrow(servers, p.cancel, logger)
}

func (p *poolFake) Return(_ context.Context, c db.Connection) error {
	p.returned = append(p.returned, c)
	return nil
}
