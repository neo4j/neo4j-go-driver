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

package router

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type poolFake struct {
	borrow   func(names []string, cancel context.CancelFunc, logger log.BoltLogger) (db.Connection, error)
	returned []db.Connection
	cancel   context.CancelFunc
}

func (p *poolFake) Borrow(ctx context.Context, servers []string, wait bool, logger log.BoltLogger) (db.Connection, error) {
	return p.borrow(servers, p.cancel, logger)
}

func (p *poolFake) Return(c db.Connection) {
	p.returned = append(p.returned, c)
}
