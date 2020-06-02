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

package router

import (
	"context"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	poolpackage "github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

type poolFake struct {
	borrow   func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error)
	returned []poolpackage.Connection
	cancel   context.CancelFunc
}

func (p *poolFake) Borrow(ctx context.Context, servers []string, wait bool) (poolpackage.Connection, error) {
	return p.borrow(servers, p.cancel)
}

func (p *poolFake) Return(c poolpackage.Connection) {
	p.returned = append(p.returned, c)
}

type connFake struct {
	name  string
	table *db.RoutingTable
	err   error
}

func (c *connFake) ServerName() string {
	return c.name
}

func (c *connFake) IsAlive() bool {
	return true
}

func (c *connFake) Reset() {
}

func (c *connFake) Close() {
}

func (c *connFake) Birthdate() time.Time {
	return time.Now()
}

func (c *connFake) GetRoutingTable(database string, context map[string]string) (*db.RoutingTable, error) {
	return c.table, c.err
}

type connFakeNoDiscovery struct {
	name string
}

func (c *connFakeNoDiscovery) ServerName() string {
	return c.name
}

func (c *connFakeNoDiscovery) IsAlive() bool {
	return true
}

func (c *connFakeNoDiscovery) Reset() {
}

func (c *connFakeNoDiscovery) Close() {
}

func (c *connFakeNoDiscovery) Birthdate() time.Time {
	return time.Now()
}
