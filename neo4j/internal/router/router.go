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
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

// Thread safe
type Router struct {
	routerContext map[string]string
	pool          Pool
	table         *db.RoutingTable
	dueUnix       int64
	tableMut      sync.Mutex
	now           func() time.Time
	rootRouter    string
	getRouters    func() []string
}

type Pool interface {
	Borrow(ctx context.Context, servers []string, wait bool) (pool.Connection, error)
	Return(c pool.Connection)
}

func New(rootRouter string, getRouters func() []string, routerContext map[string]string, pool Pool) *Router {
	return &Router{
		rootRouter:    rootRouter,
		getRouters:    getRouters,
		routerContext: routerContext,
		pool:          pool,
		now:           time.Now,
	}
}

func (r *Router) getTable() (*db.RoutingTable, error) {
	r.tableMut.Lock()
	defer r.tableMut.Unlock()

	now := r.now()

	if r.table != nil && now.Unix() < r.dueUnix {
		return r.table, nil
	}

	var routers []string
	if r.table != nil {
		routers = r.table.Routers
	}
	if len(routers) == 0 {
		routers = []string{r.rootRouter}
	}

	table, err := readTable(context.Background(), r.pool, routers, r.routerContext)
	if err != nil {
		// Use hook to retrieve possibly different set of routers and retry
		if r.getRouters != nil {
			routers = r.getRouters()
			table, err = readTable(context.Background(), r.pool, routers, r.routerContext)
		}
		if err != nil {
			return nil, err
		}
	}
	r.table = table
	r.dueUnix = now.Add(time.Duration(table.TimeToLive) * time.Second).Unix()

	return table, nil
}

func (r *Router) Readers() ([]string, error) {
	table, err := r.getTable()
	if err != nil {
		return nil, err
	}
	return table.Readers, nil
}

func (r *Router) Writers() ([]string, error) {
	table, err := r.getTable()
	if err != nil {
		return nil, err
	}
	return table.Writers, nil
}
