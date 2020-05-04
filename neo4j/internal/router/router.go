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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/log"
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
	log           log.Logger
	logId         string
}

type Pool interface {
	Borrow(ctx context.Context, servers []string, wait bool) (pool.Connection, error)
	Return(c pool.Connection)
}

var routerid uint32

func New(rootRouter string, getRouters func() []string, routerContext map[string]string, pool Pool, logger log.Logger) *Router {
	id := atomic.AddUint32(&routerid, 1)
	r := &Router{
		rootRouter:    rootRouter,
		getRouters:    getRouters,
		routerContext: routerContext,
		pool:          pool,
		now:           time.Now,
		log:           logger,
		logId:         fmt.Sprintf("router %d", id),
	}
	r.log.Infof(r.logId, "Created {context: %v}", routerContext)
	return r
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

	r.log.Infof(r.logId, "Reading routing table from any of %v", routers)
	table, err := readTable(context.Background(), r.pool, routers, r.routerContext)
	if err != nil {
		// Use hook to retrieve possibly different set of routers and retry
		if r.getRouters != nil {
			routers = r.getRouters()
			table, err = readTable(context.Background(), r.pool, routers, r.routerContext)
		}
		if err != nil {
			r.log.Error(r.logId, err)
			return nil, err
		}
	}
	r.table = table
	r.dueUnix = now.Add(time.Duration(table.TimeToLive) * time.Second).Unix()
	r.log.Debugf(r.logId, "New routing table, TTL %d", table.TimeToLive)

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

func (r *Router) Context() map[string]string {
	return r.routerContext
}

func (r *Router) Invalidate() {
	r.tableMut.Lock()
	defer r.tableMut.Unlock()
	// Reset due time to the 70s, this will make next access refresh the routing table using
	// last set of routers instead of the original one.
	r.dueUnix = 0
	r.log.Infof(r.logId, "Invalidating routing table")
}
