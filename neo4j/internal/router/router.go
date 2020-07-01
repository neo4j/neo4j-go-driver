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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/log"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

const missingWriterRetries = 100

type databaseRouter struct {
	dueUnix int64
	table   *connection.RoutingTable
}

// Thread safe
type Router struct {
	routerContext map[string]string
	pool          Pool
	dbRouters     map[string]*databaseRouter
	dbRoutersMut  sync.Mutex
	now           func() time.Time
	sleep         func(time.Duration)
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
		dbRouters:     make(map[string]*databaseRouter),
		now:           time.Now,
		sleep:         time.Sleep,
		log:           logger,
		logId:         fmt.Sprintf("router %d", id),
	}
	r.log.Infof(r.logId, "Created {context: %v}", routerContext)
	return r
}

func (r *Router) getTable(database string) (*connection.RoutingTable, error) {
	now := r.now()

	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()

	dbRouter := r.dbRouters[database]
	if dbRouter != nil && now.Unix() < dbRouter.dueUnix {
		return dbRouter.table, nil
	}

	var routers []string
	if dbRouter != nil {
		routers = dbRouter.table.Routers
	}
	if len(routers) == 0 {
		routers = []string{r.rootRouter}
	}

	r.log.Infof(r.logId, "Reading routing table for '%s' from any of %v", database, routers)
	table, err := readTable(context.Background(), r.pool, database, routers, r.routerContext)
	if err != nil {
		// Use hook to retrieve possibly different set of routers and retry
		if r.getRouters != nil {
			routers = r.getRouters()
			table, err = readTable(context.Background(), r.pool, database, routers, r.routerContext)
		}
		if err != nil {
			r.log.Error(r.logId, err)
			return nil, err
		}
	}
	r.dbRouters[database] = &databaseRouter{
		table:   table,
		dueUnix: now.Add(time.Duration(table.TimeToLive) * time.Second).Unix(),
	}
	r.log.Debugf(r.logId, "New routing table for '%s', TTL %d", database, table.TimeToLive)

	return table, nil
}

func (r *Router) Readers(database string) ([]string, error) {
	table, err := r.getTable(database)
	if err != nil {
		return nil, err
	}
	return table.Readers, nil
}

func (r *Router) Writers(database string) ([]string, error) {
	table, err := r.getTable(database)
	if err != nil {
		return nil, err
	}

	// During election we can get tables without any writers
	retries := missingWriterRetries
	for len(table.Writers) == 0 {
		retries--
		if retries == 0 {
			break
		}
		r.log.Debugf(r.logId, "Invalidating routing table, no writers")
		r.sleep(100 * time.Millisecond)
		r.Invalidate(database)
		table, err = r.getTable(database)
		if err != nil {
			return nil, err
		}
	}
	if len(table.Writers) == 0 {
		return nil, wrapInReadRoutingTableError(r.rootRouter, errors.New("No writers"))
	}

	return table.Writers, nil
}

func (r *Router) Context() map[string]string {
	return r.routerContext
}

func (r *Router) Invalidate(database string) {
	r.log.Infof(r.logId, "Invalidating routing table for '%s'", database)
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()
	// Reset due time to the 70s, this will make next access refresh the routing table using
	// last set of routers instead of the original one.
	dbRouter := r.dbRouters[database]
	if dbRouter != nil {
		dbRouter.dueUnix = 0
	}
}

func (r *Router) CleanUp() {
	r.log.Debugf(r.logId, "Cleaning up")
	now := r.now().Unix()
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()

	for dbName, dbRouter := range r.dbRouters {
		if now > dbRouter.dueUnix {
			delete(r.dbRouters, dbName)
		}
	}
}
