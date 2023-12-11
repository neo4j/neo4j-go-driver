/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	"sync"
	"time"

	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

const missingWriterRetries = 100
const missingReaderRetries = 100

type databaseRouter struct {
	dueUnix int64
	table   *idb.RoutingTable
}

// Router is thread safe
type Router struct {
	routerContext   map[string]string
	pool            Pool
	idlenessTimeout time.Duration
	dbRouters       map[string]*databaseRouter
	updating        map[string][]chan struct{}
	dbRoutersMut    sync.Mutex
	sleep           func(time.Duration)
	rootRouter      string
	getRouters      func() []string
	log             log.Logger
	logId           string
}

type Pool interface {
	// Borrow acquires a connection from the provided list of servers
	// If all connections are busy and the pool is full, calls to Borrow may wait for a connection to become idle
	// If a connection has been idle for longer than idlenessTimeout, it will be reset
	// to check if it's still alive.
	Borrow(ctx context.Context, getServers func() []string, wait bool, boltLogger log.BoltLogger, idlenessTimeout time.Duration, auth *idb.ReAuthToken) (idb.Connection, error)
	Return(ctx context.Context, c idb.Connection)
}

func New(rootRouter string, getRouters func() []string, routerContext map[string]string, pool Pool, idlenessTimeout time.Duration, logger log.Logger, logId string) *Router {
	r := &Router{
		rootRouter:      rootRouter,
		getRouters:      getRouters,
		routerContext:   routerContext,
		pool:            pool,
		idlenessTimeout: idlenessTimeout,
		dbRouters:       make(map[string]*databaseRouter),
		updating:        make(map[string][]chan struct{}),
		dbRoutersMut:    sync.Mutex{},
		sleep:           time.Sleep,
		log:             logger,
		logId:           logId,
	}
	r.log.Infof(log.Router, r.logId, "Created {context: %v}", routerContext)
	return r
}

func (r *Router) readTable(
	ctx context.Context,
	dbRouter *databaseRouter,
	bookmarks []string,
	database,
	impersonatedUser string,
	auth *idb.ReAuthToken,
	boltLogger log.BoltLogger,
) (*idb.RoutingTable, error) {
	var (
		table *idb.RoutingTable
		err   error
	)

	// Try last known set of routers if there are any
	if dbRouter != nil && len(dbRouter.table.Routers) > 0 {
		routers := dbRouter.table.Routers
		r.log.Infof(log.Router, r.logId, "Reading routing table for '%s' from previously known routers: %v", database, routers)
		table, err = readTable(ctx, r.pool, routers, r.routerContext, r.idlenessTimeout, bookmarks, database, impersonatedUser, auth, boltLogger)
	}
	if errorutil.IsFatalDuringDiscovery(err) {
		r.log.Error(log.Router, r.logId, err)
		return nil, err
	}

	// Try initial router if no routers or failed
	if table == nil {
		r.log.Infof(log.Router, r.logId, "Reading routing table from initial router: %s", r.rootRouter)
		table, err = readTable(ctx, r.pool, []string{r.rootRouter}, r.routerContext, r.idlenessTimeout, bookmarks, database, impersonatedUser, auth, boltLogger)
	}
	if errorutil.IsFatalDuringDiscovery(err) {
		r.log.Error(log.Router, r.logId, err)
		return nil, err
	}

	// Use hook to retrieve possibly different set of routers and retry
	if table == nil && r.getRouters != nil {
		routers := r.getRouters()
		r.log.Infof(log.Router, r.logId, "Reading routing table for '%s' from custom routers: %v", routers)
		table, err = readTable(ctx, r.pool, routers, r.routerContext, r.idlenessTimeout, bookmarks, database, impersonatedUser, auth, boltLogger)
	}
	if errorutil.IsFatalDuringDiscovery(err) {
		r.log.Error(log.Router, r.logId, err)
		return nil, err
	}

	if err != nil {
		r.log.Error(log.Router, r.logId, err)
		return nil, err
	}

	if table == nil {
		// Safeguard for logical error somewhere else
		err = errors.New("no error and no table")
		r.log.Error(log.Router, r.logId, err)
		return nil, err
	}
	return table, nil
}

func (r *Router) getTable(database string) *idb.RoutingTable {
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()

	dbRouter := r.dbRouters[database]
	return r.getTableLocked(dbRouter)
}

func (r *Router) getOrUpdateTable(ctx context.Context, bookmarksFn func(context.Context) ([]string, error), database string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) (*idb.RoutingTable, error) {
	r.dbRoutersMut.Lock()
	var unlock = new(sync.Once)
	defer unlock.Do(r.dbRoutersMut.Unlock)
	for {
		dbRouter := r.dbRouters[database]
		if table := r.getTableLocked(dbRouter); table != nil {
			return table, nil
		}
		waiters, ok := r.updating[database]
		if ok {
			// Wait for the table to be updated by other goroutine
			ch := make(chan struct{})
			r.updating[database] = append(waiters, ch)
			unlock.Do(r.dbRoutersMut.Unlock)
			select {
			case <-ctx.Done():
				return nil, racing.LockTimeoutError("timed out waiting for other goroutine to update routing table")
			case <-ch:
				r.dbRoutersMut.Lock()
				*unlock = sync.Once{}
				continue
			}
		}
		// this goroutine will update the table
		r.updating[database] = make([]chan struct{}, 0)
		unlock.Do(r.dbRoutersMut.Unlock)

		table, err := r.updateTable(ctx, bookmarksFn, database, auth, boltLogger, dbRouter)
		r.dbRoutersMut.Lock()
		*unlock = sync.Once{}
		// notify all waiters
		for _, waiter := range r.updating[database] {
			close(waiter)
		}
		delete(r.updating, database)
		return table, err
	}
}

func (r *Router) getTableLocked(dbRouter *databaseRouter) *idb.RoutingTable {
	now := itime.Now()
	if dbRouter != nil && now.Unix() < dbRouter.dueUnix {
		return dbRouter.table
	}
	return nil
}

func (r *Router) updateTable(ctx context.Context, bookmarksFn func(context.Context) ([]string, error), database string, auth *idb.ReAuthToken, boltLogger log.BoltLogger, dbRouter *databaseRouter) (*idb.RoutingTable, error) {
	bookmarks, err := bookmarksFn(ctx)
	if err != nil {
		return nil, err
	}
	table, err := r.readTable(ctx, dbRouter, bookmarks, database, "", auth, boltLogger)
	if err != nil {
		return nil, err
	}

	err = r.storeRoutingTable(ctx, database, table, itime.Now())
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (r *Router) GetOrUpdateReaders(ctx context.Context, bookmarks func(context.Context) ([]string, error), database string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) ([]string, error) {
	table, err := r.getOrUpdateTable(ctx, bookmarks, database, auth, boltLogger)
	if err != nil {
		return nil, err
	}

	// During startup, we can get tables without any readers
	retries := missingReaderRetries
	for len(table.Readers) == 0 {
		retries--
		if retries == 0 {
			break
		}
		r.log.Infof(log.Router, r.logId, "Invalidating routing table, no readers")
		r.Invalidate(table.DatabaseName)
		r.sleep(100 * time.Millisecond)
		table, err = r.getOrUpdateTable(ctx, bookmarks, database, auth, boltLogger)
		if err != nil {
			return nil, err
		}
	}
	if len(table.Readers) == 0 {
		return nil, wrapError(r.rootRouter, errors.New("no readers"))
	}

	return table.Readers, nil
}

func (r *Router) Readers(database string) []string {
	table := r.getTable(database)
	if table == nil {
		return nil
	}
	return table.Readers
}

func (r *Router) GetOrUpdateWriters(ctx context.Context, bookmarks func(context.Context) ([]string, error), database string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) ([]string, error) {
	table, err := r.getOrUpdateTable(ctx, bookmarks, database, auth, boltLogger)
	if err != nil {
		return nil, err
	}

	// During election, we can get tables without any writers
	retries := missingWriterRetries
	for len(table.Writers) == 0 {
		retries--
		if retries == 0 {
			break
		}
		r.log.Infof(log.Router, r.logId, "Invalidating routing table, no writers")
		r.Invalidate(database)
		r.sleep(100 * time.Millisecond)
		table, err = r.getOrUpdateTable(ctx, bookmarks, database, auth, boltLogger)
		if err != nil {
			return nil, err
		}
	}
	if len(table.Writers) == 0 {
		return nil, wrapError(r.rootRouter, errors.New("no writers"))
	}

	return table.Writers, nil
}

func (r *Router) Writers(database string) []string {
	table := r.getTable(database)
	if table == nil {
		return nil
	}
	return table.Writers
}

func (r *Router) GetNameOfDefaultDatabase(ctx context.Context, bookmarks []string, user string, auth *idb.ReAuthToken, boltLogger log.BoltLogger) (string, error) {
	// FIXME: this seems to indirectly cache the home db for the routing table's TTL
	table, err := r.readTable(ctx, nil, bookmarks, idb.DefaultDatabase, user, auth, boltLogger)
	if err != nil {
		return "", err
	}
	// Store the fresh routing table as well to avoid another roundtrip to receive servers from session.
	now := itime.Now()
	err = r.storeRoutingTable(ctx, table.DatabaseName, table, now)
	if err != nil {
		return "", err
	}
	return table.DatabaseName, err
}

func (r *Router) Context() map[string]string {
	return r.routerContext
}

func (r *Router) Invalidate(database string) {
	r.log.Infof(log.Router, r.logId, "Invalidating routing table for '%s'", database)
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()
	// Reset due time to the 70s, this will make next access refresh the routing table using
	// last set of routers instead of the original one.
	dbRouter := r.dbRouters[database]
	if dbRouter != nil {
		dbRouter.dueUnix = 0
	}
}

func (r *Router) InvalidateWriter(db string, server string) {
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()

	router := r.dbRouters[db]
	if router == nil {
		return
	}
	router.table.Writers = removeServerFromList(router.table.Writers, server)
}

func (r *Router) InvalidateReader(db string, server string) {
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()

	router := r.dbRouters[db]
	if router == nil {
		return
	}
	router.table.Readers = removeServerFromList(router.table.Readers, server)
}

func (r *Router) InvalidateServer(server string) {
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()
	for _, routing := range r.dbRouters {
		routing.table.Routers = removeServerFromList(routing.table.Routers, server)
		routing.table.Readers = removeServerFromList(routing.table.Readers, server)
		routing.table.Writers = removeServerFromList(routing.table.Writers, server)
	}
}

func removeServerFromList(list []string, server string) []string {
	for i, s := range list {
		if s == server {
			return append(list[0:i], list[i+1:]...)
		}
	}
	return list
}

func (r *Router) CleanUp() {
	r.log.Debugf(log.Router, r.logId, "Cleaning up")
	now := itime.Now().Unix()
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()

	for dbName, dbRouter := range r.dbRouters {
		if now > dbRouter.dueUnix {
			delete(r.dbRouters, dbName)
		}
	}
}

func (r *Router) storeRoutingTable(ctx context.Context, database string, table *idb.RoutingTable, now time.Time) error {
	r.dbRoutersMut.Lock()
	defer r.dbRoutersMut.Unlock()
	r.dbRouters[database] = &databaseRouter{
		table:   table,
		dueUnix: now.Add(time.Duration(table.TimeToLive) * time.Second).Unix(),
	}
	r.log.Debugf(log.Router, r.logId, "New routing table for '%s', TTL %d", database, table.TimeToLive)
	return nil
}

func wrapError(server string, err error) error {
	// Preserve error originating from the database, wrap other errors
	_, isNeo4jErr := err.(*db.Neo4jError)
	if isNeo4jErr {
		return err
	}
	return &errorutil.ReadRoutingTableError{Server: server, Err: err}
}
