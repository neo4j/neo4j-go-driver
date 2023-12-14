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

// Package pool handles the database connection pool.
package pool

// Thread safe

import (
	"container/list"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

// DefaultConnectionLivenessCheckTimeout disables the liveness check of connections.
// Liveness checks are performed before a connection is deemed idle enough to be reset.
const DefaultConnectionLivenessCheckTimeout = math.MaxInt64

type Connect func(context.Context, string, *idb.ReAuthToken, bolt.ConnectionErrorListener, log.BoltLogger) (idb.Connection, error)

type poolRouter interface {
	InvalidateWriter(db string, server string)
	InvalidateReader(db string, server string)
	InvalidateServer(server string)
}

type qitem struct {
	wakeup chan bool
}

type Pool struct {
	config     *config.Config
	connect    Connect
	router     poolRouter
	servers    map[string]*server
	serversMut sync.Mutex
	queueMut   sync.Mutex
	queue      list.List
	closed     bool
	log        log.Logger
	logId      string
}

type serverPenalty struct {
	name    string
	penalty uint32
}

func New(config *config.Config, connect Connect, logger log.Logger, logId string) *Pool {
	// Means infinite life, simplifies checking later on

	p := &Pool{
		config:     config,
		connect:    connect,
		router:     nil,
		servers:    make(map[string]*server),
		serversMut: sync.Mutex{},
		queueMut:   sync.Mutex{},
		logId:      logId,
		log:        logger,
	}
	p.log.Infof(log.Pool, p.logId, "Created")
	return p
}

func (p *Pool) SetRouter(router poolRouter) {
	p.router = router
}

func (p *Pool) Close(ctx context.Context) {
	p.closed = true
	p.queueMut.Lock()
	for e := p.queue.Front(); e != nil; e = e.Next() {
		queuedRequest := e.Value.(*qitem)
		p.queue.Remove(e)
		queuedRequest.wakeup <- true
	}
	p.queueMut.Unlock()
	// Go through each server and close all connections to it
	p.serversMut.Lock()
	for n, s := range p.servers {
		s.closeAll(ctx)
		delete(p.servers, n)
	}
	p.serversMut.Unlock()
	p.log.Infof(log.Pool, p.logId, "Closed")
}

// For testing
func (p *Pool) queueSize() int {
	p.queueMut.Lock()
	defer p.queueMut.Unlock()
	return p.queue.Len()
}

// For testing
func (p *Pool) getServers() map[string]*server {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	servers := make(map[string]*server)
	for k, v := range p.servers {
		servers[k] = v
	}
	return servers
}

// CleanUp prunes all old connection on all the servers, this makes sure that servers
// gets removed from the map at some point in time. If there is a noticed
// failed connect still active  we should wait a while with removal to get
// prioritization right.
func (p *Pool) CleanUp(ctx context.Context) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	now := itime.Now()
	for n, s := range p.servers {
		s.removeIdleOlderThan(ctx, now, p.config.MaxConnectionLifetime)
		if s.size() == 0 && !s.hasFailedConnect(now) {
			delete(p.servers, n)
		}
	}
}

func (p *Pool) getPenaltiesForServers(ctx context.Context, serverNames []string) []serverPenalty {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	// Retrieve penalty for each server
	penalties := make([]serverPenalty, len(serverNames))
	now := itime.Now()
	for i, n := range serverNames {
		s := p.servers[n]
		penalties[i].name = n
		if s != nil {
			// Make sure that we don't get a too old connection
			s.removeIdleOlderThan(ctx, now, p.config.MaxConnectionLifetime)
			penalties[i].penalty = s.calculatePenalty(now)
		} else {
			penalties[i].penalty = newConnectionPenalty
		}
	}
	return penalties
}

func (p *Pool) anyHasCapacity(serverNames []string) bool {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	for _, serverName := range serverNames {
		srv := p.servers[serverName]
		if srv != nil {
			if srv.numIdle() > 0 || srv.size() < p.config.MaxConnectionPoolSize {
				return true
			}
		}
	}
	return false
}

func (p *Pool) Borrow(
	ctx context.Context,
	getServerNames func() []string,
	wait bool,
	boltLogger log.BoltLogger,
	idlenessTimeout time.Duration,
	auth *idb.ReAuthToken,
) (idb.Connection, error) {
	for {
		if p.closed {
			return nil, &errorutil.PoolClosed{}
		}
		serverNames := getServerNames()
		if len(serverNames) == 0 {
			return nil, &errorutil.PoolOutOfServers{}
		}
		p.log.Debugf(log.Pool, p.logId, "Trying to borrow connection from %s", serverNames)
		// Retrieve penalty for each server
		penalties := p.getPenaltiesForServers(ctx, serverNames)
		// Sort server penalties by lowest penalty
		sort.Slice(penalties, func(i, j int) bool {
			return penalties[i].penalty < penalties[j].penalty
		})

		var err error

		var conn idb.Connection
		for _, s := range penalties {
			conn, err = p.tryBorrow(ctx, s.name, boltLogger, idlenessTimeout, auth)
			if conn != nil {
				return conn, nil
			}

			if errorutil.IsTimeoutError(err) {
				p.log.Warnf(log.Pool, p.logId, "Borrow time-out")
				return nil, &errorutil.PoolTimeout{Servers: serverNames, Err: err}
			}
			if errorutil.IsFatalDuringDiscovery(err) {
				return nil, err
			}
		}

		if err != nil {
			// Intentionally return last error from last connection attempt to make it easier to
			// see connection errors for users.
			return nil, err
		}

		if !wait {
			return nil, &errorutil.PoolFull{Servers: serverNames}
		}

		// Wait for a matching connection to be returned from another thread.
		p.queueMut.Lock()
		// By owning the queue lock, we are guaranteed that every call to Return from now on, until we release the
		// lock, will notify us (or another waiter). To avoid starving this thread, we have to check once more whether
		// any call to Return between checking for capacity above and acquiring the lock happened. In that case, we
		// are no longer guaranteed to be notified, so we have to start over.
		if p.anyHasCapacity(serverNames) {
			p.queueMut.Unlock()
			continue
		}
		// Add a waiting request to the queue and unlock the queue to let other threads that return
		// their connections access the queue.
		q := &qitem{
			wakeup: make(chan bool, 1),
		}
		e := p.queue.PushBack(q)
		p.queueMut.Unlock()

		p.log.Warnf(log.Pool, p.logId, "Borrow queued")
		// Wait for either a wake-up signal that indicates that we got a connection or a timeout.
		select {
		case <-q.wakeup:
			continue
		case <-ctx.Done():
			p.queueMut.Lock()
			p.queue.Remove(e)
			if len(q.wakeup) == 1 {
				// We got notified, but are no longer interested.
				// Ask the next waiter.
				if e := p.queue.Front(); e != nil {
					queuedRequest := e.Value.(*qitem)
					p.queue.Remove(e)
					queuedRequest.wakeup <- true
				}
				p.queueMut.Unlock()
				continue
			}
			p.queueMut.Unlock()
			p.log.Warnf(log.Pool, p.logId, "Borrow time-out")
			return nil, &errorutil.PoolTimeout{Err: ctx.Err(), Servers: serverNames}
		}
	}
}

func (p *Pool) tryBorrow(
	ctx context.Context,
	serverName string,
	boltLogger log.BoltLogger,
	idlenessTimeout time.Duration,
	auth *idb.ReAuthToken,
) (idb.Connection, error) {
	p.serversMut.Lock()
	var unlock = new(sync.Once)
	defer unlock.Do(p.serversMut.Unlock)

	srv := p.servers[serverName]
	for {
		if srv != nil {
			srv.closing = false
			connection := srv.getIdle()
			if connection == nil {
				if srv.size() >= p.config.MaxConnectionPoolSize {
					return nil, nil
				}
				break
			}
			unlock.Do(p.serversMut.Unlock)
			healthy, err := srv.healthCheck(ctx, connection, idlenessTimeout, auth, boltLogger)
			if healthy {
				return connection, nil
			}
			p.unreg(ctx, serverName, connection, itime.Now())
			if err != nil {
				p.log.Debugf(log.Pool, p.logId, "Health check failed for %s: %s", serverName, err)
				return nil, err
			}
			p.serversMut.Lock()
			*unlock = sync.Once{}
			srv = p.servers[serverName]
		} else {
			// Make sure that there is a server in the map
			srv = NewServer()
			p.servers[serverName] = srv
			break
		}
	}

	srv.reservations++
	unlock.Do(p.serversMut.Unlock)

	// No idle connection, try to connect
	p.log.Infof(log.Pool, p.logId, "Connecting to %s", serverName)
	c, err := p.connect(ctx, serverName, auth, p, boltLogger)
	p.serversMut.Lock()
	*unlock = sync.Once{}
	srv.reservations--
	if err != nil {
		p.log.Warnf(log.Pool, p.logId, "Failed to connect to %s: %s", serverName, err)
		// FeatureNotSupportedError is not the server fault, don't penalize it
		if _, ok := err.(*db.FeatureNotSupportedError); !ok {
			srv.notifyFailedConnect(itime.Now())
		}
		return nil, err
	}

	// Ok, got a connection, register the connection
	srv.registerBusy(c)
	srv.notifySuccessfulConnect()
	return c, nil
}

func (p *Pool) unreg(ctx context.Context, serverName string, c idb.Connection, now time.Time) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	p.unregLocked(ctx, serverName, c, now)
}

func (p *Pool) unregLocked(ctx context.Context, serverName string, c idb.Connection, now time.Time) {
	defer func() {
		// Close connection in another thread to avoid potential long blocking operation during close.
		go c.Close(ctx)
	}()

	server := p.servers[serverName]
	// Check for strange condition of not finding the server.
	if server == nil {
		p.log.Warnf(log.Pool, p.logId, "Server %s not found", serverName)
		return
	}

	server.unregisterBusy(c)
	if server.size() == 0 && !server.hasFailedConnect(now) {
		delete(p.servers, serverName)
	}
}

func (p *Pool) removeIdleOlderThanOnServer(ctx context.Context, serverName string, now time.Time, maxAge time.Duration) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	server := p.servers[serverName]
	if server == nil {
		return
	}
	server.removeIdleOlderThan(ctx, now, maxAge)
}

func (p *Pool) Return(ctx context.Context, c idb.Connection) {
	if p.closed {
		p.log.Warnf(log.Pool, p.logId, "Trying to return connection to closed pool")
		return
	}

	// Get the name of the server that the connection belongs to.
	serverName := c.ServerName()
	isAlive := c.IsAlive()
	p.log.Debugf(log.Pool, p.logId, "Returning connection to %s {alive:%t}", serverName, isAlive)

	// If the connection is dead, remove all other idle connections on the same server that older
	// or of the same age as the dead connection, otherwise perform normal cleanup of old connections
	maxAge := p.config.MaxConnectionLifetime
	now := itime.Now()
	age := now.Sub(c.Birthdate())
	if !isAlive {
		// Since this connection has died all other connections that connected before this one
		// might also be bad, remove the idle ones.
		if age < maxAge {
			maxAge = age
		}
	}
	p.removeIdleOlderThanOnServer(ctx, serverName, now, maxAge)

	// Prepare connection for being used by someone else if is alive.
	// Since reset could find the connection to be in a bad state or non-recoverable state,
	// make sure again that it really is alive.
	if isAlive {
		c.Reset(ctx)
		isAlive = c.IsAlive()
	}

	c.SetBoltLogger(nil)

	// Shouldn't return a too old or dead connection back to the pool
	if !isAlive || age >= p.config.MaxConnectionLifetime {
		p.unreg(ctx, serverName, c, now)
		p.log.Infof(log.Pool, p.logId, "Unregistering dead or too old connection to %s", serverName)
	}

	if isAlive {
		// Just put it back in the list of idle connections for this server
		p.serversMut.Lock()
		server := p.servers[serverName]
		if server != nil { // Strange when server not found
			server.returnBusy(ctx, c)
			if server.closing && server.size() == 0 {
				delete(p.servers, serverName)
			}
		} else {
			p.log.Warnf(log.Pool, p.logId, "Server %s not found", serverName)
		}
		p.serversMut.Unlock()
	}

	// Check if there is anyone in the queue waiting for a connection to this server.
	p.queueMut.Lock()

	if e := p.queue.Front(); e != nil {
		queuedRequest := e.Value.(*qitem)
		p.queue.Remove(e)
		queuedRequest.wakeup <- true
	}
	p.queueMut.Unlock()
}

func (p *Pool) OnNeo4jError(ctx context.Context, connection idb.Connection, error *db.Neo4jError) error {
	if error.Code == "Neo.ClientError.Security.AuthorizationExpired" {
		serverName := connection.ServerName()
		p.serversMut.Lock()
		defer p.serversMut.Unlock()
		server := p.servers[serverName]
		server.executeForAllConnections(func(c idb.Connection) {
			c.ResetAuth()
		})
	}
	if error.Code == "Neo.TransientError.General.DatabaseUnavailable" {
		p.deactivate(ctx, connection.ServerName())
	}
	if error.IsRetriableCluster() {
		var database string
		if dbSelector, ok := connection.(idb.DatabaseSelector); ok {
			database = dbSelector.Database()
		}
		p.deactivateWriter(connection.ServerName(), database)
	}
	if error.HasSecurityCode() {
		manager, token := connection.GetCurrentAuth()
		if manager != nil {
			handled, err := manager.HandleSecurityException(ctx, token, error)
			if err != nil {
				return err
			}
			if handled {
				error.MarkRetriable()
			}
		}
	}

	return nil
}

func (p *Pool) OnIoError(ctx context.Context, connection idb.Connection, _ error) {
	p.deactivate(ctx, connection.ServerName())
}

func (p *Pool) OnDialError(ctx context.Context, serverName string, _ error) {
	p.deactivate(ctx, serverName)
}

func (p *Pool) deactivate(ctx context.Context, serverName string) {
	p.log.Debugf(log.Pool, p.logId, "Deactivating server %s", serverName)
	p.router.InvalidateServer(serverName)
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	server := p.servers[serverName]
	if server != nil {
		server.startClosing(ctx)
	}
}

func (p *Pool) deactivateWriter(serverName string, db string) {
	p.log.Debugf(log.Pool, p.logId, "Deactivating writer %s for database %s", serverName, db)
	p.router.InvalidateWriter(db, serverName)
}
