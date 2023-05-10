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

// Package pool handles the database connection pool.
package pool

// Thread safe

import (
	"container/list"
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

// DefaultLivenessCheckThreshold disables the liveness check of connections
// Liveness checks are performed before a connection is deemed idle enough to be reset
const DefaultLivenessCheckThreshold = math.MaxInt64

type Connect func(context.Context, string, *idb.ReAuthToken, bolt.Neo4jErrorCallback, log.BoltLogger) (idb.Connection, error)

type qitem struct {
	wakeup chan bool
}

type Pool struct {
	config     *config.Config
	connect    Connect
	servers    map[string]*server
	serversMut racing.Mutex
	queueMut   racing.Mutex
	queue      list.List
	now        *func() time.Time
	closed     bool
	log        log.Logger
	logId      string
}

type serverPenalty struct {
	name    string
	penalty uint32
}

func New(config *config.Config, connect Connect, logger log.Logger, logId string, now *func() time.Time) *Pool {
	// Means infinite life, simplifies checking later on

	p := &Pool{
		config:     config,
		connect:    connect,
		servers:    make(map[string]*server),
		serversMut: racing.NewMutex(),
		queueMut:   racing.NewMutex(),
		now:        now,
		logId:      logId,
		log:        logger,
	}
	p.log.Infof(log.Pool, p.logId, "Created")
	return p
}

func (p *Pool) Close(ctx context.Context) error {
	p.closed = true
	// Cancel everything in the queue by just emptying at and let all callers timeout
	if !p.queueMut.TryLock(ctx) {
		return racing.LockTimeoutError("could not acquire queue lock in time when closing pool")
	}
	p.queue.Init()
	p.queueMut.Unlock()
	// Go through each server and close all connections to it
	if !p.serversMut.TryLock(ctx) {
		return racing.LockTimeoutError("could not acquire server lock in time when closing pool")
	}
	for n, s := range p.servers {
		s.closeAll(ctx)
		delete(p.servers, n)
	}
	p.serversMut.Unlock()
	p.log.Infof(log.Pool, p.logId, "Closed")
	return nil
}

// For testing
func (p *Pool) queueSize(ctx context.Context) (int, error) {
	if !p.queueMut.TryLock(ctx) {
		return -1, fmt.Errorf("could not acquire queue lock in time when checking queue size")
	}
	defer p.queueMut.Unlock()
	return p.queue.Len(), nil
}

// For testing
func (p *Pool) getServers(ctx context.Context) (map[string]*server, error) {
	if !p.serversMut.TryLock(ctx) {
		return nil, fmt.Errorf("could not acquire server lock in time when getting servers")
	}
	defer p.serversMut.Unlock()
	servers := make(map[string]*server)
	for k, v := range p.servers {
		servers[k] = v
	}
	return servers, nil
}

// CleanUp prunes all old connection on all the servers, this makes sure that servers
// gets removed from the map at some point in time. If there is a noticed
// failed connect still active  we should wait a while with removal to get
// prioritization right.
func (p *Pool) CleanUp(ctx context.Context) error {
	if !p.serversMut.TryLock(ctx) {
		return fmt.Errorf("could not acquire server lock in time when cleaning up pool")
	}
	defer p.serversMut.Unlock()
	now := (*p.now)()
	for n, s := range p.servers {
		s.removeIdleOlderThan(ctx, now, p.config.MaxConnectionLifetime)
		if s.size() == 0 && !s.hasFailedConnect(now) {
			delete(p.servers, n)
		}
	}
	return nil
}

func (p *Pool) Now() time.Time {
	return (*p.now)()
}

func (p *Pool) getPenaltiesForServers(ctx context.Context, serverNames []string) ([]serverPenalty, error) {
	if !p.serversMut.TryLock(ctx) {
		return nil, fmt.Errorf("could not acquire server lock in time when computing server penalties")
	}
	defer p.serversMut.Unlock()

	// Retrieve penalty for each server
	penalties := make([]serverPenalty, len(serverNames))
	now := (*p.now)()
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
	return penalties, nil
}

func (p *Pool) tryAnyIdle(ctx context.Context, serverNames []string, idlenessThreshold time.Duration, auth *idb.ReAuthToken, logger log.BoltLogger) (idb.Connection, error) {
	if !p.serversMut.TryLock(ctx) {
		return nil, racing.LockTimeoutError("could not acquire server lock in time when getting idle connection")
	}
	var unlock = new(sync.Once)
	defer unlock.Do(p.serversMut.Unlock)
serverLoop:
	for _, serverName := range serverNames {
		for {
			srv := p.servers[serverName]
			if srv != nil {
				conn := srv.getIdle()
				if conn == nil {
					continue serverLoop
				}
				unlock.Do(p.serversMut.Unlock)
				healthy, err := srv.healthCheck(ctx, conn, idlenessThreshold, auth, logger)
				if healthy {
					return conn, nil
				}
				if err := p.unreg(context.Background(), serverName, conn, p.Now()); err != nil {
					panic("lock with Background context should never time out")
				}
				if err != nil {
					p.log.Debugf(log.Pool, p.logId, "Health check failed for %s: %s", serverName, err)
					return nil, err
				}
				if !p.serversMut.TryLock(ctx) {
					return nil, racing.LockTimeoutError("could not acquire lock in time when borrowing a connection")
				}
				*unlock = sync.Once{}
			}
		}
	}
	return nil, nil
}

func (p *Pool) Borrow(ctx context.Context, getServerNames func(context.Context) ([]string, error), wait bool, boltLogger log.BoltLogger, idlenessThreshold time.Duration, auth *idb.ReAuthToken) (idb.Connection, error) {
	if p.closed {
		return nil, &errorutil.PoolClosed{}
	}

	for {
		serverNames, err := getServerNames(ctx)
		if err != nil {
			return nil, err
		}
		if len(serverNames) == 0 {
			return nil, &errorutil.PoolOutOfServers{}
		}
		p.log.Debugf(log.Pool, p.logId, "Trying to borrow connection from %s", serverNames)
		// Retrieve penalty for each server
		penalties, err := p.getPenaltiesForServers(ctx, serverNames)
		if err != nil {
			return nil, err
		}
		// Sort server penalties by lowest penalty
		sort.Slice(penalties, func(i, j int) bool {
			return penalties[i].penalty < penalties[j].penalty
		})

		var conn idb.Connection
		for _, s := range penalties {
			conn, err = p.tryBorrow(ctx, s.name, boltLogger, idlenessThreshold, auth)
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
		if !p.queueMut.TryLock(ctx) {
			return nil, racing.LockTimeoutError("could not acquire lock in time when trying to get an idle connection")
		}
		// Ok, now that we own the queue we can add the item there but between getting the lock
		// and above check for an existing connection another thread might have returned a connection
		// so check again to avoid potentially starving this thread.
		conn, err = p.tryAnyIdle(ctx, serverNames, idlenessThreshold, auth, boltLogger)
		if err != nil {
			p.queueMut.Unlock()
			return nil, err
		}
		if conn != nil {
			p.queueMut.Unlock()
			return conn, nil
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
			if !p.queueMut.TryLock(context.Background()) {
				return nil, racing.LockTimeoutError("could not acquire lock in time when removing server wait request")
			}
			p.queue.Remove(e)
			p.queueMut.Unlock()
			p.log.Warnf(log.Pool, p.logId, "Borrow time-out")
			return nil, &errorutil.PoolTimeout{Err: ctx.Err(), Servers: serverNames}
		}
	}
}

func (p *Pool) tryBorrow(ctx context.Context, serverName string, boltLogger log.BoltLogger, idlenessThreshold time.Duration, auth *idb.ReAuthToken) (idb.Connection, error) {
	// For now, lock complete servers map to avoid over connecting but with the downside
	// that long connect times will block connects to other servers as well. To fix this
	// we would need to add a pending connect to the server and lock per server.
	if !p.serversMut.TryLock(ctx) {
		return nil, racing.LockTimeoutError("could not acquire lock in time when borrowing a connection")
	}
	var unlock = new(sync.Once)
	defer unlock.Do(p.serversMut.Unlock)

	srv := p.servers[serverName]
	for {
		if srv != nil {
			connection := srv.getIdle()
			if connection == nil {
				if srv.size() >= p.config.MaxConnectionPoolSize {
					return nil, nil
				}
				break
			}
			unlock.Do(p.serversMut.Unlock)
			healthy, err := srv.healthCheck(ctx, connection, idlenessThreshold, auth, boltLogger)
			if healthy {
				return connection, nil
			}
			if err := p.unreg(context.Background(), serverName, connection, p.Now()); err != nil {
				panic("lock with Background context should never time out")
			}
			if err != nil {
				p.log.Debugf(log.Pool, p.logId, "Health check failed for %s: %s", serverName, err)
				return nil, err
			}
			if !p.serversMut.TryLock(ctx) {
				return nil, racing.LockTimeoutError("could not acquire lock in time when borrowing a connection")
			}
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
	c, err := p.connect(ctx, serverName, auth, p.OnConnectionError, boltLogger)
	if !p.serversMut.TryLock(context.Background()) {
		panic("lock with Background context should never time out")
	}
	*unlock = sync.Once{}
	srv.reservations--
	if err != nil {
		// FeatureNotSupportedError is not the server fault, don't penalize it
		if _, ok := err.(*db.FeatureNotSupportedError); !ok {
			srv.notifyFailedConnect((*p.now)())
		}
		p.log.Warnf(log.Pool, p.logId, "Failed to connect to %s: %s", serverName, err)
		return nil, err
	}

	// Ok, got a connection, register the connection
	srv.registerBusy(c)
	srv.notifySuccessfulConnect()
	return c, nil
}

func (p *Pool) unreg(ctx context.Context, serverName string, c idb.Connection, now time.Time) error {
	if !p.serversMut.TryLock(ctx) {
		return racing.LockTimeoutError("could not acquire server lock in time when unregistering server")
	}
	defer p.serversMut.Unlock()

	return p.unregUnlocked(ctx, serverName, c, now)
}

func (p *Pool) unregUnlocked(ctx context.Context, serverName string, c idb.Connection, now time.Time) error {
	defer func() {
		// Close connection in another thread to avoid potential long blocking operation during close.
		go c.Close(ctx)
	}()

	server := p.servers[serverName]
	// Check for strange condition of not finding the server.
	if server == nil {
		p.log.Warnf(log.Pool, p.logId, "Server %s not found", serverName)
		return nil
	}

	server.unregisterBusy(c)
	if server.size() == 0 && !server.hasFailedConnect(now) {
		delete(p.servers, serverName)
	}
	return nil
}

func (p *Pool) removeIdleOlderThanOnServer(ctx context.Context, serverName string, now time.Time, maxAge time.Duration) error {
	if !p.serversMut.TryLock(ctx) {
		return racing.LockTimeoutError("could not acquire server lock in time before removing old idle connections")
	}
	defer p.serversMut.Unlock()
	server := p.servers[serverName]
	if server == nil {
		return nil
	}
	server.removeIdleOlderThan(ctx, now, maxAge)
	return nil
}

func (p *Pool) Return(ctx context.Context, c idb.Connection) error {
	if p.closed {
		p.log.Warnf(log.Pool, p.logId, "Trying to return connection to closed pool")
		return nil
	}

	// Get the name of the server that the connection belongs to.
	serverName := c.ServerName()
	isAlive := c.IsAlive()
	p.log.Debugf(log.Pool, p.logId, "Returning connection to %s {alive:%t}", serverName, isAlive)

	// If the connection is dead, remove all other idle connections on the same server that older
	// or of the same age as the dead connection, otherwise perform normal cleanup of old connections
	maxAge := p.config.MaxConnectionLifetime
	now := (*p.now)()
	age := now.Sub(c.Birthdate())
	if !isAlive {
		// Since this connection has died all other connections that connected before this one
		// might also be bad, remove the idle ones.
		if age < maxAge {
			maxAge = age
		}
	}
	if err := p.removeIdleOlderThanOnServer(ctx, serverName, now, maxAge); err != nil {
		return err
	}

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
		if err := p.unreg(ctx, serverName, c, now); err != nil {
			return err
		}
		p.log.Infof(log.Pool, p.logId, "Unregistering dead or too old connection to %s", serverName)
	}

	if isAlive {
		// Just put it back in the list of idle connections for this server
		if !p.serversMut.TryLock(ctx) {
			return racing.LockTimeoutError("could not acquire server lock when putting connection back to idle")
		}
		server := p.servers[serverName]
		if server != nil { // Strange when server not found
			server.returnBusy(c)
		} else {
			p.log.Warnf(log.Pool, p.logId, "Server %s not found", serverName)
		}
		p.serversMut.Unlock()
	}

	// Check if there is anyone in the queue waiting for a connection to this server.
	if !p.queueMut.TryLock(ctx) {
		return racing.LockTimeoutError("could not acquire queue lock when checking connection requests")
	}
	for e := p.queue.Front(); e != nil; e = e.Next() {
		queuedRequest := e.Value.(*qitem)
		p.queue.Remove(e)
		queuedRequest.wakeup <- true
	}
	p.queueMut.Unlock()

	return nil
}

func (p *Pool) OnConnectionError(ctx context.Context, connection idb.Connection, error *db.Neo4jError) error {
	if error.Code == "Neo.ClientError.Security.AuthorizationExpired" {
		serverName := connection.ServerName()
		if !p.serversMut.TryLock(ctx) {
			return racing.LockTimeoutError(fmt.Sprintf(
				"could not acquire server lock in time before marking all connection to %s for re-authentication",
				serverName))
		}
		defer p.serversMut.Unlock()
		server := p.servers[serverName]
		server.executeForAllConnections(func(c idb.Connection) {
			c.ResetAuth()
		})
	} else if error.Code == "Neo.ClientError.Security.TokenExpired" {
		manager, token := connection.GetCurrentAuth()
		if manager != nil {
			if err := manager.OnTokenExpired(ctx, token); err != nil {
				return err
			}
			if _, isStaticToken := manager.(auth.Token); isStaticToken {
				return nil
			} else {
				error.MarkRetriable()
			}
		}
	}
	return nil
}
