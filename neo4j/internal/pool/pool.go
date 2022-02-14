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

// Package pool handles the database connection pool.
package pool

// Thread safe

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"sort"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type Connect func(context.Context, string, log.BoltLogger) (db.Connection, error)

type qitem struct {
	servers []string
	wakeup  chan bool
	conn    db.Connection
}

type Pool struct {
	maxSize    int
	maxAge     time.Duration
	connect    Connect
	servers    map[string]*server
	serversMut sync.Mutex
	queueMut   sync.Mutex
	queue      list.List
	now        func() time.Time
	closed     bool
	log        log.Logger
	logId      string
}

type serverPenalty struct {
	name    string
	penalty uint32
}

func New(maxSize int, maxAge time.Duration, connect Connect, logger log.Logger, logId string) *Pool {
	// Means infinite life, simplifies checking later on
	if maxAge <= 0 {
		maxAge = 1<<63 - 1
	}

	p := &Pool{
		maxSize: maxSize,
		maxAge:  maxAge,
		connect: connect,
		servers: make(map[string]*server),
		now:     time.Now,
		logId:   logId,
		log:     logger,
	}
	p.log.Infof(log.Pool, p.logId, "Created")
	return p
}

func (p *Pool) Close(ctx context.Context) {
	p.closed = true
	// Cancel everything in the queue by just emptying at and let all callers timeout
	p.queueMut.Lock()
	p.queue.Init()
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

func (p *Pool) anyExistingConnectionsOnServers(serverNames []string) bool {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	for _, s := range serverNames {
		b := p.servers[s]
		if b != nil {
			if b.size() > 0 {
				return true
			}
		}
	}
	return false
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

// Prune all old connection on all the servers, this makes sure that servers
// gets removed from the map at some point in time. If there is a noticed
// failed connect still active  we should wait a while with removal to get
// prioritization right.
func (p *Pool) CleanUp(ctx context.Context) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	now := p.now()
	for n, s := range p.servers {
		s.removeIdleOlderThan(ctx, now, p.maxAge)
		if s.size() == 0 && !s.hasFailedConnect(now) {
			delete(p.servers, n)
		}
	}
}

func (p *Pool) tryBorrow(ctx context.Context, serverName string, boltLogger log.BoltLogger) (db.Connection, error) {
	// For now, lock complete servers map to avoid over connecting but with the downside
	// that long connect times will block connects to other servers as well. To fix this
	// we would need to add a pending connect to the server and lock per server.
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	srv := p.servers[serverName]
	if srv != nil {
		// Try to get an existing idle connection
		if c := srv.getIdle(); c != nil {
			c.SetBoltLogger(boltLogger)
			return c, nil
		}
		if srv.size() >= p.maxSize {
			return nil, &PoolFull{servers: []string{serverName}}
		}
	} else {
		// Make sure that there is a server in the map
		srv = &server{}
		p.servers[serverName] = srv
	}

	// No idle connection, try to connect
	p.log.Infof(log.Pool, p.logId, "Connecting to %s", serverName)
	c, err := p.connect(ctx, serverName, boltLogger)
	if err != nil {
		// Failed to connect, keep track that it was bad for a while
		srv.notifyFailedConnect(p.now())
		p.log.Warnf(log.Pool, p.logId, "Failed to connect to %s: %s", serverName, err)
		return nil, err
	}

	// Ok, got a connection, register the connection
	srv.registerBusy(c)
	srv.notifySuccesfulConnect()
	return c, nil
}

func (p *Pool) getPenaltiesForServers(ctx context.Context, serverNames []string) []serverPenalty {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	// Retrieve penalty for each server
	penalties := make([]serverPenalty, len(serverNames))
	now := p.now()
	for i, n := range serverNames {
		s := p.servers[n]
		penalties[i].name = n
		if s != nil {
			// Make sure that we don't get a too old connection
			s.removeIdleOlderThan(ctx, now, p.maxAge)
			penalties[i].penalty = s.calculatePenalty(now)
		} else {
			penalties[i].penalty = newConnectionPenalty
		}
	}
	return penalties
}

func (p *Pool) tryAnyIdle(serverNames []string) db.Connection {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	for _, serverName := range serverNames {
		srv := p.servers[serverName]
		if srv != nil {
			// Try to get an existing idle connection
			conn := srv.getIdle()
			if conn != nil {
				return conn
			}
		}
	}
	return nil
}

// Borrow tries to borrow an existing database connection or tries to create a new one
// if none exists. The wait flag indicates if the caller wants to wait for a connection
// to be returned if there aren't any idle connection available.
func (p *Pool) Borrow(ctx context.Context, serverNames []string, wait bool, boltLogger log.BoltLogger) (db.Connection, error) {
	if p.closed {
		return nil, &PoolClosed{}
	}
	p.log.Debugf(log.Pool, p.logId, "Trying to borrow connection from %s", serverNames)

	// Retrieve penalty for each server
	penalties := p.getPenaltiesForServers(ctx, serverNames)
	// Sort server penalties by lowest penalty
	sort.Slice(penalties, func(i, j int) bool {
		return penalties[i].penalty < penalties[j].penalty
	})

	var err error
	var conn db.Connection
	for _, s := range penalties {
		conn, err = p.tryBorrow(ctx, s.name, boltLogger)
		if err == nil {
			return conn, nil
		}

		if bolt.IsTimeoutError(err) {
			p.log.Warnf(log.Pool, p.logId, "Borrow time-out")
			return nil, &PoolTimeout{servers: serverNames, err: err}
		}
	}

	// If there are no connections for any of the servers, there is no point in waiting for anything
	// to be returned.
	if !p.anyExistingConnectionsOnServers(serverNames) {
		p.log.Warnf(log.Pool, p.logId, "No server connection available to any of %v", serverNames)
		if err == nil {
			err = errors.New(fmt.Sprintf("No server connection available to any of %v", serverNames))
		}
		// Intentionally return last error from last connection attempt to make it easier to
		// see connection errors for users.
		return nil, err
	}

	if !wait {
		return nil, &PoolFull{servers: serverNames}
	}

	// Wait for a matching connection to be returned from another thread.
	p.queueMut.Lock()
	// Ok, now that we own the queue we can add the item there but between getting the lock
	// and above check for an existing connection another thread might have returned a connection
	// so check again to avoid potentially starving this thread.
	conn = p.tryAnyIdle(serverNames)
	if conn != nil {
		p.queueMut.Unlock()
		return conn, nil
	}
	// Add a waiting request to the queue and unlock the queue to let other threads that returns
	// their connections access the queue.
	q := &qitem{
		servers: serverNames,
		wakeup:  make(chan bool),
	}
	e := p.queue.PushBack(q)
	p.queueMut.Unlock()

	p.log.Warnf(log.Pool, p.logId, "Borrow queued")
	// Wait for either a wake-up signal that indicates that we got a connection or a timeout.
	select {
	case <-q.wakeup:
		return q.conn, nil
	case <-ctx.Done():
		p.queueMut.Lock()
		p.queue.Remove(e)
		p.queueMut.Unlock()
		if q.conn != nil {
			return q.conn, nil
		}
		p.log.Warnf(log.Pool, p.logId, "Borrow time-out")
		return nil, &PoolTimeout{err: ctx.Err(), servers: serverNames}
	}
}

func (p *Pool) unreg(ctx context.Context, serverName string, c db.Connection,
	now time.Time) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

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

func (p *Pool) Return(ctx context.Context, c db.Connection) {
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
	maxAge := p.maxAge
	now := p.now()
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
	if !isAlive || age >= p.maxAge {
		p.unreg(ctx, serverName, c, now)
		p.log.Infof(log.Pool, p.logId, "Unregistering dead or too old connection to %s", serverName)
		// Returning here could cause a waiting thread to wait until it times out, to do it
		// properly we could wake up threads that waits on the server and wake them up if there
		// are no more connections to wait for.
		return
	}

	// Check if there is anyone in the queue waiting for a connection to this server.
	p.queueMut.Lock()
	for e := p.queue.Front(); e != nil; e = e.Next() {
		qitem := e.Value.(*qitem)
		// Check requested servers
		for _, rserver := range qitem.servers {
			if rserver == serverName {
				qitem.conn = c
				p.queue.Remove(e)
				p.queueMut.Unlock()
				qitem.wakeup <- true
				return
			}
		}
	}
	p.queueMut.Unlock()

	// Just put it back in the list of idle connections for this server
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	server := p.servers[serverName]
	if server != nil { // Strange when server not found
		server.returnBusy(c)
	} else {
		p.log.Warnf(log.Pool, p.logId, "Server %s not found", serverName)
	}
}
