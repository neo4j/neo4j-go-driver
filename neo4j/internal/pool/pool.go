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

package pool

// Thread safe

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Connection interface {
	ServerName() string
	IsAlive() bool
	Reset()
	Close()
	Birthdate() time.Time
}

type Connect func(string) (Connection, error)

type qitem struct {
	servers []string
	wakeup  chan bool
	conn    Connection
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
}

func New(maxSize int, maxAge time.Duration, connect Connect) *Pool {
	p := &Pool{
		maxSize: maxSize,
		maxAge:  maxAge,
		connect: connect,
		servers: make(map[string]*server),
		now:     time.Now,
	}
	return p
}

func log(msg string) {
	//fmt.Printf("pool: %s\n", msg)
}

func (p *Pool) Close() {
	log("closing")
	// Cancel everything in the queue by just emptying at and let all callers timeout
	p.queueMut.Lock()
	p.queue.Init()
	p.queueMut.Unlock()
	// Go through each server and close all connections to it
	p.serversMut.Lock()
	for n, b := range p.servers {
		for c := b.get(); c != nil; c = b.get() {
			c.Close()
		}
		delete(p.servers, n)
	}
	p.serversMut.Unlock()
	log("closed!")
}

// Tries to find an unused connection on one of the servers we're already connected to.
func (p *Pool) tryExistingConnectionToKnownServer(serverNames []string) Connection {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	for _, s := range serverNames {
		b := p.servers[s]
		if b == nil {
			continue
		}
		for {
			c := b.get()
			if c == nil {
				break
			}
			// Check that the connection is ok
			log(fmt.Sprintf("%s: found connectio to use", s))
			return c
		}
	}

	return nil
}

func (p *Pool) tryNewConnectionToKnownServer(serverNames []string, checkTimeout func() bool, errSeed error) (Connection, error) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	var (
		err error = errSeed
		c   Connection
	)

	for _, s := range serverNames {
		if checkTimeout() {
			return nil, err
		}

		b := p.servers[s]
		if b == nil {
			continue
		}
		if b.size >= p.maxSize {
			continue
		}
		// Try to connect, this might take a while
		log(fmt.Sprintf("%s: connecting", s))
		c, err = p.connect(s)
		if err != nil || c == nil {
			continue
		}
		// Register the connection to the server
		log(fmt.Sprintf("%s: connection registered", s))
		b.reg(c)
		return c, nil
	}
	return nil, err
}

func (p *Pool) tryNewServer(serverNames []string, checkTimeout func() bool, errSeed error) (Connection, error) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	var (
		err error = errSeed
		c   Connection
	)

	for _, s := range serverNames {
		if checkTimeout() {
			return nil, err
		}

		b := p.servers[s]
		if b != nil {
			continue
		}

		// Try to connect and add this server as known
		log(fmt.Sprintf("%s: server pending, connecting", s))
		c, err = p.connect(s)
		if err != nil || c == nil {
			// Blacklist this server for a while ?
			log(fmt.Sprintf("%s: server cancelled", s))
			continue
		}
		// Ok, got a connection, track this server and the connection
		b = &server{}
		p.servers[s] = b
		log(fmt.Sprintf("%s: server added", s))
		b.reg(c)
		log(fmt.Sprintf("%s: conn registered", s))
		return c, nil
	}
	return nil, err
}

func (p *Pool) anyExistingConnections(serverNames []string) bool {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	for _, s := range serverNames {
		b := p.servers[s]
		if b != nil {
			if b.size > 0 {
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
func (p *Pool) getServers() map[string]server {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	servers := make(map[string]server)
	for k, v := range p.servers {
		servers[k] = *v
	}
	return servers
}

func (p *Pool) Borrow(ctx context.Context, servers []string) (Connection, error) {
	timedOut := false
	timeOut := func() bool {
		select {
		case <-ctx.Done():
			log("time out")
			timedOut = true
			return true
		default:
			return false
		}
	}

	log(fmt.Sprintf("Borrow %s", servers))
	var err error
	var c Connection

	// Prune all connection on all the servers, this makes sure that servers
	// gets removed from the map at some point in time (as long as someone
	// borrows new connections)
	p.serversMut.Lock()
	for n, s := range p.servers {
		s.prune(p.keepConnection)
		if s.size == 0 {
			delete(p.servers, n)
		}
	}
	p.serversMut.Unlock()

	// Try to use an existing connection to a known server, that is cheapest.
	c = p.tryExistingConnectionToKnownServer(servers)
	if c != nil {
		return c, nil
	}
	if timeOut() {
		return nil, ctx.Err()
	}

	// Prefer to have few connections to many servers over many connections to few servers.
	//  A non-functional server would have it's server pruned above but retried to connect to
	// here, could be good if the server went down and now is up again but could also be bad if
	// it is still down.
	c, err = p.tryNewServer(servers, timeOut, nil)
	if c != nil {
		return c, nil
	}
	if timedOut {
		return nil, ctx.Err()
	}

	// Try another connection to one of the known servers
	c, err = p.tryNewConnectionToKnownServer(servers, timeOut, err)
	if c != nil {
		return c, nil
	}
	if timedOut {
		return nil, ctx.Err()
	}

	// If there are no connections for any of the servers, there is no point in waiting for anything
	// to be returned.
	if !p.anyExistingConnections(servers) {
		if err == nil {
			err = errors.New("No conns to wait for")
		}
		return nil, err
	}

	// Wait for a matching connection to be returned from another thread.
	p.queueMut.Lock()
	// Ok, now that we own the queue we can add the item there but between getting the lock
	// and above check for an existing connection another thread might have returned a connection
	// so check again to avoid potentially starving this thread.
	c = p.tryExistingConnectionToKnownServer(servers)
	if c != nil {
		p.queueMut.Unlock()
		return c, nil
	}
	// Add a waiting request to the queue and unlock the queue to let other threads that returns
	// their connections access the queue.
	q := &qitem{
		servers: servers,
		wakeup:  make(chan bool),
	}
	e := p.queue.PushBack(q)
	p.queueMut.Unlock()

	log("in queue")

	// Wait for either a wake up signal that indicates that we got a connection or a timeout.
	select {
	case <-q.wakeup:
		log("woke up, got a conn")
		return q.conn, nil
	case <-ctx.Done():
		log("timed out, checking queue")
		p.queueMut.Lock()
		p.queue.Remove(e)
		p.queueMut.Unlock()
		if q.conn != nil {
			log("got a conn, recovering")
			return q.conn, nil
		}
		return nil, ctx.Err()
	}
}

func (p *Pool) unreg(serverName string, c Connection) {
	p.serversMut.Lock()
	defer p.serversMut.Unlock()

	server := p.servers[serverName]
	// Check for strange condition of not finding the server.
	if server != nil {
		server.unreg(c)
		if server.size == 0 {
			delete(p.servers, serverName)
		}
	}

	// Close connection in another thread to avoid potential long blocking operation during close.
	go func() {
		c.Close()
	}()
}

func (p *Pool) keepConnection(c Connection) bool {
	if !c.IsAlive() {
		return false
	}

	// Zero or less disables the check
	if p.maxAge > 0 {
		age := p.now().Sub(c.Birthdate())
		if age >= p.maxAge {
			return false
		}
	}

	return true
}

func (p *Pool) Return(c Connection) {
	// Get the name of the server that the connection belongs to.
	serverName := c.ServerName()
	log(fmt.Sprintf("Return conn in %s", serverName))

	if !p.keepConnection(c) {
		log(fmt.Sprintf("%s: throwing away connection", serverName))
		p.unreg(serverName, c)
		// This might cause someone waiting in the queue to wait in vain.
		return
	}

	// Prepare connection for being used by someone else
	c.Reset()

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

	// Just put it back in the server
	p.serversMut.Lock()
	defer p.serversMut.Unlock()
	server := p.servers[serverName]
	if server != nil { // Strange when server not found
		log(fmt.Sprintf("%s: back in server", serverName))
		server.ret(c)
	}
}
