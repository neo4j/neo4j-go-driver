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

import (
	"container/list"
)

// Represents a server with a number of connections that either is in use (borrowed) or
// is ready for use.
// Not thread safe
type server struct {
	idle list.List
	busy list.List
}

// Returns a idle connection if any
func (s *server) getIdle() Connection {
	// Remove from idle list and add to busy list
	e := s.idle.Front()
	if e != nil {
		c := s.idle.Remove(e)
		s.busy.PushFront(c)
		return c.(Connection)
	}
	return nil
}

// Returns a busy connection, makes it idle
func (s *server) retBusy(c Connection) {
	s.unregBusy(c)
	s.idle.PushFront(c)
}

// Number of idle connections
func (s server) numIdle() int {
	return s.idle.Len()
}

// Adds a connection to busy list
func (s *server) regBusy(c Connection) {
	s.busy.PushFront(c)
}

func (s *server) unregBusy(c Connection) {
	found := false
	for e := s.busy.Front(); e != nil && !found; e = e.Next() {
		x := e.Value.(Connection)
		found = x == c
		if found {
			s.busy.Remove(e)
		}
	}
}

func (s *server) size() int {
	return s.busy.Len() + s.idle.Len()
}

// Removes old and dead connections from idle list
func (s *server) prune(keep func(c Connection) bool) {
	e := s.idle.Front()
	for e != nil {
		n := e.Next()
		c := e.Value.(Connection)
		if !keep(c) {
			s.idle.Remove(e)
			go c.Close()
		}
		e = n
	}
}

func closeAndEmptyConnections(l list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		c := e.Value.(Connection)
		c.Close()
	}
	l.Init()
}

func (s *server) closeAll() {
	closeAndEmptyConnections(s.idle)
	// Closing the busy connections could mean here that we do close from another thread.
	closeAndEmptyConnections(s.busy)
}
