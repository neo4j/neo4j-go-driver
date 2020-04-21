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
// is ready for use. The borrows connections are not kept.
// Not thread safe
type server struct {
	ready list.List
	size  int
}

// Returns a ready connection if any
func (s *server) get() Connection {
	e := s.ready.Front()
	if e != nil {
		c := s.ready.Remove(e)
		return c.(Connection)
	}
	return nil
}

func (s *server) ret(c Connection) {
	s.ready.PushFront(c)
}

func (s server) num() int {
	return s.ready.Len()
}

func (s *server) reg(c Connection) {
	s.size += 1
}

func (s *server) unreg(c Connection) {
	s.size -= 1
}

func (s *server) prune(keep func(c Connection) bool) {
	e := s.ready.Front()
	for e != nil {
		n := e.Next()
		c := e.Value.(Connection)
		if !keep(c) {
			s.ready.Remove(e)
			s.size -= 1
			go c.Close()
		}
		e = n
	}
}
