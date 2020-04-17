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

// Not thread safe

import (
	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

// Represents a server with a number of connections.
type server struct {
	ready []conn.Connection
	size  int
}

func (s *server) get() conn.Connection {
	l := len(s.ready)
	if l == 0 {
		return nil
	}
	// Pop the last one, faster to just pop the end of the slice
	c := s.ready[l-1]
	s.ready = s.ready[:l-1]
	return c
}

func (s *server) ret(c conn.Connection) {
	s.ready = append(s.ready, c)
}

func (s server) num() int {
	return len(s.ready)
}

func (s *server) reg(c conn.Connection) {
	s.size += 1
}

func (s *server) unreg(c conn.Connection) {
	s.size -= 1
}
