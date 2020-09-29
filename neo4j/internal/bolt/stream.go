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
package bolt

import (
	"container/list"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type stream struct {
	keys []string
	fifo list.List
	sum  *db.Summary
	err  error
}

// Acts on buffered data, first return value indicates if buffering
// is active or not.
func (s *stream) bufferedNext() (bool, *db.Record, *db.Summary, error) {
	e := s.fifo.Front()
	if e != nil {
		s.fifo.Remove(e)
		return true, e.Value.(*db.Record), nil, nil
	}
	if s.err != nil {
		return true, nil, nil, s.err
	}
	if s.sum != nil {
		return true, nil, s.sum, nil
	}

	return false, nil, nil, nil
}

func (s *stream) push(rec *db.Record) {
	s.fifo.PushBack(rec)
}
