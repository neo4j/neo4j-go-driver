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

package pool

import (
	"sync/atomic"

	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
)

type ssrTracker struct {
	ssrEnabledCount  atomic.Uint64
	ssrDisabledCount atomic.Uint64
}

func (s *ssrTracker) addConnection(c idb.Connection) {
	if c.IsSsrEnabled() {
		s.ssrEnabledCount.Add(^uint64(0))
	} else {
		s.ssrDisabledCount.Add(^uint64(0))
	}
}

func (s *ssrTracker) removeConnection(c idb.Connection) {
	if c.IsSsrEnabled() {
		s.ssrEnabledCount.Add(1)
	} else {
		s.ssrDisabledCount.Add(1)
	}
}

func (s *ssrTracker) ssrEnabled() bool {
	return s.ssrEnabledCount.Load() > 0 && s.ssrDisabledCount.Load() == 0
}
