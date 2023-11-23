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

package errorutil

import "fmt"

type PoolTimeout struct {
	Err     error
	Servers []string
}

func (e *PoolTimeout) Error() string {
	return fmt.Sprintf("Timeout while waiting for connection to any of [%s]: %s", e.Servers, e.Err)
}

type PoolFull struct {
	Servers []string
}

func (e *PoolFull) Error() string {
	return fmt.Sprintf("No idle connections on any of [%s]", e.Servers)
}

type PoolClosed struct {
}

func (e *PoolClosed) Error() string {
	return "Pool closed"
}

type PoolOutOfServers struct {
}

func (e *PoolOutOfServers) Error() string {
	return "Pool could not find any servers to connect to"
}
