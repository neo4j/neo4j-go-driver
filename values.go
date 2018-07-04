/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

// Node represents a node in the neo4j graph database
type Node interface {
	Id() int64
	Labels() []string
	Props() map[string]interface{}
}

// Relationship represents a relationship in the neo4j graph database
type Relationship interface {
	Id() int64
	StartId() int64
	EndId() int64
	Type() string
	Props() map[string]interface{}
}
