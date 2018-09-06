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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// Counters contains statistics about the changes made to the database made as part
// of the statement execution.
type Counters interface {
	NodesCreated() int
	NodesDeleted() int
	RelationshipsCreated() int
	RelationshipsDeleted() int
	PropertiesSet() int
	LabelsAdded() int
	LabelsRemoved() int
	IndexesAdded() int
	IndexesRemoved() int
	ConstraintsAdded() int
	ConstraintsRemoved() int
}

type neoCounters struct {
	nodesCreated         int
	nodesDeleted         int
	relationshipsCreated int
	relationshipsDeleted int
	propertiesSet        int
	labelsAdded          int
	labelsRemoved        int
	indexesAdded         int
	indexesRemoved       int
	constraintsAdded     int
	constraintsRemoved   int
}

func (counters *neoCounters) NodesCreated() int {
	return counters.nodesCreated
}

func (counters *neoCounters) NodesDeleted() int {
	return counters.nodesDeleted
}

func (counters *neoCounters) RelationshipsCreated() int {
	return counters.relationshipsCreated
}

func (counters *neoCounters) RelationshipsDeleted() int {
	return counters.relationshipsDeleted
}

func (counters *neoCounters) PropertiesSet() int {
	return counters.propertiesSet
}

func (counters *neoCounters) LabelsAdded() int {
	return counters.labelsAdded
}

func (counters *neoCounters) LabelsRemoved() int {
	return counters.labelsRemoved
}

func (counters *neoCounters) IndexesAdded() int {
	return counters.indexesAdded
}

func (counters *neoCounters) IndexesRemoved() int {
	return counters.indexesRemoved
}

func (counters *neoCounters) ConstraintsAdded() int {
	return counters.constraintsAdded
}

func (counters *neoCounters) ConstraintsRemoved() int {
	return counters.constraintsRemoved
}
