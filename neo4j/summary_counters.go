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

package neo4j

// Counters contains statistics about the changes made to the database made as part
// of the statement execution.
type Counters interface {
	// Whether there were any updates at all, eg. any of the counters are greater than 0.
	ContainsUpdates() bool
	// The number of nodes created.
	NodesCreated() int
	// The number of nodes deleted.
	NodesDeleted() int
	// The number of relationships created.
	RelationshipsCreated() int
	// The number of relationships deleted.
	RelationshipsDeleted() int
	PropertiesSet() int
	// The number of labels added to nodes.
	LabelsAdded() int
	// The number of labels removed from nodes.
	LabelsRemoved() int
	// The number of indexes added to the schema.
	IndexesAdded() int
	// The number of indexes removed from the schema.
	IndexesRemoved() int
	// The number of constraints added to the schema.
	ConstraintsAdded() int
	// The number of constraints removed from the schema.
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

func (counters *neoCounters) ContainsUpdates() bool {
	return counters.nodesCreated > 0 ||
		counters.nodesDeleted > 0 ||
		counters.relationshipsCreated > 0 ||
		counters.relationshipsDeleted > 0 ||
		counters.propertiesSet > 0 ||
		counters.labelsAdded > 0 ||
		counters.labelsRemoved > 0 ||
		counters.indexesAdded > 0 ||
		counters.indexesRemoved > 0 ||
		counters.constraintsAdded > 0 ||
		counters.constraintsRemoved > 0
}
