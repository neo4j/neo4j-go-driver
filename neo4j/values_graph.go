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
	// Id returns the identity of this Node.
	Id() int64
	// Labels returns the labels attached to this Node.
	Labels() []string
	// Props returns the properties of this Node.
	Props() map[string]interface{}
}

// Relationship represents a relationship in the neo4j graph database
type Relationship interface {
	// Id returns the identity of this Relationship.
	Id() int64
	// StartId returns the identity of the start node of this Relationship.
	StartId() int64
	// EndId returns the identity of the end node of this Relationship.
	EndId() int64
	// Type returns the type of this Relationship.
	Type() string
	// Props returns the properties of this Relationship.
	Props() map[string]interface{}
}

type segment interface {
	Start() Node
	Relationship() Relationship
	End() Node
}

// Path represents a directed sequence of relationships between two nodes.
// This generally represents a traversal or walk through a graph and maintains a direction separate from that of any
// relationships traversed. It is allowed to be of size 0, meaning there are no relationships in it. In this case,
// it contains only a single node which is both the start and the end of the path.
type Path interface {
	// Nodes returns all the nodes in the path.
	Nodes() []Node
	// Relationships returns all the relationships in the path.
	Relationships() []Relationship
}

type relationshipValue struct {
	id      int64
	startId int64
	endId   int64
	relType string
	props   map[string]interface{}
}

type nodeValue struct {
	id     int64
	labels []string
	props  map[string]interface{}
}

type segmentValue struct {
	start        *nodeValue
	relationship *relationshipValue
	end          *nodeValue
}

type pathValue struct {
	segments      []segment
	nodes         []Node
	relationships []Relationship
}

// ID returns id of the node
func (node *nodeValue) Id() int64 {
	return node.id
}

// Labels returns labels of the node
func (node *nodeValue) Labels() []string {
	return node.labels
}

// Props returns properties of the node
func (node *nodeValue) Props() map[string]interface{} {
	return node.props
}

// ID returns id of the relationship
func (rel *relationshipValue) Id() int64 {
	return rel.id
}

// StartID returns the id of the start node
func (rel *relationshipValue) StartId() int64 {
	return rel.startId
}

// EndID returns the id of the end node
func (rel *relationshipValue) EndId() int64 {
	return rel.endId
}

// Type returns the relationship tyoe
func (rel *relationshipValue) Type() string {
	return rel.relType
}

// Props returns properties of the relationship
func (rel *relationshipValue) Props() map[string]interface{} {
	return rel.props
}

func (segment *segmentValue) Start() Node {
	return segment.start
}

func (segment *segmentValue) End() Node {
	return segment.end
}

func (segment *segmentValue) Relationship() Relationship {
	return segment.relationship
}

// Nodes returns the ordered list of nodes available on the path
func (path *pathValue) Nodes() []Node {
	return path.nodes
}

// Relationships returns the ordered list of relationships on the path
func (path *pathValue) Relationships() []Relationship {
	return path.relationships
}
