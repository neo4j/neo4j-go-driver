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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	types "github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

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

// Wrap types.Node struct in interface for backwards compatibility
type node struct {
	node *types.Node
}

func (n *node) Id() int64                     { return n.node.Id }
func (n *node) Labels() []string              { return n.node.Labels }
func (n *node) Props() map[string]interface{} { return n.node.Props }

// Wrap types.Relationship struct in interface for backwards compatibility
type relationship struct {
	rel *types.Relationship
}

func (r *relationship) Id() int64                     { return r.rel.Id }
func (r *relationship) StartId() int64                { return r.rel.StartId }
func (r *relationship) EndId() int64                  { return r.rel.EndId }
func (r *relationship) Type() string                  { return r.rel.Type }
func (r *relationship) Props() map[string]interface{} { return r.rel.Props }

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

// Wrap types.Path struct in interface for backwards compatibility
type path struct {
	path *types.Path
}

func (p path) Nodes() []Node {
	nodes := make([]Node, len(p.path.Nodes))
	for i, n := range p.path.Nodes {
		nodes[i] = &node{node: n}
	}
	return nodes
}

func (p path) Relationships() []Relationship {
	num := len(p.path.Indexes) / 2
	if num == 0 {
		return nil
	}
	rels := make([]Relationship, 0, num)

	i := 0
	n1 := p.path.Nodes[0]
	for num > 0 {
		relni := p.path.Indexes[i]
		i++
		n2i := p.path.Indexes[i]
		i++
		num--
		var reln *types.RelNode
		var n1start bool
		if relni < 0 {
			reln = p.path.RelNodes[(relni*-1)-1]
		} else {
			reln = p.path.RelNodes[relni-1]
			n1start = true
		}
		n2 := p.path.Nodes[n2i]

		rel := &types.Relationship{
			Id:    reln.Id,
			Type:  reln.Type,
			Props: reln.Props,
		}
		if n1start {
			rel.StartId = n1.Id
			rel.EndId = n2.Id
		} else {
			rel.StartId = n2.Id
			rel.EndId = n1.Id
		}
		rels = append(rels, &relationship{rel: rel})
		n1 = n2
	}
	return rels
}
