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

package dbtype

// Node represents a node in the neo4j graph database
type Node struct {
	Id     int64                  // Id of this node.
	Labels []string               // Labels attached to this Node.
	Props  map[string]interface{} // Properties of this Node.
}

// Relationship represents a relationship in the neo4j graph database
type Relationship struct {
	Id      int64                  // Identity of this Relationship.
	StartId int64                  // Identity of the start node of this Relationship.
	EndId   int64                  // Identity of the end node of this Relationship.
	Type    string                 // Type of this Relationship.
	Props   map[string]interface{} // Properties of this Relationship.
}

type RelNode struct {
	Id    int64
	Type  string
	Props map[string]interface{}
}

// Path represents a directed sequence of relationships between two nodes.
// This generally represents a traversal or walk through a graph and maintains a direction separate from that of any
// relationships traversed. It is allowed to be of size 0, meaning there are no relationships in it. In this case,
// it contains only a single node which is both the start and the end of the path.
type Path struct {
	Nodes    []*Node // All the nodes in the path.
	RelNodes []*RelNode
	Indexes  []int
}

func (p *Path) Relationships() []*Relationship {
	num := len(p.Indexes) / 2
	if num == 0 {
		return nil
	}
	rels := make([]*Relationship, 0, num)

	i := 0
	n1 := p.Nodes[0]
	for num > 0 {
		relni := p.Indexes[i]
		i++
		n2i := p.Indexes[i]
		i++
		num--
		var reln *RelNode
		var n1start bool
		if relni < 0 {
			reln = p.RelNodes[(relni*-1)-1]
		} else {
			reln = p.RelNodes[relni-1]
			n1start = true
		}
		n2 := p.Nodes[n2i]

		rel := &Relationship{
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
		rels = append(rels, rel)
		n1 = n2
	}
	return rels
}
