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

package bolt

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
)

func TestBuildPath(ot *testing.T) {
	cases := []struct {
		name     string
		nodes    []dbtype.Node
		relNodes []*relNode
		indexes  []int
		path     dbtype.Path
	}{
		{
			name: "Two nodes",
			path: dbtype.Path{
				Nodes: []dbtype.Node{dbtype.Node{Id: 1}, dbtype.Node{Id: 2}},
				Relationships: []dbtype.Relationship{
					dbtype.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "x"},
				},
			},
			nodes:    []dbtype.Node{dbtype.Node{Id: 1}, dbtype.Node{Id: 2}},
			relNodes: []*relNode{&relNode{id: 3, name: "x"}},
			indexes:  []int{1, 1},
		},
		{
			name: "Two nodes reverse",
			path: dbtype.Path{
				Nodes: []dbtype.Node{dbtype.Node{Id: 1}, dbtype.Node{Id: 2}},
				Relationships: []dbtype.Relationship{
					dbtype.Relationship{Id: 3, StartId: 2, EndId: 1, Type: "x"},
				},
			},
			nodes:    []dbtype.Node{dbtype.Node{Id: 1}, dbtype.Node{Id: 2}},
			relNodes: []*relNode{&relNode{id: 3, name: "x"}},
			indexes:  []int{-1, 1},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			path := buildPath(c.nodes, c.relNodes, c.indexes)
			if len(path.Relationships) != len(c.path.Relationships) {
				t.Fatal("Wrong number of relationships")
			}
			for i, rel := range path.Relationships {
				// Compare expected relationship
				erel := c.path.Relationships[i]
				if rel.Id != erel.Id {
					t.Errorf("Relation %d not as expected, ids differ", i)
				}
				if rel.StartId != erel.StartId {
					t.Errorf("Relation %d not as expected, start ids differ", i)
				}
				if rel.EndId != erel.EndId {
					t.Errorf("Relation %d not as expected, end ids differ", i)
				}
				if rel.Type != erel.Type {
					t.Errorf("Relation %d not as expected, types differ", i)
				}
			}
		})
	}
}
