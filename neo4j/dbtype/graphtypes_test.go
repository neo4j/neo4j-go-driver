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

package dbtype

import (
	"testing"
)

func TestGraphPath(ot *testing.T) {
	cases := []struct {
		name  string
		path  Path
		nodes []int64
		rels  []Relationship
	}{
		{
			name: "Two nodes",
			path: Path{
				Nodes:    []*Node{&Node{Id: 1}, &Node{Id: 2}},
				RelNodes: []*RelNode{&RelNode{Id: 3, Type: "x"}},
				Indexes:  []int{1, 1},
			},
			nodes: []int64{1, 2},
			rels: []Relationship{
				Relationship{Id: 3, StartId: 1, EndId: 2, Type: "x"},
			},
		},
		{
			name: "Two nodes reverse",
			path: Path{
				Nodes:    []*Node{&Node{Id: 1}, &Node{Id: 2}},
				RelNodes: []*RelNode{&RelNode{Id: 3, Type: "x"}},
				Indexes:  []int{-1, 1},
			},
			nodes: []int64{1, 2},
			rels: []Relationship{
				Relationship{Id: 3, StartId: 2, EndId: 1, Type: "x"},
			},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			p := c.path
			rels := p.Relationships()
			if len(rels) != len(c.rels) {
				t.Errorf("Wrong numbber of relationships")
			}
			for i, rel := range rels {
				// Compare expected relationship, hard cast interface
				// to known implementation.
				erel := c.rels[i]
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
