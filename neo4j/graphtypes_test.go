package neo4j

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func TestGraphPath(ot *testing.T) {
	cases := []struct {
		name    string
		rawPath types.Path
		nodes   []int64
		rels    []types.Relationship
	}{
		{
			name: "Two nodes",
			rawPath: types.Path{
				Nodes:    []*types.Node{&types.Node{Id: 1}, &types.Node{Id: 2}},
				RelNodes: []*types.RelNode{&types.RelNode{Id: 3, Type: "x"}},
				Indexes:  []int{1, 1},
			},
			nodes: []int64{1, 2},
			rels: []types.Relationship{
				types.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "x"},
			},
		},
		{
			name: "Two nodes reverse",
			rawPath: types.Path{
				Nodes:    []*types.Node{&types.Node{Id: 1}, &types.Node{Id: 2}},
				RelNodes: []*types.RelNode{&types.RelNode{Id: 3, Type: "x"}},
				Indexes:  []int{-1, 1},
			},
			nodes: []int64{1, 2},
			rels: []types.Relationship{
				types.Relationship{Id: 3, StartId: 2, EndId: 1, Type: "x"},
			},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			p := path{path: &c.rawPath}
			nodes := p.Nodes()
			if len(nodes) != len(c.nodes) {
				t.Errorf("Wrong numbber of nodes")
			}
			rels := p.Relationships()
			if len(rels) != len(c.rels) {
				t.Errorf("Wrong numbber of relationships")
			}
			for i, reli := range rels {
				// Compare expected relationship, hard cast interface
				// to known implementation.
				rel := reli.(*relationship).rel
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
