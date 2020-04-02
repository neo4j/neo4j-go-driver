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

package bolt

import (
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

type hydrator struct{}

// Hydrator is stateless, no need for own copy for each connection
var sharedHydrator hydrator

// Called by packstream. Returns func to handle hydration of tag.
func (h hydrator) Hydrator(tag packstream.StructTag, numFields int) (packstream.Hydrate, error) {
	switch tag {
	case msgV3Success:
		return hydrateSuccess, nil
	case msgV3Ignored:
		return hydrateIgnored, nil
	case msgV3Failure:
		return hydrateFailure, nil
	case msgV3Record:
		return hydrateRecord, nil
	case 'N':
		return hydrateNode, nil
	case 'R':
		return hydrateRelationship, nil
	case 'r':
		return hydrateRelNode, nil
	case 'P':
		return hydratePath, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unknown tag: %02x", tag))
	}
}

func hydrateNode(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Node hydrate error")
	}
	id, idok := fields[0].(int64)
	tagsx, tagsok := fields[1].([]interface{})
	props, propsok := fields[2].(map[string]interface{})
	if !idok || !tagsok || !propsok {
		return nil, errors.New("Node hydrate error")
	}
	n := &types.Node{Id: id, Props: props, Labels: make([]string, len(tagsx))}
	// Convert tags to strings
	for i, x := range tagsx {
		t, tok := x.(string)
		if !tok {
			return nil, errors.New("Node hydrate error")
		}
		n.Labels[i] = t
	}
	return n, nil
}

func hydrateRelationship(fields []interface{}) (interface{}, error) {
	if len(fields) != 5 {
		return nil, errors.New("Relationship hydrate error")
	}
	id, idok := fields[0].(int64)
	sid, sidok := fields[1].(int64)
	eid, eidok := fields[2].(int64)
	lbl, lblok := fields[3].(string)
	props, propsok := fields[4].(map[string]interface{})
	if !idok || !sidok || !eidok || !lblok || !propsok {
		return nil, errors.New("Relationship hydrate error")
	}
	return &types.Relationship{Id: id, StartId: sid, EndId: eid, Type: lbl, Props: props}, nil
}

func hydrateRelNode(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("RelNode hydrate error")
	}
	id, idok := fields[0].(int64)
	lbl, lblok := fields[1].(string)
	props, propsok := fields[2].(map[string]interface{})
	if !idok || !lblok || !propsok {
		return nil, errors.New("RelNode hydrate error")
	}
	s := &types.RelNode{Id: id, Type: lbl, Props: props}
	fmt.Printf("seg: %+v\n", s)
	return s, nil
}

func hydratePath(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Path hydrate error")
	}
	nodesx, nok := fields[0].([]interface{})
	relnodesx, rok := fields[1].([]interface{})
	indsx, iok := fields[2].([]interface{})
	if !nok || !rok || !iok {
		return nil, errors.New("Path hydrate error")
	}

	nodes := make([]*types.Node, len(nodesx))
	for i, nx := range nodesx {
		n, ok := nx.(*types.Node)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		nodes[i] = n
	}

	relnodes := make([]*types.RelNode, len(relnodesx))
	for i, rx := range relnodesx {
		r, ok := rx.(*types.RelNode)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		relnodes[i] = r
	}

	indexes := make([]int, len(indsx))
	for i, ix := range indsx {
		p, ok := ix.(int64)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		indexes[i] = int(p)
	}
	// Must be even number
	if (len(indexes) & 0x01) == 1 {
		return nil, errors.New("Path hydrate error")
	}

	p := &types.Path{Nodes: nodes, RelNodes: relnodes, Indexes: indexes}
	return p, nil
}

func hydrateSuccess(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Success hydrate error")
	}
	meta, metaok := fields[0].(map[string]interface{})
	if !metaok {
		return nil, errors.New("Success hydrate error")
	}
	return &successResponse{meta: meta}, nil
}

func hydrateRecord(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Record hydrate error")
	}
	v, vok := fields[0].([]interface{})
	if !vok {
		return nil, errors.New("Record hydrate error")
	}
	return &recordResponse{values: v}, nil
}

func hydrateIgnored(fields []interface{}) (interface{}, error) {
	if len(fields) != 0 {
		return nil, errors.New("Ignored hydrate error")
	}
	return &ignoredResponse{}, nil
}

func hydrateFailure(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Failure hydrate error")
	}
	m, mok := fields[0].(map[string]interface{})
	if !mok {
		return nil, errors.New("Failure hydrate error")
	}
	code, cok := m["code"].(string)
	msg, mok := m["message"].(string)
	if !cok || !mok {
		return nil, errors.New("Failure hydrate error")
	}
	return &failureResponse{code: code, message: msg}, nil
}
