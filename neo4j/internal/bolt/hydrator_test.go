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
	"reflect"
	"testing"

	"errors"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func TestHydrator(ot *testing.T) {
	cases := []struct {
		name        string
		tag         packstream.StructTag
		numFields   int
		hydratorErr error
		fields      []interface{}
		hydrateErr  error
		hydrated    interface{}
	}{
		{
			name:        "Illegal tag",
			tag:         0x00,
			numFields:   0,
			hydratorErr: errors.New("Something"),
		},
		{
			name:      "Node",
			tag:       'N',
			numFields: 3,
			fields: []interface{}{
				int64(112), []interface{}{"lbl1", "lbl2"}, map[string]interface{}{"prop1": 2}},
			hydrated: &types.Node{Id: 112, Labels: []string{"lbl1", "lbl2"}, Props: map[string]interface{}{"prop1": 2}},
		},
		{
			name:       "Node, no fields",
			tag:        'N',
			numFields:  3,
			fields:     []interface{}{},
			hydrateErr: errors.New("something"),
		},
		{
			name:      "Relationship",
			tag:       'R',
			numFields: 5,
			fields: []interface{}{
				int64(3), int64(1), int64(2), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &types.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
		},
		{
			name:       "Relationship, no fields",
			tag:        'R',
			numFields:  5,
			fields:     []interface{}{},
			hydrateErr: errors.New("something"),
		},
		{
			name:      "Path",
			tag:       'P',
			numFields: 3,
			fields: []interface{}{
				[]interface{}{&types.Node{}, &types.Node{}},
				[]interface{}{&types.RelNode{}},
				[]interface{}{int64(1), int64(1)},
			},
			hydrated: &types.Path{
				Nodes:    []*types.Node{&types.Node{}, &types.Node{}},
				RelNodes: []*types.RelNode{&types.RelNode{}},
				Indexes:  []int{1, 1},
			},
		},
		{
			name:      "RelNode",
			tag:       'r',
			numFields: 3,
			fields: []interface{}{
				int64(3), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &types.RelNode{Id: 3, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
		},
		{
			name:      "Success response",
			tag:       msgV3Success,
			numFields: 1,
			fields:    []interface{}{map[string]interface{}{"x": 1}},
			hydrated:  &successResponse{meta: map[string]interface{}{"x": 1}},
		},
		{
			name:      "Ignored response",
			tag:       msgV3Ignored,
			numFields: 0,
			fields:    []interface{}{},
			hydrated:  &ignoredResponse{},
		},
		{
			name:      "Failure response",
			tag:       msgV3Failure,
			numFields: 1,
			fields:    []interface{}{map[string]interface{}{"code": "Code", "message": "Msg"}},
			hydrated:  &failureResponse{code: "Code", message: "Msg"},
		},
		{
			name:      "Record response",
			tag:       msgV3Record,
			numFields: 1,
			fields:    []interface{}{[]interface{}{1, "a"}},
			hydrated:  &recordResponse{values: []interface{}{1, "a"}},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			h := &hydrator{}
			hydrate, err := h.Hydrator(c.tag, c.numFields)
			if (err != nil) != (c.hydratorErr != nil) {
				t.Fatalf("Expected error '%+v' but was '%+v'", c.hydratorErr, err)
			}
			if err != nil {
				return
			}
			hydrated, err := hydrate(c.fields)
			if (err != nil) != (c.hydrateErr != nil) {
				t.Fatalf("Expected error '%+v' but was '%+v'", c.hydrateErr, err)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(hydrated, c.hydrated) {
				t.Fatalf("Hydrated differs, expected: %+v but was: %+v", c.hydrated, hydrated)
			}
		})
	}
}
