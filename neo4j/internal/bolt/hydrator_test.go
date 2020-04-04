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
		name       string
		tag        packstream.StructTag
		fields     []interface{}
		hydrateErr error
		hydrated   interface{}
	}{
		{
			name:       "Illegal tag",
			tag:        0x00,
			hydrateErr: errors.New("Something"),
		},
		{
			name: "Node",
			tag:  'N',
			fields: []interface{}{
				int64(112), []interface{}{"lbl1", "lbl2"}, map[string]interface{}{"prop1": 2}},
			hydrated: &types.Node{Id: 112, Labels: []string{"lbl1", "lbl2"}, Props: map[string]interface{}{"prop1": 2}},
		},
		{
			name:       "Node, no fields",
			tag:        'N',
			fields:     []interface{}{},
			hydrateErr: errors.New("something"),
		},
		{
			name: "Relationship",
			tag:  'R',
			fields: []interface{}{
				int64(3), int64(1), int64(2), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &types.Relationship{Id: 3, StartId: 1, EndId: 2, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
		},
		{
			name:       "Relationship, no fields",
			tag:        'R',
			fields:     []interface{}{},
			hydrateErr: errors.New("something"),
		},
		{
			name: "Path",
			tag:  'P',
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
			name: "RelNode",
			tag:  'r',
			fields: []interface{}{
				int64(3), "lbl1", map[string]interface{}{"propx": 3}},
			hydrated: &types.RelNode{Id: 3, Type: "lbl1", Props: map[string]interface{}{"propx": 3}},
		},
		{
			name:   "Point2D",
			tag:    'X',
			fields: []interface{}{int64(1), float64(2.0), float64(3.0)},
			hydrated: &types.Point2D{
				SpatialRefId: 1,
				X:            2.0,
				Y:            3.0,
			},
		},
		{
			name:   "Point3D",
			tag:    'Y',
			fields: []interface{}{int64(1), float64(2.0), float64(3.0), float64(4.0)},
			hydrated: &types.Point3D{
				SpatialRefId: 1,
				X:            2.0,
				Y:            3.0,
				Z:            4.0,
			},
		},
		{
			name:     "Success response",
			tag:      msgV3Success,
			fields:   []interface{}{map[string]interface{}{"x": 1}},
			hydrated: &successResponse{meta: map[string]interface{}{"x": 1}},
		},
		{
			name:     "Ignored response",
			tag:      msgV3Ignored,
			fields:   []interface{}{},
			hydrated: &ignoredResponse{},
		},
		{
			name:     "Failure response",
			tag:      msgV3Failure,
			fields:   []interface{}{map[string]interface{}{"code": "Code", "message": "Msg"}},
			hydrated: &failureResponse{code: "Code", message: "Msg"},
		},
		{
			name:     "Record response",
			tag:      msgV3Record,
			fields:   []interface{}{[]interface{}{1, "a"}},
			hydrated: &recordResponse{values: []interface{}{1, "a"}},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			hydrated, err := hydrate(c.tag, c.fields)
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
