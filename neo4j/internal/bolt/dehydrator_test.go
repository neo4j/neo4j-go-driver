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
	"testing"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func TestDehydrator(ot *testing.T) {
	cases := []struct {
		name string
		x    interface{}
		s    *packstream.Struct
	}{
		{
			name: "Point2D",
			x:    &types.Point2D{SpatialRefId: 7, X: 8.0, Y: 9.0},
			s:    &packstream.Struct{Tag: 'X', Fields: []interface{}{uint32(7), 8.0, 9.0}},
		},
		{
			name: "Point3D",
			x:    &types.Point3D{SpatialRefId: 7, X: 8.0, Y: 9.0, Z: 10},
			s:    &packstream.Struct{Tag: 'Y', Fields: []interface{}{uint32(7), 8.0, 9.0, 10.0}},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			s, _ := dehydrate(c.x)
			// Compare Structs
			if s.Tag != c.s.Tag {
				t.Errorf("Wrong tags, expected %d but was %d", c.s.Tag, s.Tag)
			}
			if len(c.s.Fields) != len(s.Fields) {
				t.Errorf("Wrong number of fields, expected %d but was %d", len(c.s.Fields), len(s.Fields))
			}
			for i, v := range c.s.Fields {
				if s.Fields[i] != v {
					t.Errorf("Field %d differs, expected %+v but was %+v", i, v, s.Fields[i])
				}
			}
		})
	}
}
