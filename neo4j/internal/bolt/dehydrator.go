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

// Called by packstream during packing when it encounters an unknown type.
func dehydrate(x interface{}) (*packstream.Struct, error) {
	switch v := x.(type) {
	case *types.Point2D:
		return &packstream.Struct{
			Tag:    packstream.StructTag('X'),
			Fields: []interface{}{v.SpatialRefId, v.X, v.Y},
		}, nil
	case *types.Point3D:
		return &packstream.Struct{
			Tag:    packstream.StructTag('Y'),
			Fields: []interface{}{v.SpatialRefId, v.X, v.Y, v.Z},
		}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unable to dehydrate type %T", x))
	}
}
