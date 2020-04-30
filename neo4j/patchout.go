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

package neo4j

import (
	"math"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func patchOutX(v interface{}) interface{} {
	switch x := v.(type) {
	case *types.Node:
		for k, v := range x.Props {
			x.Props[k] = patchOutX(v)
		}
		return &node{node: x}
	case *types.Relationship:
		for k, v := range x.Props {
			x.Props[k] = patchOutX(v)
		}
		return &relationship{rel: x}
	case *types.Path:
		for _, n := range x.Nodes {
			for k, v := range n.Props {
				n.Props[k] = patchOutX(v)
			}
		}
		for _, n := range x.RelNodes {
			for k, v := range n.Props {
				n.Props[k] = patchOutX(v)
			}
		}
		return &path{path: x}
	case *types.Point2D:
		return &Point{dimension: 2, srId: int(x.SpatialRefId), x: x.X, y: x.Y, z: math.NaN()}
	case *types.Point3D:
		return &Point{dimension: 3, srId: int(x.SpatialRefId), x: x.X, y: x.Y, z: x.Z}
	case types.Time:
		return OffsetTimeOf(time.Time(x))
	case types.Date:
		return DateOf(time.Time(x))
	case types.LocalTime:
		return LocalTimeOf(time.Time(x))
	case types.LocalDateTime:
		return LocalDateTimeOf(time.Time(x))
	case types.Duration:
		return Duration{months: x.Months, days: x.Days, seconds: x.Seconds, nanos: x.Nanos}
	case []interface{}:
		for i, w := range x {
			x[i] = patchOutX(w)
		}
		return x
	case map[string]interface{}:
		for k, w := range x {
			x[k] = patchOutX(w)
		}
		return x
	default:
		return v
	}
}
