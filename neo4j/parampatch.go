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
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func patchParamType(x interface{}) interface{} {
	switch v := x.(type) {
	case *Point:
		if v.dimension == 2 {
			return &types.Point2D{X: v.x, Y: v.y, SpatialRefId: uint32(v.srId)}
		}
		return &types.Point3D{X: v.x, Y: v.y, Z: v.z, SpatialRefId: uint32(v.srId)}
	case Date:
		return types.Date(v.Time())
	case LocalTime:
		return types.LocalTime(v.Time())
	case OffsetTime:
		return types.Time(v.Time())
	case LocalDateTime:
		return types.LocalDateTime(v.Time())
	case Duration:
		return types.Duration{Months: v.months, Days: v.days, Seconds: v.seconds, Nanos: v.nanos}
	default:
		return v
	}
}

func patchParams(params map[string]interface{}) map[string]interface{} {
	patched := make(map[string]interface{}, len(params))
	for k, v := range params {
		patched[k] = patchParamType(v)
	}
	return patched
}
