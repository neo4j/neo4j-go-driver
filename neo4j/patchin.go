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
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func patchInPoint(v *Point) interface{} {
	if v == nil {
		return nil
	}
	if v.dimension == 2 {
		return &types.Point2D{X: v.x, Y: v.y, SpatialRefId: uint32(v.srId)}
	}
	return &types.Point3D{X: v.x, Y: v.y, Z: v.z, SpatialRefId: uint32(v.srId)}
}

func patchInDuration(v *Duration) interface{} {
	if v == nil {
		return nil
	}
	return types.Duration{Months: v.months, Days: v.days, Seconds: v.seconds, Nanos: v.nanos}
}

func patchInX(x interface{}) interface{} {
	switch v := x.(type) {
	case *Point:
		return patchInPoint(v)
	case Point:
		return patchInPoint(&v)
	case []*Point:
		patched := make([]interface{}, len(v))
		for i, p := range v {
			patched[i] = patchInPoint(p)
		}
		return patched
	case []Point:
		patched := make([]interface{}, len(v))
		for i, p := range v {
			patched[i] = patchInPoint(&p)
		}
		return patched
	case Date:
		return types.Date(v.Time())
	case *Date:
		if v == nil {
			return nil
		}
		return types.Date(v.Time())
	case LocalTime:
		return types.LocalTime(v.Time())
	case *LocalTime:
		if v == nil {
			return nil
		}
		return types.LocalTime(v.Time())
	case OffsetTime:
		return types.Time(v.Time())
	case []OffsetTime:
		patched := make([]interface{}, len(v))
		for i, x := range v {
			patched[i] = types.Time(x.Time())
		}
		return patched
	case *OffsetTime:
		if v == nil {
			return nil
		}
		return types.Time(v.Time())
	case LocalDateTime:
		return types.LocalDateTime(v.Time())
	case *LocalDateTime:
		if v == nil {
			return nil
		}
		return types.LocalDateTime(v.Time())
	case Duration:
		return patchInDuration(&v)
	case *Duration:
		return patchInDuration(v)
	case []Duration:
		patched := make([]interface{}, len(v))
		for i, x := range v {
			patched[i] = patchInDuration(&x)
		}
		return patched
	case []interface{}:
		patched := make([]interface{}, len(v))
		for i, x := range v {
			patched[i] = patchInX(x)
		}
		return patched
	case *time.Time:
		if v == nil {
			return nil
		}
		return *v
	default:
		return v
	}
}

func patchInMapX(params map[string]interface{}) map[string]interface{} {
	patched := make(map[string]interface{}, len(params))
	for k, v := range params {
		patched[k] = patchInX(v)
	}
	return patched
}
