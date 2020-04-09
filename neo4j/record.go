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

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
	types "github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

type Record interface {
	// Keys returns the keys available
	Keys() []string
	// Values returns the values
	Values() []interface{}
	// Get returns the value (if any) corresponding to the given key
	Get(key string) (interface{}, bool)
	// GetByIndex returns the value at given index
	GetByIndex(index int) interface{}
}

type record struct {
	rec *conn.Record
}

func patchRecordType(v interface{}) interface{} {
	switch x := v.(type) {
	case *types.Node:
		for k, v := range x.Props {
			x.Props[k] = patchRecordType(v)
		}
		return &node{node: x}
	case *types.Relationship:
		for k, v := range x.Props {
			x.Props[k] = patchRecordType(v)
		}
		return &relationship{rel: x}
	case *types.Path:
		for _, n := range x.Nodes {
			for k, v := range n.Props {
				n.Props[k] = patchRecordType(v)
			}
		}
		for _, n := range x.RelNodes {
			for k, v := range n.Props {
				n.Props[k] = patchRecordType(v)
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
			x[i] = patchRecordType(w)
		}
		return x
	case map[string]interface{}:
		for k, w := range x {
			x[k] = patchRecordType(w)
		}
		return x
	default:
		return v
	}
}

func newRecord(rec *conn.Record) *record {
	// Wrap internal types in interface implementation for backwards compatibility
	for i, v := range rec.Values {
		rec.Values[i] = patchRecordType(v)
	}
	return &record{rec: rec}
}

func (r *record) Keys() []string {
	return r.rec.Keys
}

func (r *record) Values() []interface{} {
	return r.rec.Values
}

func (r *record) GetByIndex(index int) interface{} {
	return r.rec.Values[index]
}

func (r *record) Get(key string) (interface{}, bool) {
	for i, k := range r.rec.Keys {
		if k == key {
			return r.rec.Values[i], true
		}
	}
	return nil, false
}
