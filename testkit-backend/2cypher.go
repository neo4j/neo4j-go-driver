/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
	"fmt"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/dbtype"
	"time"

	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j"
)

// Converts native type to proxied "cypher" to be sent to frontend.
func nativeToCypher(v any) map[string]any {
	if v == nil {
		return map[string]any{"name": "CypherNull", "data": nil}
	}
	switch x := v.(type) {
	case int64:
		return valueResponse("CypherInt", x)
	case string:
		return valueResponse("CypherString", x)
	case bool:
		return valueResponse("CypherBool", x)
	case float64:
		return valueResponse("CypherFloat", x)
	case dbtype.Date:
		date := x.Time()
		values := map[string]any{
			"year":  date.Year(),
			"month": date.Month(),
			"day":   date.Day(),
		}
		return map[string]any{
			"name": "CypherDate",
			"data": values,
		}
	case dbtype.LocalDateTime:
		localDateTime := x.Time()
		values := map[string]any{
			"year":       localDateTime.Year(),
			"month":      localDateTime.Month(),
			"day":        localDateTime.Day(),
			"hour":       localDateTime.Hour(),
			"minute":     localDateTime.Minute(),
			"second":     localDateTime.Second(),
			"nanosecond": localDateTime.Nanosecond(),
		}
		return map[string]any{
			"name": "CypherDateTime",
			"data": values,
		}
	case dbtype.Duration:
		values := map[string]any{
			"months":      x.Months,
			"days":        x.Days,
			"seconds":     x.Seconds,
			"nanoseconds": x.Nanos,
		}
		return map[string]any{
			"name": "CypherDuration",
			"data": values,
		}
	case dbtype.Time:
		time := x.Time()
		_, offset := time.Zone()
		values := map[string]any{
			"hour":         time.Hour(),
			"minute":       time.Minute(),
			"second":       time.Second(),
			"nanosecond":   time.Nanosecond(),
			"utc_offset_s": offset,
		}
		return map[string]any{
			"name": "CypherTime",
			"data": values,
		}
	case dbtype.LocalTime:
		localTime := x.Time()
		values := map[string]any{
			"hour":       localTime.Hour(),
			"minute":     localTime.Minute(),
			"second":     localTime.Second(),
			"nanosecond": localTime.Nanosecond(),
		}
		return map[string]any{
			"name": "CypherTime",
			"data": values,
		}
	case time.Time:
		tzName, offset := x.Zone()
		values := map[string]any{
			"year":         x.Year(),
			"month":        x.Month(),
			"day":          x.Day(),
			"hour":         x.Hour(),
			"minute":       x.Minute(),
			"second":       x.Second(),
			"nanosecond":   x.Nanosecond(),
			"utc_offset_s": offset,
		}
		if tzName != "Offset" {
			values["timezone_id"] = x.Location().String()
		}
		return map[string]any{
			"name": "CypherDateTime",
			"data": values,
		}

	case []any:
		values := make([]any, len(x))
		for i, y := range x {
			values[i] = nativeToCypher(y)
		}
		return valueResponse("CypherList", values)
	case []string:
		values := make([]any, len(x))
		for i, y := range x {
			values[i] = nativeToCypher(y)
		}
		return valueResponse("CypherList", values)
	case map[string]any:
		values := make(map[string]any)
		for k, v := range x {
			values[k] = nativeToCypher(v)
		}
		return valueResponse("CypherMap", values)
	case neo4j.Node:
		return map[string]any{
			"name": "Node",
			"data": map[string]any{
				//lint:ignore SA1019 Id is supported at least until 6.0
				"id":        nativeToCypher(x.Id),
				"elementId": nativeToCypher(x.ElementId),
				"labels":    nativeToCypher(x.Labels),
				"props":     nativeToCypher(x.Props),
			}}
	case neo4j.Relationship:
		return map[string]any{
			"name": "Relationship",
			"data": map[string]any{
				//lint:ignore SA1019 Id is supported at least until 6.0
				"id":        nativeToCypher(x.Id),
				"elementId": nativeToCypher(x.ElementId),
				//lint:ignore SA1019 StartId is supported at least until 6.0
				"startNodeId":        nativeToCypher(x.StartId),
				"startNodeElementId": nativeToCypher(x.StartElementId),
				//lint:ignore SA1019 EndId is supported at least until 6.0
				"endNodeId":        nativeToCypher(x.EndId),
				"endNodeElementId": nativeToCypher(x.EndElementId),
				"type":             nativeToCypher(x.Type),
				"props":            nativeToCypher(x.Props),
			}}
	case neo4j.Path:
		nodes := make([]any, len(x.Nodes))
		for i := range x.Nodes {
			nodes[i] = x.Nodes[i]
		}
		rels := make([]any, len(x.Relationships))
		for i := range x.Relationships {
			rels[i] = x.Relationships[i]
		}
		return map[string]any{
			"name": "Path",
			"data": map[string]any{
				"nodes":         nativeToCypher(nodes),
				"relationships": nativeToCypher(rels),
			},
		}
	case dbtype.Point2D:
		return map[string]any{
			"name": "CypherPoint",
			"data": map[string]any{
				"system": spatialReference(x.SpatialRefId),
				"x":      x.X,
				"y":      x.Y,
			},
		}
	case dbtype.Point3D:
		return map[string]any{
			"name": "CypherPoint",
			"data": map[string]any{
				"system": spatialReference(x.SpatialRefId),
				"x":      x.X,
				"y":      x.Y,
				"z":      x.Z,
			},
		}
	}
	panic(fmt.Sprintf("Don't know how to patch %T", v))
}

// Helper to wrap proxied "cypher" value into a response
func valueResponse(name string, v any) map[string]any {
	return map[string]any{"name": name, "data": map[string]any{"value": v}}
}

func spatialReference(spatialRefId uint32) string {
	switch spatialRefId {
	case 7203:
		fallthrough
	case 9157:
		return "cartesian"
	case 4326:
		fallthrough
	case 4979:
		return "wgs84"
	default:
		panic(fmt.Errorf("unknown spatial reference ID: %d", spatialRefId))
	}
}
