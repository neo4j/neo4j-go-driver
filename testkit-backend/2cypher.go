/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
)

// Converts native type to proxied "cypher" to be sent to frontend.
func nativeToCypher(v any) map[string]any {
	if v == nil {
		return map[string]any{"name": "CypherNull", "data": nil}
	}
	switch x := v.(type) {
	case int:
		return valueResponse("CypherInt", x)
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
	case dbtype.Vector[int8]:
		return vectorToCypher("i8", x)
	case dbtype.Vector[int16]:
		return vectorToCypher("i16", x)
	case dbtype.Vector[int32]:
		return vectorToCypher("i32", x)
	case dbtype.Vector[int64]:
		return vectorToCypher("i64", x)
	case dbtype.Vector[float32]:
		return vectorToCypher("f32", x)
	case dbtype.Vector[float64]:
		return vectorToCypher("f64", x)
	}

	panic(fmt.Sprintf("Don't know how to patch %T", v))
}

// Helper to wrap proxied "cypher" value into a response
func valueResponse(name string, v any) map[string]any {
	return map[string]any{"name": name, "data": map[string]any{"value": v}}
}

// Helper to convert Vector types to CypherVector format
func vectorToCypher[T dbtype.VectorElement](dtype string, vec dbtype.Vector[T]) map[string]any {
	// Convert vector to hex string
	var hexData string
	switch v := any(vec).(type) {
	case dbtype.Vector[int8]:
		bytes := make([]byte, 0, len(v))
		for _, val := range v {
			bytes = append(bytes, byte(val))
		}
		hexData = addSpacesToHex(fmt.Sprintf("%x", bytes))
	case dbtype.Vector[int16]:
		bytes := make([]byte, 0, len(v)*2)
		for _, val := range v {
			bytes = binary.BigEndian.AppendUint16(bytes, uint16(val))
		}
		hexData = addSpacesToHex(fmt.Sprintf("%x", bytes))
	case dbtype.Vector[int32]:
		bytes := make([]byte, 0, len(v)*4)
		for _, val := range v {
			bytes = binary.BigEndian.AppendUint32(bytes, uint32(val))
		}
		hexData = addSpacesToHex(fmt.Sprintf("%x", bytes))
	case dbtype.Vector[int64]:
		bytes := make([]byte, 0, len(v)*8)
		for _, val := range v {
			bytes = binary.BigEndian.AppendUint64(bytes, uint64(val))
		}
		hexData = addSpacesToHex(fmt.Sprintf("%x", bytes))
	case dbtype.Vector[float32]:
		bytes := make([]byte, 0, len(v)*4)
		for _, val := range v {
			bytes = binary.BigEndian.AppendUint32(bytes, math.Float32bits(val))
		}
		hexData = addSpacesToHex(fmt.Sprintf("%x", bytes))
	case dbtype.Vector[float64]:
		bytes := make([]byte, 0, len(v)*8)
		for _, val := range v {
			bytes = binary.BigEndian.AppendUint64(bytes, math.Float64bits(val))
		}
		hexData = addSpacesToHex(fmt.Sprintf("%x", bytes))
	default:
		panic(fmt.Sprintf("unsupported vector type: %T", v))
	}

	return map[string]any{
		"name": "CypherVector",
		"data": map[string]any{
			"dtype": dtype,
			"data":  hexData,
		},
	}
}

// Helper to add spaces between bytes in hex string
func addSpacesToHex(hexStr string) string {
	if len(hexStr) == 0 {
		return hexStr
	}

	var result strings.Builder
	for i := 0; i < len(hexStr); i += 2 {
		if i > 0 {
			result.WriteString(" ")
		}
		if i+1 < len(hexStr) {
			result.WriteString(hexStr[i : i+2])
		} else {
			result.WriteString(hexStr[i:])
		}
	}
	return result.String()
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
