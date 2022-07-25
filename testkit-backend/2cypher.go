/*
 * Copyright (c) "Neo4j"
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

package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// Converts native type to proxied "cypher" to be sent to frontend.
func nativeToCypher(v interface{}) map[string]interface{} {
	if v == nil {
		return map[string]interface{}{"name": "CypherNull", "data": nil}
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
		values := map[string]interface{}{
			"year":  date.Year(),
			"month": date.Month(),
			"day":   date.Day(),
		}
		return map[string]interface{}{
			"name": "CypherDate",
			"data": values,
		}
	case dbtype.LocalDateTime:
		localDateTime := x.Time()
		values := map[string]interface{}{
			"year":       localDateTime.Year(),
			"month":      localDateTime.Month(),
			"day":        localDateTime.Day(),
			"hour":       localDateTime.Hour(),
			"minute":     localDateTime.Minute(),
			"second":     localDateTime.Second(),
			"nanosecond": localDateTime.Nanosecond(),
		}
		return map[string]interface{}{
			"name": "CypherDateTime",
			"data": values,
		}
	case dbtype.Duration:
		values := map[string]interface{}{
			"months":      x.Months,
			"days":        x.Days,
			"seconds":     x.Seconds,
			"nanoseconds": x.Nanos,
		}
		return map[string]interface{}{
			"name": "CypherDuration",
			"data": values,
		}
	case dbtype.Time:
		time := x.Time()
		_, offset := time.Zone()
		values := map[string]interface{}{
			"hour":         time.Hour(),
			"minute":       time.Minute(),
			"second":       time.Second(),
			"nanosecond":   time.Nanosecond(),
			"utc_offset_s": offset,
		}
		return map[string]interface{}{
			"name": "CypherTime",
			"data": values,
		}
	case dbtype.LocalTime:
		localTime := x.Time()
		values := map[string]interface{}{
			"hour":       localTime.Hour(),
			"minute":     localTime.Minute(),
			"second":     localTime.Second(),
			"nanosecond": localTime.Nanosecond(),
		}
		return map[string]interface{}{
			"name": "CypherTime",
			"data": values,
		}
	case time.Time:
		tzName, offset := x.Zone()
		values := map[string]interface{}{
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
		return map[string]interface{}{
			"name": "CypherDateTime",
			"data": values,
		}

	case []interface{}:
		values := make([]interface{}, len(x))
		for i, y := range x {
			values[i] = nativeToCypher(y)
		}
		return valueResponse("CypherList", values)
	case []string:
		values := make([]interface{}, len(x))
		for i, y := range x {
			values[i] = nativeToCypher(y)
		}
		return valueResponse("CypherList", values)
	case map[string]interface{}:
		values := make(map[string]interface{})
		for k, v := range x {
			values[k] = nativeToCypher(v)
		}
		return valueResponse("CypherMap", values)
	case neo4j.Node:
		return map[string]interface{}{
			"name": "Node", "data": map[string]interface{}{
				"id":     nativeToCypher(x.Id),
				"labels": nativeToCypher(x.Labels),
				"props":  nativeToCypher(x.Props),
			}}
	case neo4j.Relationship:
		return map[string]interface{}{
			"name": "Relationship",
			"data": map[string]interface{}{
				"id":          nativeToCypher(x.Id),
				"startNodeId": nativeToCypher(x.StartId),
				"endNodeId":   nativeToCypher(x.EndId),
				"type":        nativeToCypher(x.Type),
				"props":       nativeToCypher(x.Props),
			}}
	case neo4j.Path:
		nodes := make([]interface{}, len(x.Nodes))
		for i := range x.Nodes {
			nodes[i] = x.Nodes[i]
		}
		rels := make([]interface{}, len(x.Relationships))
		for i := range x.Relationships {
			rels[i] = x.Relationships[i]
		}
		return map[string]interface{}{
			"name": "Path",
			"data": map[string]interface{}{
				"nodes":         nativeToCypher(nodes),
				"relationships": nativeToCypher(rels),
			},
		}
	}
	panic(fmt.Sprintf("Don't know how to patch %T", v))
}

// Helper to wrap proxied "cypher" value into a response
func valueResponse(name string, v interface{}) map[string]interface{} {
	return map[string]interface{}{"name": name, "data": map[string]interface{}{"value": v}}
}
