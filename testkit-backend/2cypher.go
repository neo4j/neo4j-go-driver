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

package main

import (
	"fmt"

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
	}
	panic(fmt.Sprintf("Don't know how to patch %T", v))
}

// Helper to wrap proxied "cypher" value into a response
func valueResponse(name string, v interface{}) map[string]interface{} {
	return map[string]interface{}{"name": name, "data": map[string]interface{}{"value": v}}
}
