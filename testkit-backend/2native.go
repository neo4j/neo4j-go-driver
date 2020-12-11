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
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c interface{}) interface{} {
	m := c.(map[string]interface{})
	d := m["data"].(map[string]interface{})
	n := m["name"]
	switch n {
	case "CypherString":
		return d["value"].(string)
	case "CypherInt":
		return int64(d["value"].(float64))
	case "CypherBool":
		return d["value"].(bool)
	case "CypherFloat":
		return d["value"].(float64)
	case "CypherNull":
		return nil
	case "CypherList":
		lc := d["value"].([]interface{})
		ln := make([]interface{}, len(lc))
		for i, x := range lc {
			ln[i] = cypherToNative(x)
		}
		return ln
	case "CypherMap":
		mc := d["value"].(map[string]interface{})
		mn := make(map[string]interface{})
		for k, x := range mc {
			mn[k] = cypherToNative(x)
		}
		return mn
	}
	panic(fmt.Sprintf("Don't know how to convert %s to native", n))
}
