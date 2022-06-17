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
	"time"
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c interface{}) (any, error) {
	m := c.(map[string]interface{})
	d := m["data"].(map[string]interface{})
	n := m["name"]
	switch n {
	case "CypherDateTime":
		year := d["year"].(float64)
		month := d["month"].(float64)
		day := d["day"].(float64)
		hour := d["hour"].(float64)
		minute := d["minute"].(float64)
		second := d["second"].(float64)
		nanosecond := d["nanosecond"].(float64)
		offset := d["utc_offset_s"].(float64)
		var timezone *time.Location
		var err error
		if timezoneId, found := d["timezone_id"]; !found || timezoneId == nil {
			timezone = time.FixedZone("Offset", int(offset))
		} else {
			timezone, err = time.LoadLocation(timezoneId.(string))
			if err != nil {
				return nil, err
			}
		}
		// TODO: check whether the offset (possibly from the named time zone) matches
		// the offset sent by Testkit, if Testkit sends one along the tz name
		return time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), int(nanosecond), timezone), nil
	case "CypherString":
		return d["value"].(string), nil
	case "CypherInt":
		return int64(d["value"].(float64)), nil
	case "CypherBool":
		return d["value"].(bool), nil
	case "CypherFloat":
		return d["value"].(float64), nil
	case "CypherNull":
		return nil, nil
	case "CypherList":
		lc := d["value"].([]interface{})
		ln := make([]interface{}, len(lc))
		var err error
		for i, x := range lc {
			if ln[i], err = cypherToNative(x); err != nil {
				return nil, err
			}
		}
		return ln, nil
	case "CypherMap":
		mc := d["value"].(map[string]interface{})
		mn := make(map[string]interface{})
		var err error
		for k, x := range mc {
			if mn[k], err = cypherToNative(x); err != nil {
				return nil, err
			}
		}
		return mn, nil
	}
	panic(fmt.Sprintf("Don't know how to convert %s to native", n))
}
