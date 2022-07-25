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
	"encoding/json"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"time"
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c interface{}) (interface{}, error) {
	m := c.(map[string]interface{})
	d := m["data"].(map[string]interface{})
	n := m["name"]
	switch n {
	case "CypherDateTime":
		year := d["year"].(json.Number)
		month := d["month"].(json.Number)
		day := d["day"].(json.Number)
		hour := d["hour"].(json.Number)
		minute := d["minute"].(json.Number)
		second := d["second"].(json.Number)
		nanosecond := d["nanosecond"].(json.Number)
		timezone, expectedOffset, err := loadTimezone(d)
		if err != nil {
			return nil, err
		}
		dateTime := time.Date(asInt(year), time.Month(asInt(month)), asInt(day), asInt(hour), asInt(minute), asInt(second), asInt(nanosecond), timezone)
		if timezone == time.Local {
			return dbtype.LocalDateTime(dateTime), nil
		}
		if _, actualOffset := dateTime.Zone(); actualOffset != expectedOffset {
			return nil, fmt.Errorf("expected UTC offset of %d for %s, but actual offset is %d", expectedOffset, d, actualOffset)
		}
		return dateTime, nil
	case "CypherDate":
		year := d["year"].(json.Number)
		month := d["month"].(json.Number)
		day := d["day"].(json.Number)
		return dbtype.Date(time.Date(asInt(year), time.Month(asInt(month)), asInt(day), 0, 0, 0, 0, time.Local)), nil
	case "CypherDuration":
		months := d["months"].(json.Number)
		days := d["days"].(json.Number)
		seconds := d["seconds"].(json.Number)
		nanoseconds := d["nanoseconds"].(json.Number)
		return dbtype.Duration{
			Months:  asInt64(months),
			Days:    asInt64(days),
			Seconds: asInt64(seconds),
			Nanos:   asInt(nanoseconds),
		}, nil
	case "CypherTime":
		hour := d["hour"].(json.Number)
		minute := d["minute"].(json.Number)
		second := d["second"].(json.Number)
		nanosecond := d["nanosecond"].(json.Number)
		timeZone := time.Local
		if offset, foundOffset := readOffset(d); foundOffset {
			timeZone = time.FixedZone("Offset", offset)
			return dbtype.Time(time.Date(0, 0, 0, asInt(hour), asInt(minute), asInt(second), asInt(nanosecond), timeZone)), nil
		}
		return dbtype.LocalTime(time.Date(0, 0, 0, asInt(hour), asInt(minute), asInt(second), asInt(nanosecond), timeZone)), nil
	case "CypherString":
		return d["value"].(string), nil
	case "CypherInt":
		return d["value"].(json.Number).Int64()
	case "CypherBool":
		return d["value"].(bool), nil
	case "CypherFloat":
		return d["value"].(json.Number).Float64()
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

func loadTimezone(data map[string]interface{}) (*time.Location, int, error) {
	offset, foundOffset := readOffset(data)
	rawTimezoneId := data["timezone_id"]
	if rawTimezoneId != nil {
		timezoneId := rawTimezoneId.(string)
		location, err := time.LoadLocation(timezoneId)
		if err != nil {
			return nil, 0, err
		}
		return location, offset, nil
	}
	if !foundOffset {
		return time.Local, 0, nil
	}
	return time.FixedZone("Offset", offset), offset, nil
}

func readOffset(data map[string]interface{}) (int, bool) {
	if rawOffset := data["utc_offset_s"]; rawOffset != nil {
		return asInt(rawOffset.(json.Number)), true
	}
	return 0, false
}

func asInt(number json.Number) int {
	return int(asInt64(number))
}

func asInt64(number json.Number) int64 {
	result, err := number.Int64()
	if err != nil {
		panic(fmt.Sprintf("could not convert JSON value to int64: %v", err))
	}
	return result
}
