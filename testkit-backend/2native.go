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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c any) (any, error) {
	m := c.(map[string]any)
	d := m["data"].(map[string]any)
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
		lc := d["value"].([]any)
		ln := make([]any, len(lc))
		var err error
		for i, x := range lc {
			if ln[i], err = cypherToNative(x); err != nil {
				return nil, err
			}
		}
		return ln, nil
	case "CypherMap":
		mc := d["value"].(map[string]any)
		mn := make(map[string]any)
		var err error
		for k, x := range mc {
			if mn[k], err = cypherToNative(x); err != nil {
				return nil, err
			}
		}
		return mn, nil
	case "CypherPoint":
		spatialReference := d["system"].(string)
		is2d := d["z"] == nil
		x := asFloat64(d["x"].(json.Number))
		y := asFloat64(d["y"].(json.Number))
		if spatialReference == "cartesian" {
			if is2d {
				return dbtype.Point2D{
					SpatialRefId: 7203,
					X:            x,
					Y:            y,
				}, nil
			}
			return dbtype.Point3D{
				SpatialRefId: 9157,
				X:            x,
				Y:            y,
				Z:            asFloat64(d["z"].(json.Number)),
			}, nil
		}
		if spatialReference == "wgs84" {
			if is2d {
				return dbtype.Point2D{
					SpatialRefId: 4326,
					X:            x,
					Y:            y,
				}, nil
			}
			return dbtype.Point3D{
				SpatialRefId: 4979,
				X:            x,
				Y:            y,
				Z:            asFloat64(d["z"].(json.Number)),
			}, nil
		}
		panic(fmt.Errorf("unknown spatial reference ID: %s", spatialReference))
	case "CypherVector":
		dtype := d["dtype"].(string)
		data := d["data"].(string)

		// Parse hex string to bytes
		hexData := strings.ReplaceAll(data, " ", "")
		var bytes []byte
		var err error
		if hexData == "" {
			// Empty hex string means empty vector
			bytes = []byte{}
		} else {
			bytes, err = hex.DecodeString(hexData)
			if err != nil {
				return nil, fmt.Errorf("failed to decode hex data: %v", err)
			}
		}

		switch dtype {
		case "i8":
			vec := make(dbtype.Vector[int8], 0, len(bytes))
			for _, b := range bytes {
				vec = append(vec, int8(b))
			}
			return vec, nil
		case "i16":
			if len(bytes)%2 != 0 {
				return nil, fmt.Errorf("invalid data length for i16: %d", len(bytes))
			}
			vec := make(dbtype.Vector[int16], 0, len(bytes)/2)
			for i := 0; i < len(bytes); i += 2 {
				vec = append(vec, int16(binary.BigEndian.Uint16(bytes[i:i+2])))
			}
			return vec, nil
		case "i32":
			if len(bytes)%4 != 0 {
				return nil, fmt.Errorf("invalid data length for i32: %d", len(bytes))
			}
			vec := make(dbtype.Vector[int32], 0, len(bytes)/4)
			for i := 0; i < len(bytes); i += 4 {
				vec = append(vec, int32(binary.BigEndian.Uint32(bytes[i:i+4])))
			}
			return vec, nil
		case "i64":
			if len(bytes)%8 != 0 {
				return nil, fmt.Errorf("invalid data length for i64: %d", len(bytes))
			}
			vec := make(dbtype.Vector[int64], 0, len(bytes)/8)
			for i := 0; i < len(bytes); i += 8 {
				vec = append(vec, int64(binary.BigEndian.Uint64(bytes[i:i+8])))
			}
			return vec, nil
		case "f32":
			if len(bytes)%4 != 0 {
				return nil, fmt.Errorf("invalid data length for f32: %d", len(bytes))
			}
			vec := make(dbtype.Vector[float32], 0, len(bytes)/4)
			for i := 0; i < len(bytes); i += 4 {
				vec = append(vec, math.Float32frombits(binary.BigEndian.Uint32(bytes[i:i+4])))
			}
			return vec, nil
		case "f64":
			if len(bytes)%8 != 0 {
				return nil, fmt.Errorf("invalid data length for f64: %d", len(bytes))
			}
			vec := make(dbtype.Vector[float64], 0, len(bytes)/8)
			for i := 0; i < len(bytes); i += 8 {
				vec = append(vec, math.Float64frombits(binary.BigEndian.Uint64(bytes[i:i+8])))
			}
			return vec, nil
		default:
			return nil, fmt.Errorf("unsupported vector dtype: %s", dtype)
		}
	}
	panic(fmt.Sprintf("Don't know how to convert %s to native", n))
}

func loadTimezone(data map[string]any) (*time.Location, int, error) {
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

func readOffset(data map[string]any) (int, bool) {
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

func asFloat64(number json.Number) float64 {
	result, err := number.Float64()
	if err != nil {
		panic(fmt.Sprintf("could not convert JSON value to float64: %v", err))
	}
	return result
}
