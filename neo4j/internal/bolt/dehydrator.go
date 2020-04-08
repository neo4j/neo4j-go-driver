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

package bolt

import (
	"errors"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

// Called by packstream during packing when it encounters an unknown type.
func dehydrate(x interface{}) (*packstream.Struct, error) {
	switch v := x.(type) {
	case *types.Point2D:
		return &packstream.Struct{
			Tag:    packstream.StructTag('X'),
			Fields: []interface{}{v.SpatialRefId, v.X, v.Y},
		}, nil
	case *types.Point3D:
		return &packstream.Struct{
			Tag:    packstream.StructTag('Y'),
			Fields: []interface{}{v.SpatialRefId, v.X, v.Y, v.Z},
		}, nil
	case time.Time:
		fields := []interface{}{v.Unix(), v.Nanosecond(), 0 /*Make room*/}
		zone, offset := v.Zone()
		// If this value has been hydrated by us the zone is set to "Offset" to indicate
		// that the offset type has been and should be used again, otherwise we don't know,
		// so use the zone name to be more specific.
		if zone == "Offset" {
			fields[2] = offset
			return &packstream.Struct{Tag: packstream.StructTag('F'), Fields: fields}, nil
		} else {
			fields[2] = v.Location().String()
			return &packstream.Struct{Tag: packstream.StructTag('f'), Fields: fields}, nil
		}
	case types.LocalDateTime:
		t := time.Time(v)
		fields := []interface{}{t.Unix(), t.Nanosecond()}
		return &packstream.Struct{Tag: packstream.StructTag('d'), Fields: fields}, nil
	case types.Date:
		// v is UTC
		daysSinceEpoch := int64(time.Time(v).Sub(time.Unix(0, 0)).Hours()) / 24
		return &packstream.Struct{Tag: packstream.StructTag('D'), Fields: []interface{}{daysSinceEpoch}}, nil
	case types.Time:
		t := time.Time(v)
		_, tzOffsetSecs := t.Zone()
		t = t.UTC()
		d := t.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
		return &packstream.Struct{Tag: packstream.StructTag('T'), Fields: []interface{}{d.Nanoseconds(), tzOffsetSecs}}, nil
	case types.LocalTime:
		t := time.Time(v)
		nanos := int64(time.Hour)*int64(t.Hour()) + int64(time.Minute)*int64(t.Minute()) + int64(time.Second)*int64(t.Second()) + int64(t.Nanosecond())
		return &packstream.Struct{Tag: packstream.StructTag('t'), Fields: []interface{}{nanos}}, nil
	// Support standard duration to simplify for client
	case time.Duration:
		nanos := v.Nanoseconds()
		months, nanos := divmod(nanos, 2629800000000000)
		days, nanos := divmod(nanos, 86400000000000)
		seconds, nanos := divmod(nanos, 1e9)
		return &packstream.Struct{Tag: packstream.StructTag('E'), Fields: []interface{}{months, days, seconds, nanos}}, nil
	case types.Duration:
		return &packstream.Struct{Tag: packstream.StructTag('E'), Fields: []interface{}{v.Months, v.Days, v.Seconds, v.Nanos}}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unable to dehydrate type %T", x))
	}
}

func divmod(num, den int64) (int64, int64) {
	return num / den, num % den
}
