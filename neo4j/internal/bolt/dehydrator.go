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

	"github.com/neo4j/neo4j-go-driver/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
)

// Called by packstream during packing when it encounters an unknown type.
func dehydrate(x interface{}) (*packstream.Struct, error) {
	switch v := x.(type) {
	case *dbtype.Point2D:
		return &packstream.Struct{
			Tag:    packstream.StructTag('X'),
			Fields: []interface{}{v.SpatialRefId, v.X, v.Y},
		}, nil
	case *dbtype.Point3D:
		return &packstream.Struct{
			Tag:    packstream.StructTag('Y'),
			Fields: []interface{}{v.SpatialRefId, v.X, v.Y, v.Z},
		}, nil
	case time.Time:
		zone, offset := v.Zone()
		secs := v.Unix() + int64(offset)
		fields := []interface{}{secs, v.Nanosecond(), 0 /*Make room*/}
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
	case dbtype.LocalDateTime:
		t := time.Time(v)
		_, offset := t.Zone()
		secs := t.Unix() + int64(offset)
		fields := []interface{}{secs, t.Nanosecond()}
		return &packstream.Struct{Tag: packstream.StructTag('d'), Fields: fields}, nil
	case dbtype.Date:
		t := time.Time(v)
		secs := t.Unix()
		_, offset := t.Zone()
		secs += int64(offset)
		days := secs / (60 * 60 * 24)
		return &packstream.Struct{Tag: packstream.StructTag('D'), Fields: []interface{}{days}}, nil
	case dbtype.Time:
		t := time.Time(v)
		_, tzOffsetSecs := t.Zone()
		d := t.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
		return &packstream.Struct{Tag: packstream.StructTag('T'), Fields: []interface{}{d.Nanoseconds(), tzOffsetSecs}}, nil
	case dbtype.LocalTime:
		t := time.Time(v)
		nanos := int64(time.Hour)*int64(t.Hour()) + int64(time.Minute)*int64(t.Minute()) + int64(time.Second)*int64(t.Second()) + int64(t.Nanosecond())
		return &packstream.Struct{Tag: packstream.StructTag('t'), Fields: []interface{}{nanos}}, nil
	case dbtype.Duration:
		return &packstream.Struct{Tag: packstream.StructTag('E'), Fields: []interface{}{v.Months, v.Days, v.Seconds, v.Nanos}}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unable to dehydrate type %T", x))
	}
}

func divmod(num, den int64) (int64, int64) {
	return num / den, num % den
}
