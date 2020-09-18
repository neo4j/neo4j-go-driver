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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
)

// Aliases to simplify client usage (fewer imports) and to provide some backwards
// compatibility with 1.x driver.
//
// A separate dbtype package is needed to avoid circular package references and to avoid
// unnecessary copying/conversions between structs.
type (
	Point2D       = dbtype.Point2D
	Point3D       = dbtype.Point3D
	Date          = dbtype.Date
	LocalTime     = dbtype.LocalTime
	LocalDateTime = dbtype.LocalDateTime
	Time          = dbtype.Time // AKA OffsetTime
	OffsetTime    = dbtype.Time
	Duration      = dbtype.Duration
	Node          = dbtype.Node
	Relationship  = dbtype.Relationship
	Path          = dbtype.Path
	Record        = db.Record
)

// TODO: Point is gone, Point2D and Point3D is the new

func DurationOf(months, days, seconds int64, nanos int) dbtype.Duration {
	return Duration{Months: months, Days: days, Seconds: seconds, Nanos: nanos}
}

// TODO: Move these time funcs somewhere
// TODO: Document these and make note that explicit casting is to be preferred to func call.
// TODO: For backwards compatibility with 1.8 driver, provide casting of temporal types as functions
// When serializing to the database only the relevant parts of the time.Time component
// will be used and deserializing will set the irrelevant parts to a known value.
//
// But to avoid confusion we make the returned time look more like the expected (makes
// writing tests simpler since they ensure equality reflect.Equal).
// Extensive use of t.Local() to force applying monotonic clock to wall time when t is from time.Now

func DateOf(t time.Time) dbtype.Date {
	y, m, d := t.Date()
	t = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	return dbtype.Date(t)
}

func LocalTimeOf(t time.Time) dbtype.LocalTime {
	t = time.Date(0, 0, 0, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	return dbtype.LocalTime(t)
}

func LocalDateTimeOf(t time.Time) dbtype.LocalDateTime {
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	return dbtype.LocalDateTime(t)
}

func OffsetTimeOf(t time.Time) dbtype.Time {
	_, offset := t.Zone()
	l := time.FixedZone("Offset", int(offset))
	t = time.Date(0, 0, 0, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l)
	return dbtype.Time(t)
}
