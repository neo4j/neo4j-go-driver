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

package neo4j_test

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"testing/quick"
	"time"
)

func TestGetRecordValue(outer *testing.T) {
	outer.Parallel()

	outer.Run("gets record value", func(t *testing.T) {
		checkValue := func(key string, value int64) bool {
			val, isNil, err := neo4j.GetRecordValue[int64](record(key, value), key)
			return value == val && !isNil && err == nil
		}
		checkNotFoundValue := func(key string, value int64) bool {
			_, isNil, err := neo4j.GetRecordValue[int64](record(key, value), key+"_nope")
			return !isNil && err.Error() == "record value "+key+"_nope not found"
		}
		checkIncompatibleValue := func(key string, value int64) bool {
			_, isNil, err := neo4j.GetRecordValue[string](record(key, value), key)
			return !isNil && err.Error() == "expected value to have type string but found type int64"
		}
		checkNilValue := func(key string) bool {
			_, isNil, err := neo4j.GetRecordValue[int64](record(key, nil), key)
			return isNil && err == nil
		}

		for _, check := range []any{checkValue, checkNotFoundValue, checkIncompatibleValue, checkNilValue} {
			if err := quick.Check(check, nil); err != nil {
				outer.Error(err)
			}
		}
	})

	outer.Run("supports only valid record values", func(inner *testing.T) {
		now := time.Now()

		inner.Run("booleans", func(t *testing.T) {
			entity := record("k", true)

			value, isNil, err := neo4j.GetRecordValue[bool](entity, "k")

			AssertDeepEquals(t, value, true)
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("longs", func(t *testing.T) {
			entity := record("k", int64(98))

			value, isNil, err := neo4j.GetRecordValue[int64](entity, "k")

			AssertDeepEquals(t, value, int64(98))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("doubles", func(t *testing.T) {
			entity := record("k", float64(99.42))

			value, isNil, err := neo4j.GetRecordValue[float64](entity, "k")

			AssertDeepEquals(t, value, float64(99.42))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("strings", func(t *testing.T) {
			entity := record("k", "v")

			value, isNil, err := neo4j.GetRecordValue[string](entity, "k")

			AssertDeepEquals(t, value, "v")
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("point2Ds", func(t *testing.T) {
			entity := record("k", neo4j.Point2D{X: 3, Y: 14, SpatialRefId: 7203})

			value, isNil, err := neo4j.GetRecordValue[neo4j.Point2D](entity, "k")

			AssertDeepEquals(t, value, neo4j.Point2D{X: 3, Y: 14, SpatialRefId: 7203})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("point3Ds", func(t *testing.T) {
			entity := record("k", neo4j.Point3D{X: 3, Y: 1, Z: 4, SpatialRefId: 4979})

			value, isNil, err := neo4j.GetRecordValue[neo4j.Point3D](entity, "k")

			AssertDeepEquals(t, value, neo4j.Point3D{X: 3, Y: 1, Z: 4, SpatialRefId: 4979})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("dates", func(t *testing.T) {
			entity := record("k", neo4j.DateOf(now))

			value, isNil, err := neo4j.GetRecordValue[neo4j.Date](entity, "k")

			AssertDeepEquals(t, value, neo4j.DateOf(now))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("local times", func(t *testing.T) {
			entity := record("k", neo4j.LocalTimeOf(now))

			value, isNil, err := neo4j.GetRecordValue[neo4j.LocalTime](entity, "k")

			AssertDeepEquals(t, value, neo4j.LocalTimeOf(now))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("local datetimes", func(t *testing.T) {
			entity := record("k", neo4j.LocalDateTimeOf(now))

			value, isNil, err := neo4j.GetRecordValue[neo4j.LocalDateTime](entity, "k")

			AssertDeepEquals(t, value, neo4j.LocalDateTimeOf(now))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("times", func(t *testing.T) {
			entity := record("k", neo4j.Time(now))

			value, isNil, err := neo4j.GetRecordValue[neo4j.Time](entity, "k")

			AssertDeepEquals(t, value, neo4j.Time(now))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("offset times", func(t *testing.T) {
			entity := record("k", neo4j.OffsetTimeOf(now))

			value, isNil, err := neo4j.GetRecordValue[neo4j.OffsetTime](entity, "k")

			AssertDeepEquals(t, value, neo4j.OffsetTimeOf(now))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("datetimes", func(t *testing.T) {
			entity := record("k", now)

			value, isNil, err := neo4j.GetRecordValue[time.Time](entity, "k")

			AssertDeepEquals(t, value, now)
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("durations", func(t *testing.T) {
			entity := record("k", neo4j.DurationOf(5, 4, 3, 2))

			value, isNil, err := neo4j.GetRecordValue[neo4j.Duration](entity, "k")

			AssertDeepEquals(t, value, neo4j.DurationOf(5, 4, 3, 2))
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("byte arrays", func(t *testing.T) {
			entity := record("k", []byte{1, 2, 3})

			value, isNil, err := neo4j.GetRecordValue[[]byte](entity, "k")

			AssertDeepEquals(t, value, []byte{1, 2, 3})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("slices", func(t *testing.T) {
			entity := record("k", []any{1, 2, 3})

			value, isNil, err := neo4j.GetRecordValue[[]any](entity, "k")

			AssertDeepEquals(t, value, []any{1, 2, 3})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("maps", func(t *testing.T) {
			entity := record("k", map[string]any{"un": 1, "dos": 2, "tres": 3})

			value, isNil, err := neo4j.GetRecordValue[map[string]any](entity, "k")

			AssertDeepEquals(t, value, map[string]any{"un": 1, "dos": 2, "tres": 3})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("nodes", func(t *testing.T) {
			entity := record("k", neo4j.Node{Props: singleProp("n", 12)})

			value, isNil, err := neo4j.GetRecordValue[neo4j.Node](entity, "k")

			AssertDeepEquals(t, value, neo4j.Node{Props: singleProp("n", 12)})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("relationships", func(t *testing.T) {
			entity := record("k", neo4j.Relationship{Props: singleProp("n", 12)})

			value, isNil, err := neo4j.GetRecordValue[neo4j.Relationship](entity, "k")

			AssertDeepEquals(t, value, neo4j.Relationship{Props: singleProp("n", 12)})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

		inner.Run("paths", func(t *testing.T) {
			entity := record("k", neo4j.Path{
				Nodes:         []neo4j.Node{{Props: singleProp("n", 12)}},
				Relationships: []neo4j.Relationship{{Props: singleProp("m", 21)}},
			})

			value, isNil, err := neo4j.GetRecordValue[neo4j.Path](entity, "k")

			AssertDeepEquals(t, value, neo4j.Path{
				Nodes:         []neo4j.Node{{Props: singleProp("n", 12)}},
				Relationships: []neo4j.Relationship{{Props: singleProp("m", 21)}},
			})
			AssertFalse(t, isNil)
			AssertNoError(t, err)
		})

	})
}

func record(key string, value any) *neo4j.Record {
	return &neo4j.Record{
		Values: []any{value},
		Keys:   []string{key},
	}
}
