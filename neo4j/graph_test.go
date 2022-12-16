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
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"testing/quick"
	"time"
)

func TestGetProperty(outer *testing.T) {
	outer.Parallel()

	type testCase struct {
		description string
		make        func(map[string]any) neo4j.Entity
	}

	testCases := []testCase{
		{
			description: "Node",
			make: func(props map[string]any) neo4j.Entity {
				return neo4j.Node{Props: props}
			},
		},
		{
			description: "Rel",
			make: func(props map[string]any) neo4j.Entity {
				return neo4j.Relationship{Props: props}
			},
		},
	}

	for _, test := range testCases {
		outer.Run(fmt.Sprintf("[%s] gets property", test.description), func(t *testing.T) {
			check := func(key string, value int64) bool {
				entity := test.make(singleProp(key, value))

				validValue, noErr := neo4j.GetProperty[int64](entity, key)
				_, err1 := neo4j.GetProperty[int64](entity, key+"_nope")
				_, err2 := neo4j.GetProperty[string](entity, key)

				return validValue == value && noErr == nil && err1 != nil && err2 != nil
			}
			if err := quick.Check(check, nil); err != nil {
				t.Error(err)
			}
		})

		outer.Run(fmt.Sprintf("[%s] supports only valid property types", test.description), func(inner *testing.T) {
			now := time.Now()

			inner.Run("booleans", func(t *testing.T) {
				entity := test.make(singleProp("k", true))

				prop, err := neo4j.GetProperty[bool](entity, "k")

				AssertDeepEquals(t, prop, true)
				AssertNoError(t, err)
			})

			inner.Run("longs", func(t *testing.T) {
				entity := test.make(singleProp("k", int64(98)))

				prop, err := neo4j.GetProperty[int64](entity, "k")

				AssertDeepEquals(t, prop, int64(98))
				AssertNoError(t, err)
			})

			inner.Run("doubles", func(t *testing.T) {
				entity := test.make(singleProp("k", float64(99.42)))

				prop, err := neo4j.GetProperty[float64](entity, "k")

				AssertDeepEquals(t, prop, float64(99.42))
				AssertNoError(t, err)
			})

			inner.Run("strings", func(t *testing.T) {
				entity := test.make(singleProp("k", "v"))

				prop, err := neo4j.GetProperty[string](entity, "k")

				AssertDeepEquals(t, prop, "v")
				AssertNoError(t, err)
			})

			inner.Run("point2Ds", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.Point2D{X: 3, Y: 14, SpatialRefId: 7203}))

				prop, err := neo4j.GetProperty[neo4j.Point2D](entity, "k")

				AssertDeepEquals(t, prop, neo4j.Point2D{X: 3, Y: 14, SpatialRefId: 7203})
				AssertNoError(t, err)
			})

			inner.Run("point3Ds", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.Point3D{X: 3, Y: 1, Z: 4, SpatialRefId: 4979}))

				prop, err := neo4j.GetProperty[neo4j.Point3D](entity, "k")

				AssertDeepEquals(t, prop, neo4j.Point3D{X: 3, Y: 1, Z: 4, SpatialRefId: 4979})
				AssertNoError(t, err)
			})

			inner.Run("dates", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.DateOf(now)))

				prop, err := neo4j.GetProperty[neo4j.Date](entity, "k")

				AssertDeepEquals(t, prop, neo4j.DateOf(now))
				AssertNoError(t, err)
			})

			inner.Run("local times", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.LocalTimeOf(now)))

				prop, err := neo4j.GetProperty[neo4j.LocalTime](entity, "k")

				AssertDeepEquals(t, prop, neo4j.LocalTimeOf(now))
				AssertNoError(t, err)
			})

			inner.Run("local datetimes", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.LocalDateTimeOf(now)))

				prop, err := neo4j.GetProperty[neo4j.LocalDateTime](entity, "k")

				AssertDeepEquals(t, prop, neo4j.LocalDateTimeOf(now))
				AssertNoError(t, err)
			})

			inner.Run("times", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.Time(now)))

				prop, err := neo4j.GetProperty[neo4j.Time](entity, "k")

				AssertDeepEquals(t, prop, neo4j.Time(now))
				AssertNoError(t, err)
			})

			inner.Run("offset times", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.OffsetTimeOf(now)))

				prop, err := neo4j.GetProperty[neo4j.OffsetTime](entity, "k")

				AssertDeepEquals(t, prop, neo4j.OffsetTimeOf(now))
				AssertNoError(t, err)
			})

			inner.Run("durations", func(t *testing.T) {
				entity := test.make(singleProp("k", neo4j.DurationOf(5, 4, 3, 2)))

				prop, err := neo4j.GetProperty[neo4j.Duration](entity, "k")

				AssertDeepEquals(t, prop, neo4j.DurationOf(5, 4, 3, 2))
				AssertNoError(t, err)
			})

			inner.Run("byte arrays", func(t *testing.T) {
				entity := test.make(singleProp("k", []byte{1, 2, 3}))

				prop, err := neo4j.GetProperty[[]byte](entity, "k")

				AssertDeepEquals(t, prop, []byte{1, 2, 3})
				AssertNoError(t, err)
			})

			inner.Run("slices", func(t *testing.T) {
				entity := test.make(singleProp("k", []any{1, 2, 3}))

				prop, err := neo4j.GetProperty[[]any](entity, "k")

				AssertDeepEquals(t, prop, []any{1, 2, 3})
				AssertNoError(t, err)
			})
		})

	}

}

func singleProp[T any](key string, value T) map[string]any {
	return map[string]any{key: value}
}
