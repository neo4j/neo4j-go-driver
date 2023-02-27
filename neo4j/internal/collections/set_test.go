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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collections_test

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collections"
	"math"
	"math/rand"
	"testing"
	"testing/quick"
)

func TestSet(outer *testing.T) {
	outer.Parallel()

	outer.Run("adds", func(t *testing.T) {
		ints := collections.NewSet([]int{})
		addition := func(i int) bool {
			ints.Add(i)
			return containsExactlyOnce(ints, i)
		}
		if err := quick.Check(addition, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("adds all", func(t *testing.T) {
		strings := collections.NewSet([]string{})
		additions := func(strs []string) bool {
			strings.AddAll(strs)
			for _, str := range strs {
				if !containsExactlyOnce(strings, str) {
					return false
				}
			}
			return true
		}
		if err := quick.Check(additions, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("union", func(t *testing.T) {
		strings := collections.NewSet([]string{})
		union := func(strs []string) bool {
			strings.Union(collections.NewSet(strs))
			for _, str := range strs {
				if !containsExactlyOnce(strings, str) {
					return false
				}
			}
			return true
		}
		if err := quick.Check(union, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("removes", func(t *testing.T) {
		randomInts := rand.Perm(100)
		removal := collections.NewSet(randomInts)
		addition := func(rawIndex int) bool {
			index := abs(rawIndex) % len(removal)
			element := randomInts[index]
			removal.Remove(element)
			return !containsExactlyOnce(removal, element)
		}
		if err := quick.Check(addition, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("removes all", func(t *testing.T) {
		randomInts := rand.Perm(100)
		ints := collections.NewSet(randomInts)
		removals := func(rawIndices []int) bool {
			indices := make([]int, len(rawIndices))
			for i, rawIndex := range rawIndices {
				indices[i] = abs(rawIndex) % len(ints)
			}
			for _, index := range indices {
				element := randomInts[index]
				ints.Remove(element)
				if containsExactlyOnce(ints, element) {
					return false
				}
			}
			return true
		}
		if err := quick.Check(removals, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("returns slice", func(t *testing.T) {
		sliceConversion := func(values []float64) bool {
			set := collections.NewSet(values)

			for _, value := range set.Values() {
				if !containsExactlyOnce(set, value) {
					return false
				}
			}
			return true
		}
		if err := quick.Check(sliceConversion, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("copies", func(t *testing.T) {
		setCopy := func(values []int64) bool {
			originalSet := collections.NewSet(values)
			copiedSet := originalSet.Copy()

			for _, value := range copiedSet.Values() {
				if !containsExactlyOnce(originalSet, value) {
					return false
				}
			}

			extra := rand.Int63()
			copiedSet.Add(extra)
			if len(copiedSet) != len(originalSet)+1 {
				return false
			}
			return !containsExactlyOnce(originalSet, extra)
		}
		if err := quick.Check(setCopy, nil); err != nil {
			t.Error(err)
		}
	})
}

func containsExactlyOnce[T comparable](values collections.Set[T], search T) bool {
	count := 0
	for value := range values {
		if value == search {
			count++
		}
	}
	return count == 1
}

func abs(i int) int {
	if i == math.MinInt {
		return math.MaxInt
	}
	if i < 0 {
		return -i
	}
	return i
}
