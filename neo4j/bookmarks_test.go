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

package neo4j_test

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"testing/quick"
)

func TestCombineBookmarks(t *testing.T) {
	f := func(slices []neo4j.Bookmarks) bool {
		concatenation := neo4j.CombineBookmarks(slices...)
		totalLen := 0
		for _, s := range slices {
			totalLen += len(s)
		}
		if totalLen != len(concatenation) {
			return false
		}
		i := 0
		for _, slice := range slices {
			for _, str := range slice {
				if str != concatenation[i] {
					return false
				}
				i += 1
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestBookmarkManager(outer *testing.T) {
	ctx := context.Background()

	outer.Parallel()

	outer.Run("deduplicates initial bookmarks", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: neo4j.Bookmarks{"a", "a", "b"},
		})

		bookmarks, err := bookmarkManager.GetBookmarks(ctx)

		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, bookmarks, []string{"a", "b"})
	})

	outer.Run("gets no bookmarks by default", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{})
		getBookmarks := func(db string) bool {
			bookmarks, err := bookmarkManager.GetBookmarks(ctx)
			AssertNoError(t, err)
			return bookmarks == nil
		}

		if err := quick.Check(getBookmarks, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("gets bookmarks along with user-supplied bookmarks", func(t *testing.T) {
		expectedBookmarks := neo4j.Bookmarks{"a", "b", "c"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: neo4j.Bookmarks{"a", "b"},
			BookmarkSupplier: func(context.Context) (neo4j.Bookmarks, error) {
				return neo4j.Bookmarks{"b", "c"}, nil
			},
		})

		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("user-supplied bookmarks do not alter internal bookmarks", func(t *testing.T) {
		calls := 0
		expectedBookmarks := []string{"a"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: neo4j.Bookmarks{"a"},
			BookmarkSupplier: func(ctx2 context.Context) (neo4j.Bookmarks, error) {
				defer func() {
					calls++
				}()
				if calls == 0 {
					return neo4j.Bookmarks{"b"}, nil
				}
				return nil, nil
			},
		})

		_, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("returned bookmarks are copies", func(t *testing.T) {
		expectedBookmarks := []string{"a"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: neo4j.Bookmarks{"a"},
		})
		bookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		bookmarks[0] = "changed"

		bookmarks, err = bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)

		AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
	})

	outer.Run("updates bookmarks", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: neo4j.Bookmarks{"a", "b", "c"},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, []string{"b", "c"}, []string{"d", "a"})

		AssertNoError(t, err)
		expectedBookmarks := []string{"a", "d"}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("notifies updated bookmarks for new DB", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			BookmarkConsumer: func(_ context.Context, bookmarks neo4j.Bookmarks) error {
				notifyHookCalled = true
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, nil, []string{"d", "a"})

		AssertNoError(t, err)
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})

	outer.Run("does not notify updated bookmarks when empty", func(t *testing.T) {
		initialBookmarks := []string{"a", "b"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: initialBookmarks,
			BookmarkConsumer: func(_ context.Context, bookmarks neo4j.Bookmarks) error {
				t.Error("I must not be called")
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, initialBookmarks, nil)

		AssertNoError(t, err)
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, initialBookmarks)
	})

	outer.Run("notifies updated bookmarks for existing DB without bookmarks", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: nil,
			BookmarkConsumer: func(_ context.Context, bookmarks neo4j.Bookmarks) error {
				notifyHookCalled = true
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, nil, []string{"d", "a"})

		AssertNoError(t, err)
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})

	outer.Run("notifies updated bookmarks for existing DB with previous bookmarks", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: neo4j.Bookmarks{"a", "b", "c"},
			BookmarkConsumer: func(_ context.Context, bookmarks neo4j.Bookmarks) error {
				notifyHookCalled = true
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, []string{"b", "c"}, []string{"d", "a"})

		AssertNoError(t, err)
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})
}
