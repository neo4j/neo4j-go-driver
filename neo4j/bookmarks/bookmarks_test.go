package bookmarks_test

import (
	"context"
	"testing"
	"testing/quick"

	bm "github.com/neo4j/neo4j-go-driver/v5/neo4j/bookmarks"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func TestCombineBookmarks(t *testing.T) {
	f := func(slices []bm.Bookmarks) bool {
		concatenation := bm.CombineBookmarks(slices...)
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
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: bm.Bookmarks{"a", "a", "b"},
		})

		bookmarks, err := bookmarkManager.GetBookmarks(ctx)

		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, bookmarks, []string{"a", "b"})
	})

	outer.Run("gets no bookmarks by default", func(t *testing.T) {
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{})
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
		expectedBookmarks := bm.Bookmarks{"a", "b", "c"}
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: bm.Bookmarks{"a", "b"},
			BookmarkSupplier: func(context.Context) (bm.Bookmarks, error) {
				return bm.Bookmarks{"b", "c"}, nil
			},
		})

		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("user-supplied bookmarks do not alter internal bookmarks", func(t *testing.T) {
		calls := 0
		expectedBookmarks := []string{"a"}
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: bm.Bookmarks{"a"},
			BookmarkSupplier: func(ctx2 context.Context) (bm.Bookmarks, error) {
				defer func() {
					calls++
				}()
				if calls == 0 {
					return bm.Bookmarks{"b"}, nil
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
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: bm.Bookmarks{"a"},
		})
		bookmarks, err := bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)
		bookmarks[0] = "changed"

		bookmarks, err = bookmarkManager.GetBookmarks(ctx)
		AssertNoError(t, err)

		AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
	})

	outer.Run("updates bookmarks", func(t *testing.T) {
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: bm.Bookmarks{"a", "b", "c"},
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
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			BookmarkConsumer: func(_ context.Context, bookmarks bm.Bookmarks) error {
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
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: initialBookmarks,
			BookmarkConsumer: func(_ context.Context, bookmarks bm.Bookmarks) error {
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
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: nil,
			BookmarkConsumer: func(_ context.Context, bookmarks bm.Bookmarks) error {
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
		bookmarkManager := bm.NewBookmarkManager(bm.BookmarkManagerConfig{
			InitialBookmarks: bm.Bookmarks{"a", "b", "c"},
			BookmarkConsumer: func(_ context.Context, bookmarks bm.Bookmarks) error {
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
