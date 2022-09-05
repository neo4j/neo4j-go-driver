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
			InitialBookmarks: map[string]neo4j.Bookmarks{
				"db1": {"a", "a", "b"},
				"db2": {"b", "c", "b"},
			},
		})

		bookmarks1 := bookmarkManager.GetBookmarks(ctx, "db1")
		expected1 := []string{"a", "b"}
		AssertEqualsInAnyOrder(t, bookmarks1, expected1)

		bookmarks2 := bookmarkManager.GetBookmarks(ctx, "db2")
		expected2 := []string{"b", "c"}
		AssertEqualsInAnyOrder(t, bookmarks2, expected2)
	})

	outer.Run("gets no bookmarks by default", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{})
		getBookmarks := func(db string) bool {
			return bookmarkManager.GetBookmarks(ctx, db) == nil
		}

		if err := quick.Check(getBookmarks, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("gets bookmarks along with user-supplied bookmarks", func(t *testing.T) {
		expectedBookmarks := neo4j.Bookmarks{"a", "b", "c"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a", "b"}},
			BookmarkSupplier: &simpleBookmarkSupplier{databaseBookmarks: func(db string) neo4j.Bookmarks {
				if db != "db1" {
					t.Errorf("expected to supply bookmarks for db1, but got %s", db)
				}
				return neo4j.Bookmarks{"b", "c"}
			}},
		})

		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")

		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("user-supplied bookmarks do not alter internal bookmarks", func(t *testing.T) {
		calls := 0
		expectedBookmarks := []string{"a"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a"}},
			BookmarkSupplier: &simpleBookmarkSupplier{databaseBookmarks: func(db string) neo4j.Bookmarks {
				defer func() {
					calls++
				}()
				if calls == 0 {
					return neo4j.Bookmarks{"b"}
				}
				return nil
			}},
		})

		_ = bookmarkManager.GetBookmarks(ctx, "db1")
		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")

		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("returned bookmarks are copies", func(t *testing.T) {
		expectedBookmarks := []string{"a"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a"}},
		})
		bookmarks := bookmarkManager.GetBookmarks(ctx, "db1")
		bookmarks[0] = "changed"

		bookmarks = bookmarkManager.GetBookmarks(ctx, "db1")

		AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
	})

	outer.Run("updates bookmarks", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{
				"db1": {"a", "b", "c"},
			},
		})

		bookmarkManager.UpdateBookmarks(ctx, "db1", []string{"b", "c"}, []string{"d", "a"})

		expectedBookmarks := []string{"a", "d"}
		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("notifies updated bookmarks for new DB", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) {
				notifyHookCalled = true
				if db != "db1" {
					t.Errorf("expected to receive notifications for DB db1 but received notifications for %s", db)
				}
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
			},
		})

		bookmarkManager.UpdateBookmarks(ctx, "db1", nil, []string{"d", "a"})

		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})

	outer.Run("does not notify updated bookmarks when empty", func(t *testing.T) {
		initialBookmarks := []string{"a", "b"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": initialBookmarks},
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) {
				t.Error("I must not be called")
			},
		})

		bookmarkManager.UpdateBookmarks(ctx, "db1", initialBookmarks, nil)

		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")
		AssertEqualsInAnyOrder(t, actualBookmarks, initialBookmarks)
	})

	outer.Run("notifies updated bookmarks for existing DB without bookmarks", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {}},
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) {
				notifyHookCalled = true
				if db != "db1" {
					t.Errorf("expected to receive notifications for DB db1 but received notifications for %s", db)
				}
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
			},
		})

		bookmarkManager.UpdateBookmarks(ctx, "db1", nil, []string{"d", "a"})

		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})

	outer.Run("notifies updated bookmarks for existing DB with previous bookmarks", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a", "b", "c"}},
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) {
				notifyHookCalled = true
				if db != "db1" {
					t.Errorf("expected to receive notifications for DB db1 but received notifications for %s", db)
				}
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
			},
		})

		bookmarkManager.UpdateBookmarks(ctx, "db1", []string{"b", "c"}, []string{"d", "a"})

		actualBookmarks := bookmarkManager.GetBookmarks(ctx, "db1")
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})

	outer.Run("forgets bookmarks of specified databases", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{
				"db":  {"z", "cooper"},
				"foo": {"bar", "fighters"},
				"par": {"rot", "don my French"},
			},
		})

		bookmarkManager.Forget(ctx, "db", "par")

		allBookmarks := bookmarkManager.GetAllBookmarks(ctx)
		AssertEqualsInAnyOrder(t, allBookmarks, []string{"bar", "fighters"})
		AssertIntEqual(t, len(bookmarkManager.GetBookmarks(ctx, "db")), 0)
		AssertEqualsInAnyOrder(t, bookmarkManager.GetBookmarks(ctx, "foo"),
			[]string{"bar", "fighters"})
		AssertIntEqual(t, len(bookmarkManager.GetBookmarks(ctx, "par")), 0)
	})

	outer.Run("can forget untracked databases", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{
				"db": {"z", "cooper"},
			},
		})

		bookmarkManager.Forget(ctx, "wat", "nope")

		allBookmarks := bookmarkManager.GetAllBookmarks(ctx)
		AssertEqualsInAnyOrder(t, allBookmarks, []string{"z", "cooper"})
		AssertEqualsInAnyOrder(t, bookmarkManager.GetBookmarks(ctx, "db"),
			[]string{"z", "cooper"})
		AssertIntEqual(t, len(bookmarkManager.GetBookmarks(ctx, "wat")), 0)
		AssertIntEqual(t, len(bookmarkManager.GetBookmarks(ctx, "nope")), 0)
	})
}

type simpleBookmarkSupplier struct {
	allBookmarks      func() neo4j.Bookmarks
	databaseBookmarks func(string) neo4j.Bookmarks
}

func (s *simpleBookmarkSupplier) GetAllBookmarks(context.Context) neo4j.Bookmarks {
	return s.allBookmarks()
}

func (s *simpleBookmarkSupplier) GetBookmarks(_ context.Context, db string) neo4j.Bookmarks {
	return s.databaseBookmarks(db)
}
