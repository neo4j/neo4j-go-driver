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

		bookmarks1, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		expected1 := []string{"a", "b"}
		AssertEqualsInAnyOrder(t, bookmarks1, expected1)

		bookmarks2, err := bookmarkManager.GetBookmarks(ctx, "db2")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		expected2 := []string{"b", "c"}
		AssertEqualsInAnyOrder(t, bookmarks2, expected2)
	})

	outer.Run("gets no bookmarks by default", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{})
		getBookmarks := func(db string) bool {
			bookmarks, err := bookmarkManager.GetBookmarks(ctx, db)
			if err != nil {
				t.Errorf("expected nil error, got %v", err)
			}
			return bookmarks == nil
		}

		if err := quick.Check(getBookmarks, nil); err != nil {
			t.Error(err)
		}
	})

	outer.Run("gets bookmarks along with user-supplied bookmarks", func(t *testing.T) {
		expectedBookmarks := neo4j.Bookmarks{"a", "b", "c"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a", "b"}},
			BookmarkSupplier: &simpleBookmarkSupplier{databaseBookmarks: func(db string) (neo4j.Bookmarks, error) {
				if db != "db1" {
					t.Errorf("expected to supply bookmarks for db1, but got %s", db)
				}
				return neo4j.Bookmarks{"b", "c"}, nil
			}},
		})

		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("user-supplied bookmarks do not alter internal bookmarks", func(t *testing.T) {
		calls := 0
		expectedBookmarks := []string{"a"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a"}},
			BookmarkSupplier: &simpleBookmarkSupplier{databaseBookmarks: func(db string) (neo4j.Bookmarks, error) {
				defer func() {
					calls++
				}()
				if calls == 0 {
					return neo4j.Bookmarks{"b"}, nil
				}
				return nil, nil
			}},
		})

		_, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("returned bookmarks are copies", func(t *testing.T) {
		expectedBookmarks := []string{"a"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {"a"}},
		})
		bookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		bookmarks[0] = "changed"

		bookmarks, err = bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}

		AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
	})

	outer.Run("updates bookmarks", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{
				"db1": {"a", "b", "c"},
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, "db1", []string{"b", "c"}, []string{"d", "a"})

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		expectedBookmarks := []string{"a", "d"}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
	})

	outer.Run("notifies updated bookmarks for new DB", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) error {
				notifyHookCalled = true
				if db != "db1" {
					t.Errorf("expected to receive notifications for DB db1 but received notifications for %s", db)
				}
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, "db1", nil, []string{"d", "a"})

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, actualBookmarks, expectedBookmarks)
		if !notifyHookCalled {
			t.Errorf("notify hook should have been called")
		}
	})

	outer.Run("does not notify updated bookmarks when empty", func(t *testing.T) {
		initialBookmarks := []string{"a", "b"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": initialBookmarks},
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) error {
				t.Error("I must not be called")
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, "db1", initialBookmarks, nil)

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, actualBookmarks, initialBookmarks)
	})

	outer.Run("notifies updated bookmarks for existing DB without bookmarks", func(t *testing.T) {
		notifyHookCalled := false
		expectedBookmarks := []string{"a", "d"}
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{"db1": {}},
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) error {
				notifyHookCalled = true
				if db != "db1" {
					t.Errorf("expected to receive notifications for DB db1 but received notifications for %s", db)
				}
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, "db1", nil, []string{"d", "a"})

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
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
			BookmarkConsumerFn: func(_ context.Context, db string, bookmarks neo4j.Bookmarks) error {
				notifyHookCalled = true
				if db != "db1" {
					t.Errorf("expected to receive notifications for DB db1 but received notifications for %s", db)
				}
				AssertEqualsInAnyOrder(t, bookmarks, expectedBookmarks)
				return nil
			},
		})

		err := bookmarkManager.UpdateBookmarks(ctx, "db1", []string{"b", "c"}, []string{"d", "a"})

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		actualBookmarks, err := bookmarkManager.GetBookmarks(ctx, "db1")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
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

		err := bookmarkManager.Forget(ctx, "db", "par")

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		allBookmarks, err := bookmarkManager.GetAllBookmarks(ctx)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, allBookmarks, []string{"bar", "fighters"})
		bookmarks, err := bookmarkManager.GetBookmarks(ctx, "db")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertIntEqual(t, len(bookmarks), 0)
		bookmarks, err = bookmarkManager.GetBookmarks(ctx, "foo")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, bookmarks, []string{"bar", "fighters"})
		bookmarks, err = bookmarkManager.GetBookmarks(ctx, "par")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertIntEqual(t, len(bookmarks), 0)
	})

	outer.Run("can forget untracked databases", func(t *testing.T) {
		bookmarkManager := neo4j.NewBookmarkManager(neo4j.BookmarkManagerConfig{
			InitialBookmarks: map[string]neo4j.Bookmarks{
				"db": {"z", "cooper"},
			},
		})

		err := bookmarkManager.Forget(ctx, "wat", "nope")
		
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		allBookmarks, err := bookmarkManager.GetAllBookmarks(ctx)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, allBookmarks, []string{"z", "cooper"})
		bookmarks, err := bookmarkManager.GetBookmarks(ctx, "db")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertEqualsInAnyOrder(t, bookmarks, []string{"z", "cooper"})
		bookmarks, err = bookmarkManager.GetBookmarks(ctx, "wat")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertIntEqual(t, len(bookmarks), 0)
		bookmarks, err = bookmarkManager.GetBookmarks(ctx, "nope")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		AssertIntEqual(t, len(bookmarks), 0)
	})
}

type simpleBookmarkSupplier struct {
	allBookmarks      func() (neo4j.Bookmarks, error)
	databaseBookmarks func(string) (neo4j.Bookmarks, error)
}

func (s *simpleBookmarkSupplier) GetAllBookmarks(context.Context) (neo4j.Bookmarks, error) {
	return s.allBookmarks()
}

func (s *simpleBookmarkSupplier) GetBookmarks(_ context.Context, db string) (neo4j.Bookmarks, error) {
	return s.databaseBookmarks(db)
}
