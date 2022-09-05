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

package neo4j

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collection"
	"sync"
)

// Bookmarks is a holder for server-side bookmarks which are used for causally-chained sessions.
// See also CombineBookmarks.
// Note: this will be changed from being a type alias to being a struct in 6.0. Please use BookmarksFromRawValues for construction
// from raw values and BookmarksToRawValues for accessing the raw values.
type Bookmarks = []string

// BookmarkManager centralizes bookmark manager supply and notification
// This API is experimental and may be changed or removed without prior notice
type BookmarkManager interface {
	// UpdateBookmarks updates the bookmark for the specified database
	// previousBookmarks are the initial bookmarks of the bookmark holder (like a Session)
	// newBookmarks are the bookmarks that are received after completion of the bookmark holder operation (like the end of a Session)
	UpdateBookmarks(ctx context.Context, database string, previousBookmarks, newBookmarks Bookmarks) error

	// GetAllBookmarks returns all the bookmarks tracked by this bookmark manager
	// Note: the order of the returned bookmark slice is not guaranteed
	GetAllBookmarks(ctx context.Context) (Bookmarks, error)

	// GetBookmarks returns all the bookmarks associated with the specified database
	// Note: the order of the returned bookmark slice does not need to be deterministic
	GetBookmarks(ctx context.Context, database string) (Bookmarks, error)

	// Forget removes all databases' bookmarks
	// Note: it is the driver user's responsibility to call this
	Forget(ctx context.Context, databases ...string) error
}

// BookmarkManagerConfig is an experimental API and may be changed or removed
// without prior notice
type BookmarkManagerConfig struct {
	// Initial bookmarks per database
	InitialBookmarks map[string]Bookmarks

	// Supplier providing external bookmarks
	BookmarkSupplier BookmarkSupplier

	// Hook called whenever bookmarks for a given database get updated
	// The hook is called with the database and the new bookmarks
	// Note: the order of the supplied bookmark slice is not guaranteed
	BookmarkConsumer func(ctx context.Context, database string, bookmarks Bookmarks) error
}

type BookmarkSupplier interface {
	// GetAllBookmarks returns all known bookmarks to the bookmark manager
	GetAllBookmarks(ctx context.Context) (Bookmarks, error)

	// GetBookmarks returns all the bookmarks of the specified database to the bookmark manager
	GetBookmarks(ctx context.Context, database string) (Bookmarks, error)
}

type bookmarkManager struct {
	bookmarks *sync.Map
	supplier  BookmarkSupplier
	consumer  func(context.Context, string, Bookmarks) error
}

func (b *bookmarkManager) UpdateBookmarks(ctx context.Context, database string, previousBookmarks, newBookmarks Bookmarks) error {
	if len(newBookmarks) == 0 {
		return nil
	}
	var bookmarksToNotify Bookmarks
	storedNewBookmarks := collection.NewSet(newBookmarks)
	if rawCurrentBookmarks, loaded := b.bookmarks.LoadOrStore(database, storedNewBookmarks); !loaded {
		bookmarksToNotify = storedNewBookmarks.Values()
	} else {
		currentBookmarks := (rawCurrentBookmarks).(collection.Set[string])
		currentBookmarks.RemoveAll(previousBookmarks)
		currentBookmarks.AddAll(newBookmarks)
		bookmarksToNotify = currentBookmarks.Values()
	}
	if b.consumer != nil {
		return b.consumer(ctx, database, bookmarksToNotify)
	}
	return nil
}

func (b *bookmarkManager) GetAllBookmarks(ctx context.Context) (Bookmarks, error) {
	allBookmarks := collection.NewSet([]string{})
	if b.supplier != nil {
		bookmarks, err := b.supplier.GetAllBookmarks(ctx)
		if err != nil {
			return nil, err
		}
		allBookmarks.AddAll(bookmarks)
	}
	b.bookmarks.Range(func(db, rawBookmarks any) bool {
		bookmarks := rawBookmarks.(collection.Set[string])
		allBookmarks.Union(bookmarks)
		return true
	})
	return allBookmarks.Values(), nil
}

func (b *bookmarkManager) GetBookmarks(ctx context.Context, database string) (Bookmarks, error) {
	var extraBookmarks Bookmarks
	if b.supplier != nil {
		bookmarks, err := b.supplier.GetBookmarks(ctx, database)
		if err != nil {
			return nil, err
		}
		extraBookmarks = bookmarks
	}
	rawBookmarks, found := b.bookmarks.Load(database)
	if !found {
		return extraBookmarks, nil
	}
	bookmarks := rawBookmarks.(collection.Set[string]).Copy()
	if extraBookmarks == nil {
		return bookmarks.Values(), nil
	}
	bookmarks.AddAll(extraBookmarks)
	return bookmarks.Values(), nil
}

func (b *bookmarkManager) Forget(ctx context.Context, databases ...string) error {
	for _, db := range databases {
		b.bookmarks.Delete(db)
	}
	return nil
}

func NewBookmarkManager(config BookmarkManagerConfig) BookmarkManager {
	return &bookmarkManager{
		bookmarks: initializeBookmarks(config.InitialBookmarks),
		supplier:  config.BookmarkSupplier,
		consumer:  config.BookmarkConsumer,
	}
}

func initializeBookmarks(allBookmarks map[string]Bookmarks) *sync.Map {
	var initialBookmarks sync.Map
	for db, bookmarks := range allBookmarks {
		initialBookmarks.Store(db, collection.NewSet(bookmarks))
	}
	return &initialBookmarks
}

// CombineBookmarks is a helper method to combine []Bookmarks into a single Bookmarks instance.
// Let s1, s2, s3 be Session interfaces. You can easily causally chain the sessions like so:
// ```go
//
//	s4 := driver.NewSession(neo4j.SessionConfig{
//		Bookmarks: neo4j.CombineBookmarks(s1.LastBookmarks(), s2.LastBookmarks(), s3.LastBookmarks()),
//	})
//
// ```
// The server will then make sure to execute all transactions in s4 after any that were already executed in s1, s2, or s3
// at the time of calling LastBookmarks.
func CombineBookmarks(bookmarks ...Bookmarks) Bookmarks {
	var lenSum int
	for _, b := range bookmarks {
		lenSum += len(b)
	}
	res := make([]string, lenSum)
	var i int
	for _, b := range bookmarks {
		i += copy(res[i:], b)
	}
	return res
}

// BookmarksToRawValues exposes the raw server-side bookmarks.
// You should not need to use this method unless you want to serialize bookmarks.
// See Session.LastBookmarks and CombineBookmarks for alternatives.
func BookmarksToRawValues(bookmarks Bookmarks) []string {
	return bookmarks
}

// BookmarksFromRawValues creates Bookmarks from raw server-side bookmarks.
// You should not need to use this method unless you want to de-serialize bookmarks.
// See Session.LastBookmarks and CombineBookmarks for alternatives.
func BookmarksFromRawValues(values ...string) Bookmarks {
	return values
}
