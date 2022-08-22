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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collection"
	"sync"
)

// Bookmarks is a holder for server-side bookmarks which are used for causally-chained sessions.
// See also CombineBookmarks.
// Note: this will be changed from being a type alias to being a struct in 6.0. Please use BookmarksFromRawValues for construction
// from raw values and BookmarksToRawValues for accessing the raw values.
type Bookmarks = []string

type BookmarkManager interface {
	// UpdateBookmarks updates the bookmark for the specified database
	// previousBookmarks are the initial bookmarks of the bookmark holder (like a Session)
	// newBookmarks are the bookmarks that are received after completion of the bookmark holder operation (like the end of a Session)
	UpdateBookmarks(database string, previousBookmarks, newBookmarks Bookmarks)

	// GetAllBookmarks returns all the bookmarks tracked by this bookmark manager
	// Note: the order of the returned bookmark slice is not guaranteed
	GetAllBookmarks() Bookmarks

	// GetBookmarks returns all the bookmarks associated with the specified database
	// Note: the order of the returned bookmark slice does not need to be deterministic
	GetBookmarks(database string) Bookmarks

	// Forget removes all databases' bookmarks
	// Note: it is the driver user's responsibility to call this
	Forget(databases ...string)
}

type BookmarkManagerConfig struct {
	// Initial bookmarks per database
	InitialBookmarks map[string]Bookmarks

	// Supplier providing external bookmarks
	BookmarkSupplier BookmarkSupplier

	// Hook called whenever bookmarks for a given database get updated
	// The hook is called with the database and the new bookmarks
	// Note: the order of the supplied bookmark slice is not guaranteed
	BookmarkUpdateNotifier func(string, Bookmarks)
}

type BookmarkSupplier interface {
	// GetAllBookmarks returns all known bookmarks to the bookmark manager
	GetAllBookmarks() Bookmarks

	// GetBookmarks returns all the bookmarks of the specified database to the bookmark manager
	GetBookmarks(database string) Bookmarks
}

type bookmarkManager struct {
	bookmarks       *sync.Map
	supplier        BookmarkSupplier
	notifyUpdatesFn func(string, Bookmarks)
}

func (b *bookmarkManager) UpdateBookmarks(database string, previousBookmarks, newBookmarks Bookmarks) {
	if len(newBookmarks) == 0 {
		return
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
	if b.notifyUpdatesFn != nil {
		b.notifyUpdatesFn(database, bookmarksToNotify)
	}
}

func (b *bookmarkManager) GetAllBookmarks() Bookmarks {
	allBookmarks := collection.NewSet([]string{})
	if b.supplier != nil {
		allBookmarks.AddAll(b.supplier.GetAllBookmarks())
	}
	b.bookmarks.Range(func(db, rawBookmarks any) bool {
		bookmarks := rawBookmarks.(collection.Set[string]).Copy()
		allBookmarks.Union(bookmarks)
		return true
	})
	return allBookmarks.Values()
}

func (b *bookmarkManager) GetBookmarks(database string) Bookmarks {
	var extraBookmarks Bookmarks
	if b.supplier != nil {
		extraBookmarks = b.supplier.GetBookmarks(database)
	}
	rawBookmarks, found := b.bookmarks.Load(database)
	if !found {
		return extraBookmarks
	}
	bookmarks := rawBookmarks.(collection.Set[string]).Copy()
	if extraBookmarks == nil {
		return bookmarks.Values()
	}
	bookmarks.AddAll(extraBookmarks)
	return bookmarks.Values()
}

func (b *bookmarkManager) Forget(databases ...string) {
	for _, db := range databases {
		b.bookmarks.Delete(db)
	}
}

func NewBookmarkManager(config BookmarkManagerConfig) BookmarkManager {
	return &bookmarkManager{
		bookmarks:       initializeBookmarks(config.InitialBookmarks),
		supplier:        config.BookmarkSupplier,
		notifyUpdatesFn: config.BookmarkUpdateNotifier,
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
