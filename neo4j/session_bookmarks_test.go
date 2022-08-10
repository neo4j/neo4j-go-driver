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
	"reflect"
	"testing"
)

func TestSessionBookmarks(outer *testing.T) {
	outer.Parallel()

	outer.Run("initial set bookmarks are cleaned up", func(t *testing.T) {
		sessionBookmarks := newSessionBookmarks([]string{
			"", "bookmark", "", "deutschmark", "",
		})
		expectedBookmarks := []string{"bookmark", "deutschmark"}

		currentBookmarks := sessionBookmarks.currentBookmarks()

		if !reflect.DeepEqual(currentBookmarks, expectedBookmarks) {
			t.Errorf("expected bookmarks %v, got %v", expectedBookmarks, currentBookmarks)
		}
		lastBookmark := sessionBookmarks.lastBookmark()
		if lastBookmark != "deutschmark" {
			t.Errorf(`expected last bookmark "deutschmark", but got %q`, lastBookmark)
		}
	})

	outer.Run("replaces set bookmarks with new one", func(t *testing.T) {
		sessionBookmarks := newSessionBookmarks([]string{"book marking"})

		sessionBookmarks.replaceBookmarks("booking mark")

		currentBookmarks := sessionBookmarks.currentBookmarks()
		if !reflect.DeepEqual(currentBookmarks, []string{"booking mark"}) {
			t.Errorf(`expected bookmarks ["booking mark"], got %v`, currentBookmarks)
		}
		lastBookmark := sessionBookmarks.lastBookmark()
		if lastBookmark != "booking mark" {
			t.Errorf(`expected last bookmark "booking mark", but got %q`, lastBookmark)
		}
	})

	outer.Run("does not replace set bookmarks when new bookmark is empty", func(t *testing.T) {
		sessionBookmarks := newSessionBookmarks([]string{"book marking"})

		sessionBookmarks.replaceBookmarks("")

		currentBookmarks := sessionBookmarks.currentBookmarks()
		if !reflect.DeepEqual(currentBookmarks, []string{"book marking"}) {
			t.Errorf(`expected bookmarks ["book marking"], got %v`, currentBookmarks)
		}
		lastBookmark := sessionBookmarks.lastBookmark()
		if lastBookmark != "book marking" {
			t.Errorf(`expected last bookmark "book marking", but got %q`, lastBookmark)
		}
	})

	outer.Run("last bookmark returns empty string when no bookmarks were previously set", func(t *testing.T) {
		sessionBookmarks := newSessionBookmarks(nil)

		lastBookmark := sessionBookmarks.lastBookmark()

		if lastBookmark != "" {
			t.Errorf(`expected empty last bookmark, but got %q`, lastBookmark)
		}
	})
}
