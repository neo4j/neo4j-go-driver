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
	sessionBookmarks := newSessionBookmarks([]string{
		"", "bookmark", "", "deutschmark", "",
	})

	outer.Run("initial set bookmarks are cleaned up", func(t *testing.T) {
		expectedBookmarks := []string{"bookmark", "deutschmark"}

		currentBookmarks := sessionBookmarks.currentBookmarks()

		if !reflect.DeepEqual(currentBookmarks, expectedBookmarks) {
			t.Errorf("expected bookmarks %v, got %v", expectedBookmarks, currentBookmarks)
		}
		lastBookmark := sessionBookmarks.lastBookmark()
		if lastBookmark != "deutschmark" {
			t.Errorf(`expected last bookmark "deutschmark"", but got %q`, lastBookmark)
		}
	})

	outer.Run("replaces set bookmarks with new non-empty one", func(t *testing.T) {
		localSessionBookmarks := *sessionBookmarks

		localSessionBookmarks.replaceBookmarks("booking mark")

		currentBookmarks := localSessionBookmarks.currentBookmarks()
		if !reflect.DeepEqual(currentBookmarks, []string{"booking mark"}) {
			t.Errorf(`expected bookmarks ["booking mark"], got %v`, currentBookmarks)
		}
		lastBookmark := localSessionBookmarks.lastBookmark()
		if lastBookmark != "booking mark" {
			t.Errorf(`expected last bookmark "booking mark"", but got %q`, lastBookmark)
		}
	})
}
