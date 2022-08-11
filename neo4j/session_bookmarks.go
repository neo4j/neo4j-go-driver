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

type sessionBookmarks struct {
	bookmarks Bookmarks
}

func newSessionBookmarks(bookmarks Bookmarks) *sessionBookmarks {
	return &sessionBookmarks{
		bookmarks: cleanupBookmarks(bookmarks),
	}
}

func (sb *sessionBookmarks) currentBookmarks() Bookmarks {
	return sb.bookmarks
}

func (sb *sessionBookmarks) lastBookmark() string {
	bookmarks := sb.currentBookmarks()
	count := len(bookmarks)
	if count == 0 {
		return ""
	}
	return bookmarks[count-1]
}

func (sb *sessionBookmarks) replaceBookmarks(bookmark string) {
	if len(bookmark) == 0 {
		return
	}
	sb.bookmarks = []string{bookmark}
}

// Remove empty string bookmarks to check for "bad" callers
// To avoid allocating, first check if this is a problem
func cleanupBookmarks(bookmarks Bookmarks) Bookmarks {
	hasBad := false
	for _, b := range bookmarks {
		if len(b) == 0 {
			hasBad = true
			break
		}
	}

	if !hasBad {
		return bookmarks
	}

	cleaned := make(Bookmarks, 0, len(bookmarks)-1)
	for _, b := range bookmarks {
		if len(b) > 0 {
			cleaned = append(cleaned, b)
		}
	}
	return cleaned
}
