//go:build !internal_neo4j_testkit_no_bookmark_manager

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

package main

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const extrasBookmarkManager = "bookmarkManager"

func init() {
	registerExtra(
		extrasBookmarkManager,
		ExtrasRegisterEntry{
			newBackendExtraData: newExtrasBookmarkManagerExtraData,
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"NewBookmarkManager":         newBookmarkManagerHandler,
				"BookmarkManagerClose":       bookmarkManagerCloseHandler,
				"BookmarksSupplierCompleted": bookmarksSupplierCompletedHandler,
				"BookmarksConsumerCompleted": bookmarksConsumerCompletedHandler,
			},
			extraSessionConfigurer: extrasBookmarkManagerConfig,
		},
	)
}

type extrasBookmarkManagerExtraData struct {
	suppliedBookmarks map[string]neo4j.Bookmarks
	consumedBookmarks map[string]struct{}
	bookmarkManagers  map[string]neo4j.BookmarkManager
}

func newExtrasBookmarkManagerExtraData() any {
	return extrasBookmarkManagerExtraData{
		bookmarkManagers:  make(map[string]neo4j.BookmarkManager),
		suppliedBookmarks: make(map[string]neo4j.Bookmarks),
		consumedBookmarks: make(map[string]struct{}),
	}
}

func extrasBookmarkManagerGetBackendExtraData(backend *backend) extrasBookmarkManagerExtraData {
	return getBackendExtraData(backend, extrasBookmarkManager).(extrasBookmarkManagerExtraData)
}

func newBookmarkManagerHandler(backend *backend, data map[string]any) {
	extraData := extrasBookmarkManagerGetBackendExtraData(backend)
	bookmarkManagerId := backend.nextId()
	extraData.bookmarkManagers[bookmarkManagerId] = neo4j.NewBookmarkManager(
		backend.bookmarkManagerConfig(bookmarkManagerId, data))
	backend.writeResponse("BookmarkManager", map[string]any{
		"id": bookmarkManagerId,
	})
}

func bookmarkManagerCloseHandler(backend *backend, data map[string]any) {
	extraData := extrasBookmarkManagerGetBackendExtraData(backend)
	bookmarkManagerId := data["id"].(string)
	delete(extraData.bookmarkManagers, bookmarkManagerId)
	backend.writeResponse("BookmarkManager", map[string]any{
		"id": bookmarkManagerId,
	})
}

func bookmarksSupplierCompletedHandler(backend *backend, data map[string]any) {
	extraData := extrasBookmarkManagerGetBackendExtraData(backend)

	requestId := data["requestId"].(string)
	rawBookmarks := data["bookmarks"].([]any)
	bookmarks := make(neo4j.Bookmarks, len(rawBookmarks))
	for i, bookmark := range rawBookmarks {
		bookmarks[i] = bookmark.(string)
	}
	extraData.suppliedBookmarks[requestId] = bookmarks
}

func bookmarksConsumerCompletedHandler(backend *backend, data map[string]any) {
	extraData := extrasBookmarkManagerGetBackendExtraData(backend)

	requestId := data["requestId"].(string)
	extraData.consumedBookmarks[requestId] = struct{}{}
}

func extrasBookmarkManagerConfig(backend *backend, data map[string]any, config *neo4j.SessionConfig) error {
	if data["bookmarkManagerId"] != nil {
		extraData := extrasBookmarkManagerGetBackendExtraData(backend)
		bmmId := data["bookmarkManagerId"].(string)
		bookmarkManager := extraData.bookmarkManagers[bmmId]
		if bookmarkManager == nil {
			return fmt.Errorf("could not find bookmark manager with ID %s", bmmId)
		}
		config.BookmarkManager = bookmarkManager
	}
	return nil
}

func (b *backend) bookmarkManagerConfig(bookmarkManagerId string,
	config map[string]any) neo4j.BookmarkManagerConfig {

	var initialBookmarks neo4j.Bookmarks
	if config["initialBookmarks"] != nil {
		initialBookmarks = convertInitialBookmarks(config["initialBookmarks"].([]any))
	}
	result := neo4j.BookmarkManagerConfig{InitialBookmarks: initialBookmarks}
	supplierRegistered := config["bookmarksSupplierRegistered"]
	if supplierRegistered != nil && supplierRegistered.(bool) {
		result.BookmarkSupplier = b.supplyBookmarks(bookmarkManagerId)
	}
	consumerRegistered := config["bookmarksConsumerRegistered"]
	if consumerRegistered != nil && consumerRegistered.(bool) {
		result.BookmarkConsumer = b.consumeBookmarks(bookmarkManagerId)
	}
	return result
}

func (b *backend) supplyBookmarks(bookmarkManagerId string) func(context.Context) (neo4j.Bookmarks, error) {
	return func(ctx context.Context) (neo4j.Bookmarks, error) {
		extraData := extrasBookmarkManagerGetBackendExtraData(b)
		id := b.nextId()
		msg := map[string]any{"id": id, "bookmarkManagerId": bookmarkManagerId}
		b.writeResponse("BookmarksSupplierRequest", msg)
		b.process()
		return extraData.suppliedBookmarks[id], nil
	}
}

func (b *backend) consumeBookmarks(bookmarkManagerId string) func(context.Context, neo4j.Bookmarks) error {
	return func(_ context.Context, bookmarks neo4j.Bookmarks) error {
		extraData := extrasBookmarkManagerGetBackendExtraData(b)
		id := b.nextId()
		b.writeResponse("BookmarksConsumerRequest", map[string]any{
			"id":                id,
			"bookmarkManagerId": bookmarkManagerId,
			"bookmarks":         bookmarks,
		})
		for b.process() {
			if _, found := extraData.consumedBookmarks[id]; found {
				delete(extraData.consumedBookmarks, id)
				break
			}
		}
		return nil
	}
}

func convertInitialBookmarks(bookmarks []any) neo4j.Bookmarks {
	result := make(neo4j.Bookmarks, len(bookmarks))
	for i, bookmark := range bookmarks {
		result[i] = bookmark.(string)
	}
	return result
}
