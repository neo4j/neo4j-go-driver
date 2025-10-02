//go:build !internal_neo4j_testkit_no_rt_helper

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

import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

const extrasNameRtHelper = "rtHelper"

func init() {
	registerExtra(
		extrasNameRtHelper,
		ExtrasRegisterEntry{
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"ForcedRoutingTableUpdate": forcedRoutingTableUpdateHandler,
				"GetRoutingTable":          getRoutingTableHandler,
			},
		},
	)
}

func forcedRoutingTableUpdateHandler(backend *backend, data map[string]any) {
	databaseRaw := data["database"]
	var database string
	if databaseRaw != nil {
		database = databaseRaw.(string)
	}
	var bookmarks []string
	bookmarksRaw := data["bookmarks"]
	if bookmarksRaw != nil {
		bookmarksSlice := bookmarksRaw.([]any)
		bookmarks = make([]string, len(bookmarksSlice))
		for i, bookmark := range bookmarksSlice {
			bookmarks[i] = bookmark.(string)
		}
	}
	driverId := data["driverId"].(string)
	driver := backend.drivers[driverId]
	err := neo4j.ForceRoutingTableUpdate(driver, database, bookmarks, &streamLog{writeLine: backend.writeLineLocked})
	if err != nil {
		backend.writeError(err)
		return
	}
	backend.writeResponse("Driver", map[string]any{"id": driverId})
}

func getRoutingTableHandler(backend *backend, data map[string]any) {
	driver := backend.drivers[data["driverId"].(string)]
	databaseRaw := data["database"]
	var database string
	if databaseRaw != nil {
		database = databaseRaw.(string)
	}
	table, err := neo4j.GetRoutingTable(driver, database)
	if err != nil {
		backend.writeError(err)
		return
	}
	var databaseName any = table.DatabaseName
	if databaseName == "" {
		databaseName = nil
	}
	backend.writeResponse("RoutingTable", map[string]any{
		"database": databaseName,
		"ttl":      table.TimeToLive,
		"routers":  table.Routers,
		"readers":  table.Readers,
		"writers":  table.Writers,
	})
}
