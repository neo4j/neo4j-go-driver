//go:build !internal_neo4j_testkit_no_time_mock

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
	"encoding/json"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const extrasNameTimeMock = "timeMock"

func init() {
	registerExtra(
		extrasNameTimeMock,
		ExtrasRegisterEntry{
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"FakeTimeInstall":   fakeTimeInstallHandler,
				"FakeTimeUninstall": fakeTimeUninstallHandler,
				"FakeTimeTick":      fakeTimeTickHandler,
			},
		},
	)
}

func fakeTimeInstallHandler(backend *backend, data map[string]any) {
	if err := neo4j.FreezeTime(); err != nil {
		backend.writeError(err)
		return
	}
	backend.writeResponse("FakeTimeAck", nil)
}

func fakeTimeUninstallHandler(backend *backend, data map[string]any) {
	if err := neo4j.UnfreezeTime(); err != nil {
		backend.writeError(err)
		return
	}
	backend.writeResponse("FakeTimeAck", nil)
}

func fakeTimeTickHandler(backend *backend, data map[string]any) {
	milliseconds := asInt64(data["incrementMs"].(json.Number))
	if err := neo4j.TickTime(time.Duration(milliseconds) * time.Millisecond); err != nil {
		backend.writeError(err)
		return
	}
	backend.writeResponse("FakeTimeAck", nil)
}

var Now = neo4j.Now
