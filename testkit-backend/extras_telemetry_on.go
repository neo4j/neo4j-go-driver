//go:build !internal_neo4j_testkit_no_telemetry

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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

const extrasNameTelemetry = "telemetry"

func init() {
	registerExtra(
		extrasNameTelemetry,
		ExtrasRegisterEntry{
			extraDriverConfig: extrasTelemetry,
		},
	)
}

func extrasTelemetry(backend *backend, data map[string]any, config *config.Config) error {
	if data["telemetryDisabled"] != nil {
		config.TelemetryDisabled = data["telemetryDisabled"].(bool)
	}
	return nil
}
