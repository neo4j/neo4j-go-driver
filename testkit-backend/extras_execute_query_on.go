//go:build !internal_neo4j_testkit_no_execute_query

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
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const extrasExecuteQuery = "executeQuery"

func init() {
	registerExtra(
		extrasExecuteQuery,
		ExtrasRegisterEntry{
			extraRequestHandlers: map[string]extrasRequestHandlerFunc{
				"ExecuteQuery": executeQueryHandler,
			},
		},
	)
}

type ExecuteQueryConfiguration = neo4j.ExecuteQueryConfiguration

func executeQueryHandler(backend *backend, data map[string]any) {
	driver := backend.drivers[data["driverId"].(string)]
	var configurers []neo4j.ExecuteQueryConfigurationOption
	if rawConfig := data["config"]; rawConfig != nil {
		executeQueryConfig := rawConfig.(map[string]any)
		configurers = append(configurers, func(config *neo4j.ExecuteQueryConfiguration) {
			routing := executeQueryConfig["routing"]
			if routing != nil {
				switch routing {
				case "r":
					config.Routing = neo4jRead()
				case "w":
					config.Routing = neo4jWrite()
				default:
					backend.writeError(fmt.Errorf("unexpected executequery routing value: %v", routing))
					return
				}
			}
			impersonatedUser := executeQueryConfig["impersonatedUser"]
			if impersonatedUser != nil {
				config.ImpersonatedUser = impersonatedUser.(string)
			}
			database := executeQueryConfig["database"]
			if database != nil {
				config.Database = database.(string)
			}
			bookmarkManagerId := executeQueryConfig["bookmarkManagerId"]
			if bookmarkManagerId != nil {
				if number, ok := bookmarkManagerId.(json.Number); ok {
					id := number.String()
					if id != "-1" {
						backend.writeError(fmt.Errorf("unexpected bookmark manager id: %s", id))
						return
					}
					config.BookmarkManager = nil
				} else {
					extraDataBmm := extrasBookmarkManagerGetBackendExtraData(backend)
					config.BookmarkManager = extraDataBmm.bookmarkManagers[bookmarkManagerId.(string)]
				}
			}

			for _, configurer := range extrasExecuteQueryConfigurers {
				err := configurer(backend, data, config)
				if err != nil {
					backend.writeError(err)
					return
				}
			}
		})
	}

	cypher, params, err := backend.toCypherAndParams(data)
	if err != nil {
		backend.writeError(err)
		return
	}
	eagerResult, err := neo4j.ExecuteQuery[*neo4j.EagerResult](
		ctx, driver, cypher, params, neo4j.EagerResultTransformer, configurers...)
	if err != nil {
		backend.writeError(err)
		return
	}
	backend.writeResponse("EagerResult", map[string]any{
		"keys":    eagerResult.Keys,
		"records": serializeRecords(eagerResult.Records),
		"summary": serializeSummary(eagerResult.Summary),
	})
}
