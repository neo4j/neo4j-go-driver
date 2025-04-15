//go:build internal_neo4j_testkit_no_gql_status

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

const extrasNameGqlStatus = "gqlStatus"

func init() {
	registerExtra(
		extrasNameGqlStatus,
		ExtrasRegisterEntry{
			extraBlockedTestKitFeatures: []string{
				"Feature:API:Summary:GqlStatusObjects",
				"Feature:Bolt:5.5",
				"Feature:Bolt:5.6",
			},
		},
	)
}

func serializeGqlStatusObjects(summary neo4j.ResultSummary) []map[string]any {
	return make([]map[string]any, 0)
}
