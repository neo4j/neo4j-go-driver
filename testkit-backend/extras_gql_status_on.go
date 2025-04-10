//go:build internal_neo4j_testkit_gql_status

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

func serializeGqlStatusObjects(summary neo4j.ResultSummary) []map[string]any {
	statusObjects := summary.GqlStatusObjects()
	var res []map[string]any
	for i, status := range statusObjects {
		res = append(res, map[string]any{
			"isNotification":    status.IsNotification(),
			"gqlStatus":         status.GqlStatus(),
			"statusDescription": status.StatusDescription(),
			"rawClassification": emptyStringToNil(status.RawClassification()),
			"classification":    string(status.Classification()),
			"rawSeverity":       emptyStringToNil(status.RawSeverity()),
			"severity":          string(status.Severity()),
			"diagnosticRecord":  serializeParameters(status.DiagnosticRecord()),
		})
		if status.Position() != nil {
			res[i]["position"] = map[string]any{
				"offset": status.Position().Offset(),
				"line":   status.Position().Line(),
				"column": status.Position().Column(),
			}
		} else {
			res[i]["position"] = nil
		}
	}
	return res
}
