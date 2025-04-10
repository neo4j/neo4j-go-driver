//go:build internal_neo4j_testkit_gql_error

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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

func extrasGqlErrorFromNeo4jError(neo4jError *neo4j.Neo4jError) extrasGqlErrorInfo {
	return extrasGqlErrorInfo{
		gqlStatus:            neo4jError.GqlStatus,
		gqlStatusDescription: neo4jError.GqlStatusDescription,
		gqlClassification:    string(neo4jError.GqlClassification),
		gqlRawClassification: neo4jError.GqlRawClassification,
		gqlDiagnosticRecord:  serializeParameters(neo4jError.GqlDiagnosticRecord),
		cause:                extrasGqlErrorSerializeGqlErrorCause(neo4jError.GqlCause),
	}
}

func extrasGqlErrorSerializeGqlErrorCause(cause *db.Neo4jError) map[string]any {
	if cause == nil {
		return nil
	}
	return map[string]any{"name": "GqlError", "data": map[string]any{
		"msg":               cause.Msg,
		"gqlStatus":         cause.GqlStatus,
		"statusDescription": cause.GqlStatusDescription,
		"classification":    string(cause.GqlClassification),
		"rawClassification": emptyStringToNil(cause.GqlRawClassification),
		"diagnosticRecord":  serializeParameters(cause.GqlDiagnosticRecord),
		"cause":             extrasGqlErrorSerializeGqlErrorCause(cause.GqlCause),
	}}
}
