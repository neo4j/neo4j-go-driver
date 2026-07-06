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

package notifications

import (
	idb "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/gql"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/notifications"
)

func newSuccessGqlStatusObject() *idb.GqlStatusObject {
	return &idb.GqlStatusObject{
		GqlStatus:         "00000",
		StatusDescription: "note: successful completion",
		DiagnosticRecord:  gql.NewDefaultDiagnosticRecord(),
	}
}

func newNoDataGqlStatusObject() *idb.GqlStatusObject {
	return &idb.GqlStatusObject{
		GqlStatus:         "02000",
		StatusDescription: "note: no data",
		DiagnosticRecord:  gql.NewDefaultDiagnosticRecord(),
	}
}

func newOmittedResultGqlStatusObject() *idb.GqlStatusObject {
	return &idb.GqlStatusObject{
		GqlStatus:         "00001",
		StatusDescription: "note: successful completion - omitted result",
		DiagnosticRecord:  gql.NewDefaultDiagnosticRecord(),
	}
}

func newUnknownWarningResultGqlStatusObject() *idb.GqlStatusObject {
	return &idb.GqlStatusObject{
		GqlStatus:         "01N42",
		StatusDescription: "warn: unknown warning",
		DiagnosticRecord:  gql.NewDefaultDiagnosticRecord(),
	}
}

func newUnknownInformationResultGqlStatusObject() *idb.GqlStatusObject {
	return &idb.GqlStatusObject{
		GqlStatus:         "03N42",
		StatusDescription: "info: unknown notification",
		DiagnosticRecord:  gql.NewDefaultDiagnosticRecord(),
	}
}

// ToNotification returns a idb.Notification that corresponds to the given idb.GqlStatusObject.
// It maps fields from the status to their respective notification fields.
func ToNotification(gqlStatusObject idb.GqlStatusObject) *idb.Notification {
	return &idb.Notification{
		Code:        gqlStatusObject.Code,
		Title:       gqlStatusObject.Title,
		Description: gqlStatusObject.Description,
		Position:    gqlStatusObject.Position,
		Severity:    gqlStatusObject.Severity,
		Category:    gqlStatusObject.Classification,
	}
}

// ToGqlStatusObject returns a idb.GqlStatusObject that corresponds to the given idb.Notification.
// It maps fields from the notification to their respective status fields.
func ToGqlStatusObject(notification idb.Notification) *idb.GqlStatusObject {
	var defaultStatus *idb.GqlStatusObject
	if notification.Severity == string(notifications.Warning) {
		defaultStatus = newUnknownWarningResultGqlStatusObject()
	} else {
		defaultStatus = newUnknownInformationResultGqlStatusObject()
	}

	statusDescription := notification.Description
	if statusDescription == "" {
		statusDescription = defaultStatus.StatusDescription
	}

	diagnosticRecord := gql.NewDefaultDiagnosticRecord()

	if notification.Position != nil {
		diagnosticRecord["_position"] = map[string]any{
			"offset": notification.Position.Offset,
			"line":   notification.Position.Line,
			"column": notification.Position.Column,
		}
	}
	if notification.Severity != "" {
		diagnosticRecord["_severity"] = notification.Severity
	}
	if notification.Category != "" {
		diagnosticRecord["_classification"] = notification.Category
	}

	return &idb.GqlStatusObject{
		Code:              notification.Code,
		Title:             notification.Title,
		Description:       notification.Description,
		GqlStatus:         defaultStatus.GqlStatus,
		StatusDescription: statusDescription,
		Position:          notification.Position,
		Classification:    notification.Category,
		Severity:          notification.Severity,
		DiagnosticRecord:  diagnosticRecord,
		IsNotification:    true,
	}
}

// ToGqlStatusObjectFromSummary creates a new idb.GqlStatusObject based on the context of the db.StreamSummary.
func ToGqlStatusObjectFromSummary(summary idb.StreamSummary) *idb.GqlStatusObject {
	if summary.HadRecord {
		return newSuccessGqlStatusObject()
	} else if summary.HadKey {
		return newNoDataGqlStatusObject()
	} else {
		return newOmittedResultGqlStatusObject()
	}
}
