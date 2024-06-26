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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

// Deprecated: please use NotificationClassification. This will be removed in 6.0.
type NotificationCategory string

// NotificationClassification is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type NotificationClassification string

// TODO do these need deprecated individually?
const (
	Hint         NotificationCategory = "HINT"
	Unrecognized NotificationCategory = "UNRECOGNIZED"
	Unsupported  NotificationCategory = "UNSUPPORTED"
	Performance  NotificationCategory = "PERFORMANCE"
	Deprecation  NotificationCategory = "DEPRECATION"
	Generic      NotificationCategory = "GENERIC"
	// Security requires server version 5.14 or newer.
	Security NotificationCategory = "SECURITY"
	// Topology requires server version 5.14 or newer.
	Topology NotificationCategory = "TOPOLOGY"
)

const (
	HintClassification         NotificationClassification = "HINT"
	UnrecognizedClassification NotificationClassification = "UNRECOGNIZED"
	UnsupportedClassification  NotificationClassification = "UNSUPPORTED"
	PerformanceClassification  NotificationClassification = "PERFORMANCE"
	DeprecationClassification  NotificationClassification = "DEPRECATION"
	GenericClassification      NotificationClassification = "GENERIC"
	// SecurityClassification requires server version 5.14 or newer.
	SecurityClassification NotificationClassification = "SECURITY"
	// TopologyClassification requires server version 5.14 or newer.
	TopologyClassification NotificationClassification = "TOPOLOGY"
	UnknownClassification  NotificationClassification = "UNKNOWN"
)

// Deprecated: please use notifications.NotificationDisabledClassifications. This will be removed in 6.0.
type NotificationDisabledCategories struct {
	categories []NotificationCategory
	none       bool
}

type NotificationDisabledClassifications struct {
	classifications []NotificationClassification
	none            bool
}

// Deprecated: please use DisableClassifications. This will be removed in 6.0.
//
// DisableCategories creates a NotificationDisabledCategories that disables the given categories.
// Can be used for NotificationDisabledClassifications of config.Config and config.SessionConfig.
func DisableCategories(value ...NotificationCategory) NotificationDisabledCategories {
	return NotificationDisabledCategories{value, false}
}

// DisableClassifications creates a NotificationDisabledClassifications that disables the given classifications.
// Can be used for NotificationsDisabledClassifications of config.Config and config.SessionConfig.
func DisableClassifications(value ...NotificationClassification) NotificationDisabledClassifications {
	return NotificationDisabledClassifications{value, false}
}

// Deprecated: please use DisableNoClassifications. This will be removed in 6.0.
//
// DisableNoCategories creates a NotificationDisabledCategories that enables all categories.
// Can be used for NotificationsDisabledCategories of neo4j.Config and neo4j.SessionConfig.
func DisableNoCategories() NotificationDisabledCategories {
	return NotificationDisabledCategories{nil, true}
}

// DisableNoClassifications creates a NotificationDisabledClassifications that enables all classifications.
// Can be used for NotificationsDisabledClassifications of neo4j.Config and neo4j.SessionConfig.
func DisableNoClassifications() NotificationDisabledClassifications {
	return NotificationDisabledClassifications{nil, true}
}

// Deprecated: please use NotificationDisabledClassifications.DisablesNone. This will be removed in 6.0.
//
// DisablesNone returns true if all categories are enabled.
func (n *NotificationDisabledCategories) DisablesNone() bool {
	return n.none
}

// DisablesNone returns true if all categories are enabled.
func (n *NotificationDisabledClassifications) DisablesNone() bool {
	return n.none
}

// Deprecated: please use NotificationDisabledClassifications.DisabledClassifications. This will be removed in 6.0.
//
// DisabledCategories returns the categories that are disabled.
func (n *NotificationDisabledCategories) DisabledCategories() []NotificationCategory {
	return n.categories
}

// DisabledClassifications returns the classifications that are disabled.
func (n *NotificationDisabledClassifications) DisabledClassifications() []NotificationClassification {
	return n.classifications
}

// NotificationMinimumSeverityLevel defines the minimum severity level of notifications the server should send.
// Can be used for NotificationsMinSeverity of config.Config and config.SessionConfig.
type NotificationMinimumSeverityLevel string

const (
	DefaultLevel     NotificationMinimumSeverityLevel = ""
	DisabledLevel    NotificationMinimumSeverityLevel = "OFF"
	WarningLevel     NotificationMinimumSeverityLevel = "WARNING"
	InformationLevel NotificationMinimumSeverityLevel = "INFORMATION"
)

// ToNotification returns a db.Notification that corresponds to the given db.GqlStatusObject.
// It maps fields from the status to their respective notification fields.
func ToNotification(gqlStatusObject db.GqlStatusObject) *db.Notification {
	return &db.Notification{
		Code:        gqlStatusObject.Code,
		Title:       gqlStatusObject.Title,
		Description: gqlStatusObject.StatusDescription,
		Position:    gqlStatusObject.Position,
		Severity:    gqlStatusObject.Severity,
		Category:    gqlStatusObject.Classification,
	}
}

var (
	defaultDiagnosticRecord = map[string]any{
		"OPERATION":      "",
		"OPERATION_CODE": "0",
		"CURRENT_SCHEMA": "/",
	}

	successGqlStatusObject = &db.GqlStatusObject{
		GqlStatus:         "00000",
		StatusDescription: "note: successful completion",
		DiagnosticRecord:  defaultDiagnosticRecord,
	}

	noDataGqlStatusObject = &db.GqlStatusObject{
		GqlStatus:         "02000",
		StatusDescription: "note: no data",
		DiagnosticRecord:  defaultDiagnosticRecord,
	}

	noDataUnknownGqlStatusObject = &db.GqlStatusObject{
		GqlStatus:         "02N42",
		StatusDescription: "note: no data - unknown subcondition",
		DiagnosticRecord:  defaultDiagnosticRecord,
	}

	omittedResultGqlStatusObject = &db.GqlStatusObject{
		GqlStatus:         "00001",
		StatusDescription: "note: successful completion - omitted result",
		DiagnosticRecord:  defaultDiagnosticRecord,
	}

	unknownWarningResultGqlStatusObject = &db.GqlStatusObject{
		GqlStatus:         "01N42",
		StatusDescription: "warn: unknown warning",
		DiagnosticRecord:  defaultDiagnosticRecord,
	}

	unknownInformationResultGqlStatusObject = &db.GqlStatusObject{
		GqlStatus:         "03N42",
		StatusDescription: "info: unknown notification",
		DiagnosticRecord:  defaultDiagnosticRecord,
	}
)

// ToGqlStatusObject returns a db.GqlStatusObject that corresponds to the given db.Notification.
// It maps fields from the notification to their respective status fields.
func ToGqlStatusObject(notification db.Notification) *db.GqlStatusObject {
	defaultStatus := unknownInformationResultGqlStatusObject
	if notification.Severity == string(WarningLevel) {
		defaultStatus = unknownWarningResultGqlStatusObject
	}

	statusDescription := notification.Description
	if statusDescription == "" {
		statusDescription = defaultStatus.StatusDescription
	}

	diagnosticRecord := map[string]any{}
	for k, v := range defaultDiagnosticRecord {
		diagnosticRecord[k] = v
	}

	if notification.Position != nil {
		diagnosticRecord["_position"] = notification.Position
	}
	diagnosticRecord["_classification"] = notification.Category
	diagnosticRecord["_severity"] = notification.Severity

	return &db.GqlStatusObject{
		Code:              notification.Code,
		Title:             notification.Title,
		GqlStatus:         defaultStatus.GqlStatus,
		StatusDescription: statusDescription,
		Position:          notification.Position,
		Classification:    notification.Category,
		Severity:          notification.Severity,
		DiagnosticRecord:  diagnosticRecord,
		IsNotification:    true,
	}
}

func ToGqlStatusObjectFromSummary(summary *db.Summary) *db.GqlStatusObject {
	// TODO return successGqlStatusObject, noDataGqlStatusObject, noDataUnknownGqlStatusObject or omittedResultGqlStatusObject
	return successGqlStatusObject
}
