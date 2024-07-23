package notifications

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

func newDefaultDiagnosticRecord() map[string]any {
	return map[string]any{
		"OPERATION":      "",
		"OPERATION_CODE": "0",
		"CURRENT_SCHEMA": "/",
	}
}

func newSuccessGqlStatusObject() *db.GqlStatusObject {
	return &db.GqlStatusObject{
		GqlStatus:         "00000",
		StatusDescription: "note: successful completion",
		DiagnosticRecord:  newDefaultDiagnosticRecord(),
	}
}

func newNoDataGqlStatusObject() *db.GqlStatusObject {
	return &db.GqlStatusObject{
		GqlStatus:         "02000",
		StatusDescription: "note: no data",
		DiagnosticRecord:  newDefaultDiagnosticRecord(),
	}
}

func newOmittedResultGqlStatusObject() *db.GqlStatusObject {
	return &db.GqlStatusObject{
		GqlStatus:         "00001",
		StatusDescription: "note: successful completion - omitted result",
		DiagnosticRecord:  newDefaultDiagnosticRecord(),
	}
}

func newUnknownWarningResultGqlStatusObject() *db.GqlStatusObject {
	return &db.GqlStatusObject{
		GqlStatus:         "01N42",
		StatusDescription: "warn: unknown warning",
		DiagnosticRecord:  newDefaultDiagnosticRecord(),
	}
}

func newUnknownInformationResultGqlStatusObject() *db.GqlStatusObject {
	return &db.GqlStatusObject{
		GqlStatus:         "03N42",
		StatusDescription: "info: unknown notification",
		DiagnosticRecord:  newDefaultDiagnosticRecord(),
	}
}

// ToNotification returns a db.Notification that corresponds to the given db.GqlStatusObject.
// It maps fields from the status to their respective notification fields.
func ToNotification(gqlStatusObject db.GqlStatusObject) *db.Notification {
	return &db.Notification{
		//lint:ignore SA1019 Code is supported at least until 6.0
		Code: gqlStatusObject.Code,
		//lint:ignore SA1019 Title is supported at least until 6.0
		Title: gqlStatusObject.Title,
		//lint:ignore SA1019 Description is supported at least until 6.0
		Description: gqlStatusObject.Description,
		Position:    gqlStatusObject.Position,
		Severity:    gqlStatusObject.Severity,
		Category:    gqlStatusObject.Classification,
	}
}

// ToGqlStatusObject returns a db.GqlStatusObject that corresponds to the given db.Notification.
// It maps fields from the notification to their respective status fields.
func ToGqlStatusObject(notification db.Notification) *db.GqlStatusObject {
	var defaultStatus *db.GqlStatusObject
	if notification.Severity == string(notifications.Warning) {
		defaultStatus = newUnknownWarningResultGqlStatusObject()
	} else {
		defaultStatus = newUnknownInformationResultGqlStatusObject()
	}

	statusDescription := notification.Description
	if statusDescription == "" {
		statusDescription = defaultStatus.StatusDescription
	}

	diagnosticRecord := newDefaultDiagnosticRecord()

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

	return &db.GqlStatusObject{
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

// ToGqlStatusObjectFromSummary creates a new db.GqlStatusObject based on the context of the db.StreamSummary.
func ToGqlStatusObjectFromSummary(summary db.StreamSummary) *db.GqlStatusObject {
	if summary.HadRecord {
		return newSuccessGqlStatusObject()
	} else if summary.HadKey {
		return newNoDataGqlStatusObject()
	} else {
		return newOmittedResultGqlStatusObject()
	}
}
