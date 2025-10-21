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

// Deprecated: Use NotificationClassification instead. This will be removed in a future release.
type NotificationCategory string

type NotificationClassification = NotificationCategory

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
	// Schema requires server version 5.17 or newer.
	Schema  NotificationCategory = "SCHEMA"
	Unknown NotificationCategory = "UNKNOWN"
)

type NotificationSeverity string

const (
	Warning         NotificationSeverity = "WARNING"
	Information     NotificationSeverity = "INFORMATION"
	UnknownSeverity NotificationSeverity = "UNKNOWN"
)

// NotificationMinimumSeverityLevel defines the minimum severity level of notifications the server should send.
// Can be used for NotificationsMinSeverity of config.Config and config.SessionConfig.
type NotificationMinimumSeverityLevel string

const (
	DefaultLevel     NotificationMinimumSeverityLevel = ""
	DisabledLevel    NotificationMinimumSeverityLevel = "OFF"
	WarningLevel     NotificationMinimumSeverityLevel = "WARNING"
	InformationLevel NotificationMinimumSeverityLevel = "INFORMATION"
)

// Deprecated: Use NotificationDisabledClassifications instead. This will be removed in a future release.
type NotificationDisabledCategories struct {
	categories []NotificationCategory
	none       bool
}

type NotificationDisabledClassifications struct {
	classifications []NotificationClassification
	none            bool
}

// DisableCategories creates a NotificationDisabledCategories that disables the given categories.
// Can be used for NotificationsDisabledCategories of config.Config and config.SessionConfig.
//
// Deprecated: Use DisableClassifications instead. This will be removed in a future release.
func DisableCategories(value ...NotificationCategory) NotificationDisabledCategories {
	return NotificationDisabledCategories{value, false}
}

// DisableClassifications creates a NotificationDisabledClassifications that disables the given classifications.
// Can be used for NotificationsDisabledClassifications of config.Config and config.SessionConfig.
func DisableClassifications(value ...NotificationClassification) NotificationDisabledClassifications {
	return NotificationDisabledClassifications{value, false}
}

// DisableNoCategories creates a NotificationDisabledCategories that enables all categories.
// Can be used for NotificationsDisabledCategories of config.Config and neo4j.SessionConfig.
//
// Deprecated: Use DisableNoClassifications instead. This will be removed in a future release.
func DisableNoCategories() NotificationDisabledCategories {
	return NotificationDisabledCategories{nil, true}
}

// DisableNoClassifications creates a NotificationDisabledClassifications that enables all classifications.
// Can be used for NotificationsDisabledClassifications of config.Config and neo4j.SessionConfig.
func DisableNoClassifications() NotificationDisabledClassifications {
	return NotificationDisabledClassifications{nil, true}
}

// DisablesNone returns true if all categories are enabled.
//
// Deprecated: Use NotificationDisabledClassifications.DisablesNone instead. This will be removed in a future release.
func (n *NotificationDisabledCategories) DisablesNone() bool {
	return n.none
}

// DisablesNone returns true if all classifications are enabled.
func (n *NotificationDisabledClassifications) DisablesNone() bool {
	return n.none
}

// DisabledCategories returns the categories that are disabled.
//
// Deprecated: Use NotificationDisabledClassifications.DisabledClassifications instead. This will be removed in a future release.
func (n *NotificationDisabledCategories) DisabledCategories() []NotificationCategory {
	return n.categories
}

// DisabledClassifications returns the classifications that are disabled.
func (n *NotificationDisabledClassifications) DisabledClassifications() []NotificationClassification {
	return n.classifications
}
