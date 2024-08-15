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

type NotificationCategory string

// NotificationClassification is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
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

type NotificationDisabledCategories struct {
	categories []NotificationCategory
	none       bool
}

// NotificationDisabledClassifications is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type NotificationDisabledClassifications struct {
	classifications []NotificationClassification
	none            bool
}

// DisableCategories creates a NotificationDisabledCategories that disables the given categories.
// Can be used for NotificationsDisabledCategories of config.Config and config.SessionConfig.
func DisableCategories(value ...NotificationCategory) NotificationDisabledCategories {
	return NotificationDisabledCategories{value, false}
}

// DisableClassifications creates a NotificationDisabledClassifications that disables the given classifications.
// Can be used for NotificationsDisabledClassifications of config.Config and config.SessionConfig.
//
// DisableClassifications is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
func DisableClassifications(value ...NotificationClassification) NotificationDisabledClassifications {
	return NotificationDisabledClassifications{value, false}
}

// DisableNoCategories creates a NotificationDisabledCategories that enables all categories.
// Can be used for NotificationsDisabledCategories of neo4j.Config and neo4j.SessionConfig.
func DisableNoCategories() NotificationDisabledCategories {
	return NotificationDisabledCategories{nil, true}
}

// DisableNoClassifications creates a NotificationDisabledClassifications that enables all classifications.
// Can be used for NotificationsDisabledClassifications of neo4j.Config and neo4j.SessionConfig.
//
// DisableNoClassifications is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
func DisableNoClassifications() NotificationDisabledClassifications {
	return NotificationDisabledClassifications{nil, true}
}

// DisablesNone returns true if all categories are enabled.
func (n *NotificationDisabledCategories) DisablesNone() bool {
	return n.none
}

// DisablesNone returns true if all classifications are enabled.
//
// DisablesNone is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
func (n *NotificationDisabledClassifications) DisablesNone() bool {
	return n.none
}

// DisabledCategories returns the categories that are disabled.
func (n *NotificationDisabledCategories) DisabledCategories() []NotificationCategory {
	return n.categories
}

// DisabledClassifications returns the classifications that are disabled.
//
// DisabledClassifications is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
func (n *NotificationDisabledClassifications) DisabledClassifications() []NotificationClassification {
	return n.classifications
}
