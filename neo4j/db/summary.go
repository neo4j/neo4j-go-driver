/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package db

// Definitions of these should correspond to public API
type StatementType int

const (
	StatementTypeUnknown     StatementType = 0
	StatementTypeRead        StatementType = 1
	StatementTypeReadWrite   StatementType = 2
	StatementTypeWrite       StatementType = 3
	StatementTypeSchemaWrite StatementType = 4
)

// Counter key names
const (
	NodesCreated         = "nodes-created"
	NodesDeleted         = "nodes-deleted"
	RelationshipsCreated = "relationships-created"
	RelationshipsDeleted = "relationships-deleted"
	PropertiesSet        = "properties-set"
	LabelsAdded          = "labels-added"
	LabelsRemoved        = "labels-removed"
	IndexesAdded         = "indexes-added"
	IndexesRemoved       = "indexes-removed"
	ConstraintsAdded     = "constraints-added"
	ConstraintsRemoved   = "constraints-removed"
	SystemUpdates        = "system-updates"
)

// Plan describes the actual plan that the database planner produced and used (or will use) to execute your statement.
// This can be extremely helpful in understanding what a statement is doing, and how to optimize it. For more details,
// see the Neo4j Manual. The plan for the statement is a tree of plans - each sub-tree containing zero or more child
// plans. The statement starts with the root plan. Each sub-plan is of a specific operator, which describes what
// that part of the plan does - for instance, perform an index lookup or filter results.
// The Neo4j Manual contains a reference of the available operator types, and these may differ across Neo4j versions.
type Plan struct {
	// Operator is the operation this plan is performing.
	Operator string
	// Arguments for the operator.
	// Many operators have arguments defining their specific behavior. This map contains those arguments.
	Arguments map[string]any
	// List of identifiers used by this plan. Identifiers used by this part of the plan.
	// These can be both identifiers introduced by you, or automatically generated.
	Identifiers []string
	// Zero or more child plans. A plan is a tree, where each child is another plan.
	// The children are where this part of the plan gets its input records - unless this is an operator that
	// introduces new records on its own.
	Children []Plan
}

// ProfiledPlan is the same as a regular Plan - except this plan has been executed, meaning it also
// contains detailed information about how much work each step of the plan incurred on the database.
type ProfiledPlan struct {
	// Operator contains the operation this plan is performing.
	Operator string
	// Arguments contains the arguments for the operator used.
	// Many operators have arguments defining their specific behavior. This map contains those arguments.
	Arguments map[string]any
	// Identifiers contains a list of identifiers used by this plan. Identifiers used by this part of the plan.
	// These can be both identifiers introduced by you, or automatically generated.
	Identifiers []string
	// DbHits contains the number of times this part of the plan touched the underlying data stores/
	DbHits int64
	// Records contains the number of records this part of the plan produced.
	Records int64
	// Children contains zero or more child plans. A plan is a tree, where each child is another plan.
	// The children are where this part of the plan gets its input records - unless this is an operator that
	// introduces new records on its own.
	Children          []ProfiledPlan
	PageCacheMisses   int64
	PageCacheHits     int64
	PageCacheHitRatio float64
	Time              int64
}

// StreamSummary is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type StreamSummary struct {
	HadRecord bool
	HadKey    bool
}

type Notification struct {
	Code        string
	Title       string
	Description string
	Position    *InputPosition
	Severity    string
	Category    string
}

// GqlStatusObject represents a GqlStatusObject generated when executing a statement.
// A GqlStatusObject can be visualized in a client pinpointing problems or other information about the statement.
// Contrary to failures or errors, GqlStatusObjects do not affect the execution of the statement.
//
// GqlStatusObject is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type GqlStatusObject struct {
	// Deprecated: for backward compatibility with Notification.Code only.
	Code string
	// Deprecated: for backward compatibility with Notification.Title only.
	Title string
	// Deprecated: for backward compatibility with Notification.Description only.
	Description string
	// GqlStatus returns the GQLSTATUS.
	// The following GQLSTATUS codes denote codes that the driver will use for
	// polyfilling (when connected to an old, non-GQL-aware server). Further, they may be used by servers
	// during the transition-phase to GQLSTATUS-awareness.
	//   - "01N42" (warning - unknown warning)
	//   - "02N42" (no data - unknown subcondition)
	//   - "03N42" (informational - unknown notification)
	//   - "05N42" (general processing exception - unknown error)
	//
	// This means these codes are not guaranteed to be stable and may change in future versions.
	GqlStatus string
	// StatusDescription returns a description of the status.
	StatusDescription string
	// Position returns the position in the statement where this status points to.
	// Not all statuses have a unique position to point to and in that case the position would be set to nil.
	//
	// Only Notifications (see IsNotification) have a meaningful position.
	Position *InputPosition
	// Classification returns the mapped Classification of this status.
	// If the Classification is not a known value, Classification returns notifications.UnknownClassification.
	//
	// Only Notifications (see IsNotification) have a meaningful classification.
	Classification string
	// Severity returns the mapped Severity of this status.
	// If the Severity is not a known value, Severity returns notifications.UnknownSeverity.
	//
	// Only Notifications (see IsNotification) have a meaningful severity.
	Severity string
	// DiagnosticRecord returns further information about the status for diagnostic purposes.
	DiagnosticRecord map[string]any
	// IsNotification returns true if this status is a Notification.
	//
	// Only some statuses are Notifications.
	IsNotification bool
}

// InputPosition contains information about a specific position in a statement
type InputPosition struct {
	// Offset contains the character offset referred to by this position; offset numbers start at 0.
	Offset int
	// Line contains the line number referred to by this position; line numbers start at 1.
	Line int
	// Column contains the column number referred to by this position; column numbers start at 1.
	Column int
}

type ProtocolVersion struct {
	Major int
	Minor int
}

type Summary struct {
	Bookmark              string
	StmntType             StatementType
	ServerName            string
	Agent                 string
	Major                 int
	Minor                 int
	Counters              map[string]int
	TFirst                int64
	TLast                 int64
	Plan                  *Plan
	ProfiledPlan          *ProfiledPlan
	Notifications         []Notification
	GqlStatusObjects      []GqlStatusObject
	Database              string
	ContainsSystemUpdates *bool
	ContainsUpdates       *bool
	StreamSummary         StreamSummary
}
