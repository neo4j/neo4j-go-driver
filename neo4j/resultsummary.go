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

package neo4j

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
	"math"
	"sort"
	"time"
)

// StatementType defines the type of the statement
type StatementType int

const (
	// StatementTypeUnknown identifies an unknown statement type
	StatementTypeUnknown StatementType = 0
	// StatementTypeReadOnly identifies a read-only statement
	StatementTypeReadOnly StatementType = 1
	// StatementTypeReadWrite identifies a read-write statement
	StatementTypeReadWrite StatementType = 2
	// StatementTypeWriteOnly identifies a write-only statement
	StatementTypeWriteOnly StatementType = 3
	// StatementTypeSchemaWrite identifies a schema-write statement
	StatementTypeSchemaWrite StatementType = 4
)

func (st StatementType) String() string {
	switch st {
	case StatementTypeReadOnly:
		return "r"
	case StatementTypeReadWrite:
		return "rw"
	case StatementTypeWriteOnly:
		return "w"
	case StatementTypeSchemaWrite:
		return "s"
	default:
		return ""
	}
}

type ResultSummary interface {
	// Server returns basic information about the server where the statement is carried out.
	Server() ServerInfo
	// Query returns the query that has been executed.
	Query() Query
	// StatementType returns type of statement that has been executed.
	StatementType() StatementType
	// Counters returns statistics counts for the statement.
	Counters() Counters
	// Plan returns statement plan for the executed statement if available, otherwise null.
	Plan() Plan
	// Profile returns profiled statement plan for the executed statement if available, otherwise null.
	Profile() ProfiledPlan
	// Deprecated: Notifications will be removed in 6.0.
	//
	// Notifications returns a slice of notifications produced while executing the statement.
	// The list will be empty if no notifications produced while executing the statement.
	Notifications() []Notification
	// GqlStatusObjects returns a slice of GqlStatusObjects that arose when executing the query.
	//
	// The slice always contains at least 1 status representing the Success, No Data, or Omitted Result.
	// All other statuses are notifications such as warnings about problematic queries or other valuable
	// information that can be presented to a client.
	//
	// The GqlStatusObjects will be presented in the following order:
	//   - A "no data" (``02xxx``) has precedence over a warning.
	//   - A "warning" (``01xxx``) has precedence over a success.
	//   - A "success" (``00xxx``) has precedence over anything informational (``03xxx``).
	//
	// GqlStatusObjects is part of the GQL compliant notifications preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlStatusObjects() []GqlStatusObject
	// ResultAvailableAfter returns the time it took for the server to make the result available for consumption.
	// Since 5.0, this returns a negative duration if the server has not sent the corresponding statistic.
	ResultAvailableAfter() time.Duration
	// ResultConsumedAfter returns the time it took the server to consume the result.
	// Since 5.0, this returns a negative duration if the server has not sent the corresponding statistic.
	ResultConsumedAfter() time.Duration
	// Database returns information about the database where the result is obtained from
	// Returns nil for Neo4j versions prior to v4.
	// Returns the default "neo4j" database for Community Edition servers.
	Database() DatabaseInfo
}

// Counters contains statistics about the changes made to the database made as part
// of the statement execution.
type Counters interface {
	// Whether there were any updates at all, eg. any of the counters are greater than 0.
	ContainsUpdates() bool
	// The number of nodes created.
	NodesCreated() int
	// The number of nodes deleted.
	NodesDeleted() int
	// The number of relationships created.
	RelationshipsCreated() int
	// The number of relationships deleted.
	RelationshipsDeleted() int
	PropertiesSet() int
	// The number of labels added to nodes.
	LabelsAdded() int
	// The number of labels removed from nodes.
	LabelsRemoved() int
	// The number of indexes added to the schema.
	IndexesAdded() int
	// The number of indexes removed from the schema.
	IndexesRemoved() int
	// The number of constraints added to the schema.
	ConstraintsAdded() int
	// The number of constraints removed from the schema.
	ConstraintsRemoved() int
	// The number of system updates
	SystemUpdates() int
	// ContainsSystemUpdates indicates whether the query contains system updates
	ContainsSystemUpdates() bool
}

type Statement interface {
	Query
}

type Query interface {
	// Text returns the statement's text.
	Text() string
	// Parameters returns the statement's parameters.
	Parameters() map[string]any
}

// ServerInfo contains basic information of the server.
type ServerInfo interface {
	// Address returns the address of the server.
	Address() string
	Agent() string
	ProtocolVersion() db.ProtocolVersion
}

type simpleServerInfo struct {
	address         string
	agent           string
	protocolVersion db.ProtocolVersion
}

func (s simpleServerInfo) Address() string {
	return s.address
}

func (s simpleServerInfo) Agent() string {
	return s.agent
}

func (s simpleServerInfo) ProtocolVersion() db.ProtocolVersion {
	return s.protocolVersion
}

// DatabaseInfo contains basic information of the database the query result has been obtained from.
type DatabaseInfo interface {
	Name() string
}

// Plan describes the actual plan that the database planner produced and used (or will use) to execute your statement.
// This can be extremely helpful in understanding what a statement is doing, and how to optimize it. For more details,
// see the Neo4j Manual. The plan for the statement is a tree of plans - each sub-tree containing zero or more child
// plans. The statement starts with the root plan. Each sub-plan is of a specific operator, which describes what
// that part of the plan does - for instance, perform an index lookup or filter results.
// The Neo4j Manual contains a reference of the available operator types, and these may differ across Neo4j versions.
type Plan interface {
	// Operator returns the operation this plan is performing.
	Operator() string
	// Arguments returns the arguments for the operator used.
	// Many operators have arguments defining their specific behavior. This map contains those arguments.
	Arguments() map[string]any
	// Identifiers returns a list of identifiers used by this plan. Identifiers used by this part of the plan.
	// These can be both identifiers introduced by you, or automatically generated.
	Identifiers() []string
	// Children returns zero or more child plans. A plan is a tree, where each child is another plan.
	// The children are where this part of the plan gets its input records - unless this is an operator that
	// introduces new records on its own.
	Children() []Plan
}

// ProfiledPlan is the same as a regular Plan - except this plan has been executed, meaning it also
// contains detailed information about how much work each step of the plan incurred on the database.
type ProfiledPlan interface {
	// Operator returns the operation this plan is performing.
	Operator() string
	// Arguments returns the arguments for the operator used.
	// Many operators have arguments defining their specific behavior. This map contains those arguments.
	Arguments() map[string]any
	// Identifiers returns a list of identifiers used by this plan. Identifiers used by this part of the plan.
	// These can be both identifiers introduced by you, or automatically generated.
	Identifiers() []string
	// DbHits returns the number of times this part of the plan touched the underlying data stores/
	DbHits() int64
	// Records returns the number of records this part of the plan produced.
	Records() int64
	// Children returns zero or more child plans. A plan is a tree, where each child is another plan.
	// The children are where this part of the plan gets its input records - unless this is an operator that
	// introduces new records on its own.
	Children() []ProfiledPlan
	PageCacheMisses() int64
	PageCacheHits() int64
	PageCacheHitRatio() float64
	Time() int64
}

// Notification represents notifications generated when executing a statement.
// A notification can be visualized in a client pinpointing problems or other information about the statement.
// Contrary to failures or errors, notifications do not affect the execution of the statement.
type Notification interface {
	// Code returns a notification code for the discovered issue of this notification.
	Code() string
	// Title returns a short summary of this notification.
	Title() string
	// Description returns a longer description of this notification.
	Description() string
	// Position returns the position in the statement where this notification points to.
	// Not all notifications have a unique position to point to and in that case the position would be set to nil.
	Position() InputPosition
	// Severity returns the severity level of this notification.
	//
	// Deprecated: please use SeverityLevel (or RawSeverityLevel) instead. Severity will be removed in 6.0.
	Severity() string
	// RawSeverityLevel returns the unmapped severity level of this notification.
	// This is useful when the driver cannot interpret the severity level returned by the server
	// In that case, SeverityLevel returns UnknownSeverity while RawSeverityLevel returns the raw string
	RawSeverityLevel() string
	// RawCategory returns the unmapped category of this notification.
	// This is useful when the driver cannot interpret the category returned by the server
	// In that case, Category returns UnknownCategory while RawCategory returns the raw string
	RawCategory() string
	// SeverityLevel returns the mapped security level of this notification.
	// If the severity level is not a known value, SeverityLevel returns UnknownSeverity
	// Call RawSeverityLevel to get access to the raw string value
	SeverityLevel() NotificationSeverity
	// Category returns the mapped category of this notification.
	// If the category is not a known value, Category returns UnknownCategory
	// Call RawCategory to get access to the raw string value
	Category() NotificationCategory
}

// GqlStatusObject represents a GqlStatusObject generated when executing a statement.
// A GqlStatusObject can be visualized in a client pinpointing problems or other information about the statement.
// Contrary to failures or errors, GqlStatusObjects do not affect the execution of the statement.
//
// GqlStatusObject is part of the GQL compliant notifications preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type GqlStatusObject interface {
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
	GqlStatus() string
	// StatusDescription returns a description of the status.
	StatusDescription() string
	// Position returns the position in the statement where this status points to.
	// Not all statuses have a unique position to point to and in that case the position would be set to nil.
	//
	// Only Notifications (see IsNotification) have a meaningful position.
	Position() InputPosition
	// Classification returns the mapped Classification of this status.
	// If the Classification is not a known value, Classification returns notifications.UnknownClassification.
	// Call RawClassification to get access to the raw string value
	//
	// Only Notifications (see IsNotification) have a meaningful classification.
	Classification() notifications.NotificationClassification
	// RawClassification returns the unmapped Classification of this status.
	// This is useful when the driver cannot interpret the Classification returned by the server.
	//
	// Only Notifications (see IsNotification) have a meaningful classification.
	RawClassification() string
	// Severity returns the mapped Severity of this status.
	// If the Severity is not a known value, Severity returns UnknownSeverity. // TODO should this be notifications.UnknownSeverity?
	// Call RawSeverity to get access to the raw string value
	//
	// Only Notifications (see IsNotification) have a meaningful severity.
	Severity() NotificationSeverity // TODO should this be or notifications.NotificationSeverity in the notification package?
	// RawSeverity returns the unmapped Severity level of this status.
	// This is useful when the driver cannot interpret the Severity returned by the server
	//
	// Only Notifications (see IsNotification) have a meaningful severity.
	RawSeverity() string
	// DiagnosticRecord returns further information about the status for diagnostic purposes.
	// Call RawDiagnosticRecord to get access to the raw string value.
	DiagnosticRecord() map[string]any
	// RawDiagnosticRecord returns a string version of DiagnosticRecord.
	// The purpose of this function is to provide a serialized object for human inspection.
	RawDiagnosticRecord() string
	// IsNotification returns true if this status is a Notification.
	//
	// Only some statuses are Notifications.
	IsNotification() bool
}

// InputPosition contains information about a specific position in a statement
type InputPosition interface {
	// Offset returns the character offset referred to by this position; offset numbers start at 0.
	Offset() int
	// Line returns the line number referred to by this position; line numbers start at 1.
	Line() int
	// Column returns the column number referred to by this position; column numbers start at 1.
	Column() int
}

type NotificationSeverity string

const (
	Warning         NotificationSeverity = "WARNING"
	Information     NotificationSeverity = "INFORMATION"
	UnknownSeverity NotificationSeverity = "UNKNOWN"
)

// Deprecated: please use notifications.NotificationCategory directly. This will be removed in 6.0.
type NotificationCategory notifications.NotificationCategory // TODO is this ok to do from string?

// TODO do these need deprecated individually to their notification.x equivalent?
const (
	Hint            NotificationCategory = "HINT"
	Unrecognized    NotificationCategory = "UNRECOGNIZED"
	Unsupported     NotificationCategory = "UNSUPPORTED"
	Performance     NotificationCategory = "PERFORMANCE"
	Deprecation     NotificationCategory = "DEPRECATION"
	Generic         NotificationCategory = "GENERIC"
	Security        NotificationCategory = "SECURITY"
	Topology        NotificationCategory = "TOPOLOGY"
	UnknownCategory NotificationCategory = "UNKNOWN"
)

type resultSummary struct {
	sum    *db.Summary
	cypher string
	params map[string]any
}

func (s *resultSummary) Agent() string {
	return s.sum.Agent
}

func (s *resultSummary) ProtocolVersion() db.ProtocolVersion {
	return db.ProtocolVersion{
		Major: s.sum.Major,
		Minor: s.sum.Minor,
	}
}

func (s *resultSummary) Server() ServerInfo {
	return s
}

func (s *resultSummary) Address() string {
	return s.sum.ServerName
}

func (s *resultSummary) Query() Query {
	return s
}

func (s *resultSummary) StatementType() StatementType {
	return StatementType(s.sum.StmntType)
}

func (s *resultSummary) Text() string {
	return s.cypher
}

func (s *resultSummary) Parameters() map[string]any {
	return s.params
}

func (s *resultSummary) Counters() Counters {
	return s
}

func (s *resultSummary) ContainsUpdates() bool {
	if s.sum.ContainsUpdates != nil {
		return *s.sum.ContainsUpdates
	}
	return s.ConstraintsAdded() > 0 || s.ConstraintsRemoved() > 0 ||
		s.IndexesAdded() > 0 || s.IndexesRemoved() > 0 ||
		s.NodesCreated() > 0 || s.NodesDeleted() > 0 ||
		s.RelationshipsCreated() > 0 || s.RelationshipsDeleted() > 0 ||
		s.LabelsAdded() > 0 || s.LabelsRemoved() > 0 ||
		s.PropertiesSet() > 0
}

func (s *resultSummary) getCounter(n string) int {
	if s.sum.Counters == nil {
		return 0
	}
	return s.sum.Counters[n]
}

func (s *resultSummary) SystemUpdates() int {
	return s.getCounter(db.SystemUpdates)
}

func (s *resultSummary) ContainsSystemUpdates() bool {
	if s.sum.ContainsSystemUpdates != nil {
		return *s.sum.ContainsSystemUpdates
	}
	return s.getCounter(db.SystemUpdates) > 0
}

func (s *resultSummary) NodesCreated() int {
	return s.getCounter(db.NodesCreated)
}

func (s *resultSummary) NodesDeleted() int {
	return s.getCounter(db.NodesDeleted)
}

func (s *resultSummary) RelationshipsCreated() int {
	return s.getCounter(db.RelationshipsCreated)
}

func (s *resultSummary) RelationshipsDeleted() int {
	return s.getCounter(db.RelationshipsDeleted)
}

func (s *resultSummary) PropertiesSet() int {
	return s.getCounter(db.PropertiesSet)
}

func (s *resultSummary) LabelsAdded() int {
	return s.getCounter(db.LabelsAdded)
}

func (s *resultSummary) LabelsRemoved() int {
	return s.getCounter(db.LabelsRemoved)
}

func (s *resultSummary) IndexesAdded() int {
	return s.getCounter(db.IndexesAdded)
}

func (s *resultSummary) IndexesRemoved() int {
	return s.getCounter(db.IndexesRemoved)
}

func (s *resultSummary) ConstraintsAdded() int {
	return s.getCounter(db.ConstraintsAdded)
}

func (s *resultSummary) ConstraintsRemoved() int {
	return s.getCounter(db.ConstraintsRemoved)
}

func (s *resultSummary) ResultAvailableAfter() time.Duration {
	return time.Duration(s.sum.TFirst) * time.Millisecond
}

func (s *resultSummary) ResultConsumedAfter() time.Duration {
	return time.Duration(s.sum.TLast) * time.Millisecond
}

func (s *resultSummary) Plan() Plan {
	if s.sum.Plan == nil {
		return nil
	}
	return &plan{plan: s.sum.Plan}
}

func (s *resultSummary) Database() DatabaseInfo {
	database := s.sum.Database
	if database == "" {
		return nil
	}
	return &databaseInfo{name: database}
}

type databaseInfo struct {
	name string
}

func (d *databaseInfo) Name() string {
	return d.name
}

type plan struct {
	plan *db.Plan
}

func (p *plan) Operator() string {
	return p.plan.Operator
}

func (p *plan) Arguments() map[string]any {
	return p.plan.Arguments
}

func (p *plan) Identifiers() []string {
	return p.plan.Identifiers
}

func (p *plan) Children() []Plan {
	children := make([]Plan, len(p.plan.Children))
	for i, c := range p.plan.Children {
		children[i] = &plan{plan: &c}
	}
	return children
}

func (s *resultSummary) Profile() ProfiledPlan {
	if s.sum.ProfiledPlan == nil {
		return nil
	}
	return &profile{profile: s.sum.ProfiledPlan}
}

type profile struct {
	profile *db.ProfiledPlan
}

func (p *profile) String() string {
	return fmt.Sprintf("%v", *p.profile)
}

func (p *profile) Operator() string {
	return p.profile.Operator
}

func (p *profile) Arguments() map[string]any {
	return p.profile.Arguments
}

func (p *profile) Identifiers() []string {
	return p.profile.Identifiers
}

func (p *profile) DbHits() int64 {
	return p.profile.DbHits
}

func (p *profile) Records() int64 {
	return p.profile.Records
}

func (p *profile) Children() []ProfiledPlan {
	children := make([]ProfiledPlan, len(p.profile.Children))
	for i, c := range p.profile.Children {
		child := c
		children[i] = &profile{profile: &child}
	}
	return children
}

func (p *profile) PageCacheMisses() int64 {
	return p.profile.PageCacheMisses
}

func (p *profile) PageCacheHits() int64 {
	return p.profile.PageCacheHits
}

func (p *profile) PageCacheHitRatio() float64 {
	return p.profile.PageCacheHitRatio
}

func (p *profile) Time() int64 {
	return p.profile.Time
}

func (s *resultSummary) Notifications() []Notification {
	if s.sum.Notifications != nil {
		n := make([]Notification, len(s.sum.Notifications))
		for i := range s.sum.Notifications {
			n[i] = &notification{notification: &s.sum.Notifications[i]}
		}
		return n
	}

	if s.sum.GqlStatusObjects != nil {
		// Polyfill Notifications from GqlStatusObjects
		n := make([]Notification, len(s.sum.GqlStatusObjects))
		for i := range s.sum.GqlStatusObjects {
			gqlStatusObject := s.sum.GqlStatusObjects[i]
			if gqlStatusObject.IsNotification {
				n[i] = &notification{notification: notifications.ToNotification(gqlStatusObject)}
			}
		}
		return n
	}
	return nil
}

func (s *resultSummary) GqlStatusObjects() []GqlStatusObject {
	if s.sum.GqlStatusObjects != nil {
		g := make([]GqlStatusObject, len(s.sum.GqlStatusObjects))
		for i := range s.sum.GqlStatusObjects {
			g[i] = &gqlStatusObject{gqlStatusObject: &s.sum.GqlStatusObjects[i]}
		}
		return g
	}
	if s.sum.Notifications != nil {
		// Polyfill GqlStatusObjects from Notifications
		g := make([]GqlStatusObject, len(s.sum.Notifications))
		for i := range s.sum.Notifications {
			notification := s.sum.Notifications[i]
			g[i] = &gqlStatusObject{gqlStatusObject: notifications.ToGqlStatusObject(notification)}
		}
		// Client generated polyfill status representing SUCCESS, NO DATA or OMITTED RESULT
		g = append(g, &gqlStatusObject{gqlStatusObject: notifications.ToGqlStatusObjectFromSummary(s.sum)})
		// Sort by GqlStatus weight
		sort.Slice(g, func(i, j int) bool {
			return calculateGqlStatusWeight(g[i]) < calculateGqlStatusWeight(g[j])
		})
		return g
	}
	return nil
}

func calculateGqlStatusWeight(gqlStatusObject GqlStatusObject) int {
	status := gqlStatusObject.GqlStatus()[:2]
	switch status {
	case "02":
		return 0 // no data
	case "01":
		return 1 // warning
	case "00":
		return 2 // success
	case "03":
		return 3 // informational
	default:
		return math.MaxInt // unknown or error statuses
	}
}

type notification struct {
	notification *db.Notification
}

func (n *notification) Code() string {
	return n.notification.Code
}

func (n *notification) Title() string {
	return n.notification.Title
}

func (n *notification) Description() string {
	return n.notification.Description
}

func (n *notification) Severity() string {
	return n.RawSeverityLevel()
}

func (n *notification) RawSeverityLevel() string {
	return n.notification.Severity
}

func (n *notification) SeverityLevel() NotificationSeverity {
	switch n.notification.Severity {
	case "WARNING":
		return Warning
	case "INFORMATION":
		return Information
	default:
		return UnknownSeverity
	}
}

func (n *notification) RawCategory() string {
	return n.notification.Category
}

func (n *notification) Category() NotificationCategory {
	switch n.notification.Category {
	case "HINT":
		return Hint
	case "UNRECOGNIZED":
		return Unrecognized
	case "UNSUPPORTED":
		return Unsupported
	case "PERFORMANCE":
		return Performance
	case "DEPRECATION":
		return Deprecation
	case "SECURITY":
		return Security
	case "TOPOLOGY":
		return Topology
	case "GENERIC":
		return Generic
	default:
		return UnknownCategory
	}
}

func (n *notification) Position() InputPosition {
	if n.notification.Position == nil {
		return nil
	}
	return n
}

func (n *notification) Offset() int {
	return n.notification.Position.Offset
}

func (n *notification) Column() int {
	return n.notification.Position.Column
}

func (n *notification) Line() int {
	return n.notification.Position.Line
}

type gqlStatusObject struct {
	gqlStatusObject *db.GqlStatusObject
}

func (g *gqlStatusObject) GqlStatus() string {
	return g.gqlStatusObject.GqlStatus
}

func (g *gqlStatusObject) StatusDescription() string {
	return g.gqlStatusObject.StatusDescription
}

func (g *gqlStatusObject) Position() InputPosition {
	if g.gqlStatusObject.Position == nil {
		return nil
	}
	return g
}

func (g *gqlStatusObject) Offset() int {
	return g.gqlStatusObject.Position.Offset
}

func (g *gqlStatusObject) Column() int {
	return g.gqlStatusObject.Position.Column
}

func (g *gqlStatusObject) Line() int {
	return g.gqlStatusObject.Position.Line
}

func (g *gqlStatusObject) Classification() notifications.NotificationClassification {
	switch g.gqlStatusObject.Classification {
	case "HINT":
		return notifications.HintClassification
	case "UNRECOGNIZED":
		return notifications.UnrecognizedClassification
	case "UNSUPPORTED":
		return notifications.UnsupportedClassification
	case "PERFORMANCE":
		return notifications.PerformanceClassification
	case "DEPRECATION":
		return notifications.DeprecationClassification
	case "SECURITY":
		return notifications.SecurityClassification
	case "TOPOLOGY":
		return notifications.TopologyClassification
	case "GENERIC":
		return notifications.GenericClassification
	default:
		return notifications.UnknownClassification
	}
}

func (g *gqlStatusObject) RawClassification() string {
	return g.gqlStatusObject.Classification
}

func (g *gqlStatusObject) Severity() NotificationSeverity {
	switch g.gqlStatusObject.Severity {
	case "WARNING":
		return Warning
	case "INFORMATION":
		return Information
	default:
		return UnknownSeverity
	}
}

func (g *gqlStatusObject) RawSeverity() string {
	return g.gqlStatusObject.Severity
}

func (g *gqlStatusObject) DiagnosticRecord() map[string]any {
	return g.gqlStatusObject.DiagnosticRecord
}

func (g *gqlStatusObject) RawDiagnosticRecord() string {
	return fmt.Sprintf("%v", g.gqlStatusObject.DiagnosticRecord)
}

func (g *gqlStatusObject) IsNotification() bool {
	return g.gqlStatusObject.IsNotification
}
