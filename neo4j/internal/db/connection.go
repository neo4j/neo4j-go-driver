/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package db

import (
	"time"
)

// Definitions of these should correspond to public API
type AccessMode int

const (
	WriteMode AccessMode = 0
	ReadMode  AccessMode = 1
)

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
	Arguments map[string]interface{}
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
	// Operator returns the operation this plan is performing.
	Operator string
	// Arguments returns the arguments for the operator used.
	// Many operators have arguments defining their specific behavior. This map contains those arguments.
	Arguments map[string]interface{}
	// Identifiers returns a list of identifiers used by this plan. Identifiers used by this part of the plan.
	// These can be both identifiers introduced by you, or automatically generated.
	Identifiers []string
	// DbHits returns the number of times this part of the plan touched the underlying data stores/
	DbHits int64
	// Records returns the number of records this part of the plan produced.
	Records int64
	// Children returns zero or more child plans. A plan is a tree, where each child is another plan.
	// The children are where this part of the plan gets its input records - unless this is an operator that
	// introduces new records on its own.
	Children []ProfiledPlan
}

type Summary struct {
	Bookmark      string
	StmntType     StatementType
	ServerName    string
	ServerVersion string
	Counters      map[string]int
	TFirst        int64
	TLast         int64
	Plan          *Plan
	ProfiledPlan  *ProfiledPlan
}

type Record struct {
	Values []interface{}
	Keys   []string
}

type Handle interface{}

type Stream struct {
	Handle Handle
	Keys   []string
}

// Abstract database connection.
type Connection interface {
	TxBegin(mode AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (Handle, error)
	TxRollback(tx Handle) error
	TxCommit(tx Handle) error
	Run(cypher string, params map[string]interface{}, mode AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (*Stream, error)
	RunTx(tx Handle, cypher string, params map[string]interface{}) (*Stream, error)
	// Moves to next item in the stream.
	// If error is nil, either Record or Summary has a value, if Record is nil there are no more records.
	// If error is non nil, neither Record or Summary has a value.
	Next(streamHandle Handle) (*Record, *Summary, error)
	// Returns bookmark from last committed transaction or last finished auto-commit transaction.
	// Note that if there is an ongoing auto-commit transaction (stream active) the bookmark
	// from that is not included. Empty string if no bookmark.
	Bookmark() string
	// Returns name of the remote server
	ServerName() string
	// Returns server version on pattern Neo4j/1.2.3
	ServerVersion() string
	// Returns true if the connection is fully functional.
	// Implementation of this should be passive, no pinging or similair since it might be
	// called rather frequently.
	IsAlive() bool
	// Returns the point in time when this connection was established.
	Birthdate() time.Time
	// Resets connection to same state as directly after a connect.
	Reset()
	// Closes the database connection as well as any underlying connection.
	// The instance should not be used after being closed.
	Close()

	// Used to check if the connection is alive and ready to use
	// Ping() error
}

type RoutingTable struct {
	TimeToLive int
	Routers    []string
	Readers    []string
	Writers    []string
}

type ClusterDiscovery interface {
	GetRoutingTable(context map[string]string) (*RoutingTable, error)
}
