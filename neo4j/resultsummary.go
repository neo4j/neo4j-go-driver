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

package neo4j

import (
	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
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

type ResultSummary interface {
	// Server returns basic information about the server where the statement is carried out.
	Server() ServerInfo
	// Statement returns statement that has been executed.
	Statement() Statement
	// StatementType returns type of statement that has been executed.
	StatementType() StatementType
	// Counters returns statistics counts for the statement.
	Counters() Counters
	// Plan returns statement plan for the executed statement if available, otherwise null.
	//Plan() Plan
	// Profile returns profiled statement plan for the executed statement if available, otherwise null.
	//Profile() ProfiledPlan
	// Notifications returns a slice of notifications produced while executing the statement.
	// The list will be empty if no notifications produced while executing the statement.
	//Notifications() []Notification
	// ResultAvailableAfter returns the time it took for the server to make the result available for consumption.
	ResultAvailableAfter() time.Duration
	// ResultConsumedAfter returns the time it took the server to consume the result.
	ResultConsumedAfter() time.Duration
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
}

type Statement interface {
	// Text returns the statement's text.
	Text() string
	// Params returns the statement's parameters.
	Params() map[string]interface{}
}

// ServerInfo contains basic information of the server.
type ServerInfo interface {
	// Address returns the address of the server.
	//Address() string
	// Version returns the version of Neo4j running at the server.
	Version() string
}

type resultSummary struct {
	sum    *conn.Summary
	cypher string
	params map[string]interface{}
}

func (s *resultSummary) Server() ServerInfo {
	return s
}

func (s *resultSummary) Version() string {
	return s.sum.ServerVersion
}

func (s *resultSummary) Statement() Statement {
	return s
}

func (s *resultSummary) StatementType() StatementType {
	return StatementType(s.sum.StmntType)
}

func (s *resultSummary) Text() string {
	return s.cypher
}

func (s *resultSummary) Params() map[string]interface{} {
	return s.params
}

func (s *resultSummary) Counters() Counters {
	return s
}

func (s *resultSummary) ContainsUpdates() bool {
	return len(s.sum.Counters) > 0
}

func (s *resultSummary) getCounter(n string) int {
	if s.sum.Counters == nil {
		return 0
	}
	return s.sum.Counters[n]
}

func (s *resultSummary) NodesCreated() int {
	return s.getCounter(conn.NodesCreated)
}

func (s *resultSummary) NodesDeleted() int {
	return s.getCounter(conn.NodesDeleted)
}

func (s *resultSummary) RelationshipsCreated() int {
	return s.getCounter(conn.RelationshipsCreated)
}

func (s *resultSummary) RelationshipsDeleted() int {
	return s.getCounter(conn.RelationshipsDeleted)
}

func (s *resultSummary) PropertiesSet() int {
	return s.getCounter(conn.PropertiesSet)
}

func (s *resultSummary) LabelsAdded() int {
	return s.getCounter(conn.LabelsAdded)
}

func (s *resultSummary) LabelsRemoved() int {
	return s.getCounter(conn.LabelsRemoved)
}

func (s *resultSummary) IndexesAdded() int {
	return s.getCounter(conn.IndexesAdded)
}

func (s *resultSummary) IndexesRemoved() int {
	return s.getCounter(conn.IndexesRemoved)
}

func (s *resultSummary) ConstraintsAdded() int {
	return s.getCounter(conn.ConstraintsAdded)
}

func (s *resultSummary) ConstraintsRemoved() int {
	return s.getCounter(conn.ConstraintsRemoved)
}

func (s *resultSummary) ResultAvailableAfter() time.Duration {
	return time.Duration(s.sum.TFirst) * time.Millisecond
}

func (s *resultSummary) ResultConsumedAfter() time.Duration {
	return time.Duration(s.sum.TLast) * time.Millisecond
}
