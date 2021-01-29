/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
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

// ResultSummary contains information about statement execution.
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
	Plan() Plan
	// Profile returns profiled statement plan for the executed statement if available, otherwise null.
	Profile() ProfiledPlan
	// Notifications returns a slice of notifications produced while executing the statement.
	// The list will be empty if no notifications produced while executing the statement.
	Notifications() []Notification
	// ResultAvailableAfter returns the time it took for the server to make the result available for consumption.
	ResultAvailableAfter() time.Duration
	// ResultConsumedAfter returns the time it took the server to consume the result.
	ResultConsumedAfter() time.Duration
}

type neoResultSummary struct {
	server               ServerInfo
	statement            Statement
	statementType        StatementType
	counters             Counters
	plan                 Plan
	profile              ProfiledPlan
	notifications        []Notification
	resultAvailableAfter time.Duration
	resultConsumedAfter  time.Duration
}

func (summary *neoResultSummary) Server() ServerInfo {
	return summary.server
}

func (summary *neoResultSummary) Statement() Statement {
	return summary.statement
}

func (summary *neoResultSummary) StatementType() StatementType {
	return summary.statementType
}

func (summary *neoResultSummary) Counters() Counters {
	return summary.counters
}

func (summary *neoResultSummary) Plan() Plan {
	return summary.plan
}

func (summary *neoResultSummary) Profile() ProfiledPlan {
	return summary.profile
}

func (summary *neoResultSummary) Notifications() []Notification {
	return summary.notifications
}

func (summary *neoResultSummary) ResultAvailableAfter() time.Duration {
	return summary.resultAvailableAfter
}

func (summary *neoResultSummary) ResultConsumedAfter() time.Duration {
	return summary.resultConsumedAfter
}
