/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

package neo4j_go_driver

type StatementType int

const (
    StatementTypeReadOnly    StatementType = 0
	StatementTypeReadWrite                 = 1
	StatementTypeWriteOnly                 = 2
	StatementTypeSchemaWrite               = 3
)

type ServerInfo struct {
	address string
	version string
}

type InputPosition struct {
	offset int
	line   int
	column int
}

type Notification struct {
	code        string
	title       string
	description string
	position    InputPosition
	severity    string
}

type Counters struct {
    nodesCreated int
    nodesDeleted int
    relationshipsCreated int
    relationshipsDeleted int
    propertiesSet int
    labelsAdded int
    labelsRemoved int
    indexesAdded int
    indexesRemoved int
    constraintsAdded int
    constraintsRemoved int

}

type Plan struct {
}

type Profile struct {
}

type ResultSummary struct {
	statement             Statement
	counters              *Counters
	statementType         StatementType
	plan                  *Plan
	profile               *Profile
	notifications         *[]Notification
	resultsAvailableAfter int64
	resultsConsumedAfter  int64
	server                ServerInfo
}
