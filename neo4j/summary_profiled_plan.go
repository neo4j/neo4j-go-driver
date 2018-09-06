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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// ProfiledPlan describes the plan that the database planner produced and executed
type ProfiledPlan interface {
	Operator() string
	Arguments() map[string]interface{}
	Identifiers() []string
	DbHits() int64
	Records() int64
	Children() []ProfiledPlan
}

type neoProfiledPlan struct {
	operator    string
	arguments   map[string]interface{}
	identifiers []string
	dbHits      int64
	records     int64
	children    []ProfiledPlan
}

func (plan *neoProfiledPlan) Operator() string {
	return plan.operator
}

func (plan *neoProfiledPlan) Arguments() map[string]interface{} {
	return plan.arguments
}

func (plan *neoProfiledPlan) Identifiers() []string {
	return plan.identifiers
}

func (plan *neoProfiledPlan) DbHits() int64 {
	return plan.dbHits
}

func (plan *neoProfiledPlan) Records() int64 {
	return plan.records
}

func (plan *neoProfiledPlan) Children() []ProfiledPlan {
	return plan.children
}
