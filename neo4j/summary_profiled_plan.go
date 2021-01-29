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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// ProfiledPlan is the same as a regular Plan - except this plan has been executed, meaning it also
// contains detailed information about how much work each step of the plan incurred on the database.
type ProfiledPlan interface {
	// Operator returns the operation this plan is performing.
	Operator() string
	// Arguments returns the arguments for the operator used.
	// Many operators have arguments defining their specific behavior. This map contains those arguments.
	Arguments() map[string]interface{}
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
