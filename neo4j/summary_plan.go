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

// Plan describes the plan that the database planner produced
type Plan interface {
	Operator() string
	Arguments() map[string]interface{}
	Identifiers() []string
	Children() []Plan
}

type neoPlan struct {
	operator    string
	arguments   map[string]interface{}
	identifiers []string
	children    []Plan
}

func (plan *neoPlan) Operator() string {
	return plan.operator
}

func (plan *neoPlan) Arguments() map[string]interface{} {
	return plan.arguments
}

func (plan *neoPlan) Identifiers() []string {
	return plan.identifiers
}

func (plan *neoPlan) Children() []Plan {
	return plan.children
}

