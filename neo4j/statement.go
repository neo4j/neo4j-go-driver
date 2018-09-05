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

package neo4j

import "errors"

// Statement represents a statement along with its parameters (if any)
type Statement struct {
	cypher string
	params map[string]interface{}
}

func (statement *Statement) Cypher() string {
	return statement.cypher
}

func (statement *Statement) Params() map[string]interface{} {
	return statement.params
}

func (statement *Statement) validate() error {
	if len(statement.cypher) == 0 {
		return errors.New("cypher statement should not be empty")
	}

	return nil
}
