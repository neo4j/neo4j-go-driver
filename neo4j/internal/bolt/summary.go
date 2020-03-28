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

package bolt

import "github.com/neo4j/neo4j-go-driver/neo4j/api"

type summary struct {
	bookmark      string
	stmntType     string
	cypher        string
	params        map[string]interface{}
	serverVersion string
}

func (s *summary) Server() api.ServerInfo {
	return s
}

func (s *summary) Statement() api.Statement {
	return s
}

func (s *summary) Text() string {
	return s.cypher
}

func (s *summary) Params() map[string]interface{} {
	return s.params
}

func (i *summary) Version() string {
	return i.serverVersion
}
