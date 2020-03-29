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

package connection

type Summary struct {
	Bookmark string
	//StmntType     string // Typed!
	//Cypher        string
	//Params        map[string]interface{}
	ServerVersion string
}

type Record struct {
	Values []interface{}
	Keys   []string
}

/*
type Result interface {
	Next() bool
	Record() *Record
	Err() error
	Summary() (*Summary, error)
	FetchAll()
	ConsumeAll()
}
*/

type Stream struct {
	Keys []string
}

type State int

const (
	DISCONNECTED State = -2
	INVALID      State = -1 // Needed?
	FREE         State = 0
	READY        State = 1 // Bound to session
	STREAMING    State = 2
	TX           State = 3
	STREAMINGTX  State = 4
)

type Connection interface {
	// State must be READY or TX.
	// Upon success next state is STREAMING or STREAMINGTX.
	// Upon error next state is READY, INVALID or DISCONNECTED depending on type of error.
	// TODO: Timeout
	// TODO: Metadata
	Run(cypher string, params map[string]interface{}) (*Stream, error)
	// State must be STREAMING or STREAMINGTX.
	// If error is nil, either Record or Summary has a value, if Record is nil there are no more records.
	// If error is non nil, neither Record or Summary has a value.
	Next() (*Record, *Summary, error)
	//State() State
	//IsAlive() bool
	Close() error
	// BeginTx
	// CommitTx
	// RollbackTx
	// GetResult
	// Run
}

//type Routable interface {
// GetRoutingTabe
//}
