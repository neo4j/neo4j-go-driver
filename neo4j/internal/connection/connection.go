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

import (
	"time"
)

type Counters struct {
	NodesCreated int
}

type Summary struct {
	Bookmark *string
	//StmntType     string // Typed!
	//Cypher        string
	//Params        map[string]interface{}
	ServerVersion string
	Counters      *Counters
}

type Record struct {
	Values []interface{}
	Keys   []string
}

type Handle interface{}

type StreamHandle interface{}

type Stream struct {
	Handle Handle
	Keys   []string
}

type AccessMode int

const (
	WriteMode AccessMode = 0 // Should correspond to values in public API
	ReadMode  AccessMode = 1
)

type Connection interface {
	TxBegin(mode AccessMode, bookmarks []string, timeout time.Duration, meta map[string]interface{}) (Handle, error)
	TxRollback(tx Handle) error
	TxCommit(tx Handle) error
	// TODO: Timeout
	// TODO: Metadata
	Run(cypher string, params map[string]interface{}) (*Stream, error)
	RunTx(tx Handle, cypher string, params map[string]interface{}) (*Stream, error)
	// If error is nil, either Record or Summary has a value, if Record is nil there are no more records.
	// If error is non nil, neither Record or Summary has a value.
	Next(s StreamHandle) (*Record, *Summary, error)
	//IsAlive() bool
	Close() error
}

//type Routable interface {
// GetRoutingTabe
//}
