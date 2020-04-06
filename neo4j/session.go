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
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

// TransactionWork represents a unit of work that will be executed against the provided
// transaction
type TransactionWork func(tx Transaction) (interface{}, error)

// Session represents a logical connection (which is not tied to a physical connection)
// to the server
type Session interface {
	// LastBookmark returns the bookmark received following the last successfully completed transaction.
	// If no bookmark was received or if this transaction was rolled back, the bookmark value will not be changed.
	//LastBookmark() string
	// BeginTransaction starts a new explicit transaction on this session
	BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error)
	// ReadTransaction executes the given unit of work in a AccessModeRead transaction with
	// retry logic in place
	ReadTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error)
	// WriteTransaction executes the given unit of work in a AccessModeWrite transaction with
	// retry logic in place
	WriteTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error)
	// Run executes an auto-commit statement and returns a result
	Run(cypher string, params map[string]interface{}) (Result, error) //, configurers ...func(*TransactionConfig)) (Result, error)
	// Close closes any open resources and marks this session as unusable
	Close() error
}

type closer func(c conn.Connection)

type session struct {
	conn      conn.Connection
	closer    closer
	mode      conn.AccessMode
	bookmarks []string
}

func newSession(conn conn.Connection, closer closer, mode conn.AccessMode, bookmarks []string) *session {
	return &session{
		conn:   conn,
		closer: closer,
		mode:   mode,
	}
}

func (s *session) BeginTransaction(configurers ...func(*TransactionConfig)) (Transaction, error) {
	config := TransactionConfig{Timeout: 0, Metadata: nil}
	for _, c := range configurers {
		c(&config)
	}

	tx, err := s.conn.TxBegin(s.mode, s.bookmarks, config.Timeout, config.Metadata)
	if err != nil {
		return nil, err
	}

	return &transaction{
		conn: s.conn,
		tx:   tx,
	}, nil
}

func (s *session) runRetriable(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {
	// TODO: Retry
	tx, err := s.BeginTransaction(configurers...)
	if err != nil {
		return nil, err
	}
	defer tx.Close()

	x, err := work(tx)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return x, nil
}

func (s *session) ReadTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {
	return s.runRetriable(work, configurers...)
}

func (s *session) WriteTransaction(work TransactionWork, configurers ...func(*TransactionConfig)) (interface{}, error) {
	if s.mode == conn.ReadMode {
		return nil, newDriverError("write queries cannot be performed in read access mode")
	}
	return s.runRetriable(work, configurers...)
}

func patchParamType(x interface{}) interface{} {
	switch v := x.(type) {
	case *Point:
		if v.dimension == 2 {
			return &types.Point2D{X: v.x, Y: v.y, SpatialRefId: uint32(v.srId)}
		}
		return &types.Point3D{X: v.x, Y: v.y, Z: v.z, SpatialRefId: uint32(v.srId)}
	case Date:
		return types.Date(v.Time())
	case LocalTime:
		return types.LocalTime(v.Time())
	case OffsetTime:
		return types.Time(v.Time())
	case LocalDateTime:
		return types.LocalDateTime(v.Time())
	case Duration:
		return types.Duration{Months: v.months, Days: v.days, Seconds: v.seconds, Nanos: v.nanos}
	default:
		return v
	}
}

func patchParams(params map[string]interface{}) map[string]interface{} {
	patched := make(map[string]interface{}, len(params))
	for k, v := range params {
		patched[k] = patchParamType(v)
	}
	return patched
}

func (s *session) Run(
	cypher string, params map[string]interface{}) (Result, error) {

	stream, err := s.conn.Run(cypher, patchParams(params))
	if err != nil {
		return nil, err
	}
	return newResult(s.conn, stream, cypher, params), nil
}

func (s *session) Close() error {
	s.closer(s.conn)
	return nil
}
