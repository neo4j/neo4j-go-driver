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

import (
	"errors"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
)

// Utility for retrieving a routing table using low level db connection
func getRoutingTable(conn db.Connection, context map[string]string) (*db.RoutingTable, error) {
	const query = "CALL dbms.cluster.routing.getRoutingTable($context)"

	stream, err := conn.Run(query, map[string]interface{}{"context": context}, db.ReadMode, nil, 0, nil)
	if err != nil {
		// Give a better error
		dbError, isDbError := err.(*db.DatabaseError)
		if isDbError && dbError.Code == "Neo.ClientError.Procedure.ProcedureNotFound" {
			return nil, &db.RoutingNotSupportedError{Server: conn.ServerName()}
		}
		return nil, err
	}

	rec, _, err := conn.Next(stream.Handle)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, errors.New("No record")
	}
	// Just empty the stream, ignore the summary should leave the connecion in ready state
	conn.Next(stream.Handle)

	// Parse the record into a routing table
	ttl, ttlOk := rec.Values[0].(int64)
	list, lOk := rec.Values[1].([]interface{})
	if !ttlOk || !lOk {
		return nil, errors.New("Parse error")
	}
	table := &db.RoutingTable{
		TimeToLive: int(ttl),
	}

	for _, x := range list {
		m, mOk := x.(map[string]interface{})
		if !mOk {
			return nil, errors.New("Parse error 2")
		}
		xaddresses, aOk := m["addresses"].([]interface{})
		_, rOk := m["role"].(string)
		if !aOk || !rOk {
			return nil, errors.New("Parse error 3")
		}
		addresses := make([]string, len(xaddresses))
		for i, xaddr := range xaddresses {
			a, aOk := xaddr.(string)
			if !aOk {
				return nil, errors.New("Parse error 4")
			}
			addresses[i] = a
		}
		role, rOk := m["role"].(string)
		if !rOk {
			return nil, errors.New("Parse error 5")
		}
		switch role {
		case "READ":
			table.Readers = addresses
		case "WRITE":
			table.Writers = addresses
		case "ROUTE":
			table.Routers = addresses
		}
	}

	return table, nil
}
