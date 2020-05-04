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
		return nil, errors.New("No routing table record")
	}
	// Just empty the stream, ignore the summary should leave the connecion in ready state
	conn.Next(stream.Handle)

	table := parseRoutingTableRecord(rec)
	if table == nil {
		return nil, errors.New("Unable to parse routing table")
	}

	return table, nil
}

// Parses a record assumed to contain a routing table into common db API routing table struct
// Returns nil if error while parsing
func parseRoutingTableRecord(rec *db.Record) *db.RoutingTable {
	ttl, ok := rec.Values[0].(int64)
	if !ok {
		return nil
	}
	listOfX, ok := rec.Values[1].([]interface{})
	if !ok {
		return nil
	}

	table := &db.RoutingTable{
		TimeToLive: int(ttl),
	}

	for _, x := range listOfX {
		// Each x should be a map consisting of addresses and the role
		m, ok := x.(map[string]interface{})
		if !ok {
			return nil
		}
		addressesX, ok := m["addresses"].([]interface{})
		if !ok {
			return nil
		}
		addresses := make([]string, len(addressesX))
		for i, addrX := range addressesX {
			addr, ok := addrX.(string)
			if !ok {
				return nil
			}
			addresses[i] = addr
		}
		role, ok := m["role"].(string)
		if !ok {
			return nil
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
	return table
}
