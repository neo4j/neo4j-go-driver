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

package router

import (
	"context"
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/connection"
	poolpackage "github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/pool"
)

func TestReadTableTable(ot *testing.T) {
	standardRouters := []string{"router1", "router2"}

	assertNoTable := func(t *testing.T, table *connection.RoutingTable, err error) {
		t.Helper()
		if table != nil {
			t.Error("Shouldn't be a table")
		}
		if err == nil {
			t.Fatal("Should be an error")
		}
		// Error should always indicate that routing table couldn't be retrieved
		_ = err.(*ReadRoutingTableError)
	}

	assertTable := func(t *testing.T, table *connection.RoutingTable, err error) {
		t.Helper()
		if table == nil {
			t.Fatalf("Should be a table, error: %s", err)
		}
		if err != nil {
			t.Errorf("Shouldn't be an error: %s", err)
		}
	}

	cases := []struct {
		name       string
		routers    []string
		pool       *poolFake
		assert     func(t *testing.T, table *connection.RoutingTable, err error)
		numReturns int
	}{
		{
			name:       "No routers",
			routers:    []string{},
			assert:     assertNoTable,
			pool:       &poolFake{},
			numReturns: 0,
		},
		{
			name:    "Fail to connect to all routers",
			routers: standardRouters,
			assert:  assertNoTable,
			pool: &poolFake{
				borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
					return nil, errors.New("borrow fail")
				},
			},
			numReturns: 0,
		},
		{
			name:    "Get routing table from first router",
			routers: standardRouters,
			assert:  assertTable,
			pool: &poolFake{
				borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
					return &connFake{table: &connection.RoutingTable{}}, nil
				},
			},
			numReturns: 1,
		},
		{
			name:    "Get routing table from last router",
			routers: standardRouters,
			assert:  assertTable,
			pool: &poolFake{
				borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
					if names[0] == "router2" {
						return &connFake{table: &connection.RoutingTable{}}, nil
					}
					return nil, errors.New("borrow fail")
				},
			},
			numReturns: 1,
		},
		{
			name:    "Connection not implementing ClusterDiscovery",
			routers: standardRouters,
			assert:  assertNoTable,
			pool: &poolFake{
				borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
					return &connFakeNoDiscovery{}, nil
				},
			},
			numReturns: len(standardRouters),
		},
		{
			name:    "All routing table calls fail",
			routers: standardRouters,
			assert:  assertNoTable,
			pool: &poolFake{
				borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
					return &connFake{err: errors.New("GetRoutingTable fail")}, nil
				},
			},
			numReturns: len(standardRouters),
		},
		{
			name:    "Cancel context",
			routers: standardRouters,
			pool: &poolFake{
				borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
					if names[0] == "router2" {
						panic("Should not be called")
					}
					cancel()
					return nil, errors.New("cancelled")
				},
			},
			assert:     assertNoTable,
			numReturns: 0,
		},
	}

	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c.pool.cancel = cancel
			table, err := readTable(ctx, c.pool, "dbname", c.routers, nil)
			c.assert(t, table, err)
			if c.numReturns != len(c.pool.returned) {
				t.Errorf("Expected %d returned connections but %d was returned", c.numReturns, len(c.pool.returned))
			}
		})
	}
}
