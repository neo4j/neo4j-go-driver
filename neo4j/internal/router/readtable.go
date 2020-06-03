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

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	poolpackage "github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

// Tries to read routing table from any of the specified routers using new or existing connection
// from the supplied pool.
func readTable(ctx context.Context, pool Pool, database string, routers []string, routerContext map[string]string) (*db.RoutingTable, error) {
	// Preserve last error to be returned, set a default for case of no routers
	var err error = &ReadRoutingTableError{}

	// Try the routers one at the time since some of them might no longer support routing and we
	// can't force the pool to not re-use these when putting them back in the pool and retrieving
	// another connection.
	for _, router := range routers {
		var conn poolpackage.Connection
		if conn, err = pool.Borrow(ctx, []string{router}, true); err != nil {
			// Check if failed due to context timing out
			if ctx.Err() != nil {
				return nil, wrapInReadRoutingTableError(router, ctx.Err())
			}
			err = wrapInReadRoutingTableError(router, err)
			continue
		}
		defer pool.Return(conn)

		discovery, ok := conn.(db.ClusterDiscovery)
		if !ok {
			err = &db.RoutingNotSupportedError{Server: conn.ServerName()}
			err = wrapInReadRoutingTableError(router, err)
			continue
		}

		var table *db.RoutingTable
		table, err = discovery.GetRoutingTable(database, routerContext)
		if err == nil {
			return table, nil
		}
		err = wrapInReadRoutingTableError(router, err)
	}
	return nil, err
}
