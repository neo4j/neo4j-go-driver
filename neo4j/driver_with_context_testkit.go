/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//go:build internal_testkit

package neo4j

import (
	"context"
	"fmt"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/router"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"time"
)

type RoutingTable = idb.RoutingTable

func SetTimer(d DriverWithContext, timer func() time.Time) {
	driver := d.(*driverWithContext)
	driver.now = timer
}

func ResetTime(d DriverWithContext) {
	driver := d.(*driverWithContext)
	driver.now = time.Now
}

func ForceRoutingTableUpdate(d DriverWithContext, database string, bookmarks []string, logger log.BoltLogger) error {
	driver := d.(*driverWithContext)
	ctx := context.Background()
	driver.router.Invalidate(database)
	getBookmarks := func(context.Context) ([]string, error) {
		return bookmarks, nil
	}
	auth := &idb.ReAuthToken{
		Manager:     driver.auth,
		FromSession: false,
		ForceReAuth: false,
	}
	_, err := driver.router.GetOrUpdateReaders(ctx, getBookmarks, database, auth, logger)
	if err != nil {
		return errorutil.WrapError(err)
	}
	_, err = driver.router.GetOrUpdateWriters(ctx, getBookmarks, database, auth, logger)
	return errorutil.WrapError(err)
}

func GetRoutingTable(d DriverWithContext, database string) (*RoutingTable, error) {
	driver := d.(*driverWithContext)
	router, ok := driver.router.(*router.Router)
	if !ok {
		return nil, fmt.Errorf("GetRoutingTable is only supported for direct drivers")
	}
	table := router.GetTable(database)
	return table, nil
}
