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

package test_integration

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
	"testing"
)

func TestContext(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)

	outer.Run("server does not hold on transaction when driver cancels context", func(t *testing.T) {
		driver := server.Driver()
		defer driver.Close(ctx)
		session := driver.NewSession(ctx, neo4j.SessionConfig{FetchSize: 1})
		defer session.Close(ctx)
		tx, err := session.BeginTransaction(ctx)
		assertNil(t, err)
		defer tx.Close(ctx)
		results, err := tx.Run(ctx, "UNWIND [1,2,3] AS x RETURN x", nil)
		assertNil(t, err)
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err = results.Consume(canceledCtx)

		assertStringContains(t, err.Error(), "context canceled")
		workloads := listTransactionWorkloads(ctx, driver, server)
		// TODO: replace length assertion with query assertion when https://trello.com/c/G14xMoBG is fixed
		assertEquals(t, len(workloads), 0)
	})
}

func listTransactionWorkloads(ctx context.Context, driver neo4j.DriverWithContext, server dbserver.DbServer) []string {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	transactionQuery := server.GetTransactionWorkloadsQuery()
	results, err := session.Run(ctx, transactionQuery, nil)
	if err != nil {
		panic(err)
	}
	records, err := results.Collect(ctx)
	if err != nil {
		panic(err)
	}
	workloads := make([]string, 0, len(records)-1)
	for _, record := range records {
		rawQuery, _ := record.Get("query")
		query := rawQuery.(string)
		if query != transactionQuery {
			workloads = append(workloads, query)
		}
	}
	return workloads
}
