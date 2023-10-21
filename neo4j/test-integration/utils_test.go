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
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test_integration

import (
	"context"
	"fmt"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
	"reflect"
	"sort"
	"testing"

	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j"
)

func intReturningWork(ctx context.Context, t *testing.T, query string, params map[string]any) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		results, err := tx.Run(ctx, query, params)
		assertNil(t, err)

		record, err := results.Single(ctx)
		assertNil(t, err)
		result, ok := record.Values[0].(int64)
		assertTrue(t, ok)
		return result, nil
	}
}

func transactionWithIntWork(t *testing.T, tx neo4j.ExplicitTransaction, work neo4j.ManagedTransactionWork) int64 {
	result, err := work(tx)
	assertNil(t, err)

	return result.(int64)
}

func readTransactionWithIntWork(ctx context.Context, t *testing.T, session neo4j.SessionWithContext, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) int64 {
	result, err := session.ExecuteRead(ctx, work, configurers...)
	assertNil(t, err)

	return result.(int64)
}

func writeTransactionWithIntWork(ctx context.Context, t *testing.T, session neo4j.SessionWithContext, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) int64 {
	result, err := session.ExecuteWrite(ctx, work, configurers...)
	assertNil(t, err)

	return result.(int64)
}

func newSessionAndTx(ctx context.Context, t *testing.T, driver neo4j.DriverWithContext, mode neo4j.AccessMode, configurers ...func(*neo4j.TransactionConfig)) (neo4j.SessionWithContext, neo4j.ExplicitTransaction) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: mode})

	tx, err := session.BeginTransaction(ctx, configurers...)
	assertNil(t, err)

	return session, tx
}

func createNode(ctx context.Context, t *testing.T, session neo4j.SessionWithContext, label string, props map[string]any) {
	var (
		err     error
		result  neo4j.ResultWithContext
		summary neo4j.ResultSummary
	)

	if len(props) > 0 {
		result, err = session.Run(ctx, fmt.Sprintf("CREATE (n:%s) SET n = $props", label), map[string]any{"props": props})
	} else {
		result, err = session.Run(ctx, fmt.Sprintf("CREATE (n:%s)", label), nil)
	}
	assertNil(t, err)

	summary, err = result.Consume(ctx)
	assertNil(t, err)

	assertEquals(t, summary.Counters().NodesCreated(), 1)
}

func updateNodeInTx(ctx context.Context, t *testing.T, tx neo4j.ExplicitTransaction, label string, newProps map[string]any) {
	var (
		err     error
		result  neo4j.ResultWithContext
		summary neo4j.ResultSummary
	)

	if len(newProps) == 0 {
		t.Fatal("newProps is empty")
	}

	result, err = tx.Run(ctx, fmt.Sprintf("MATCH (n:%s) SET n = $props", label), map[string]any{"props": newProps})
	assertNil(t, err)

	summary, err = result.Consume(ctx)
	assertNil(t, err)

	assertTrue(t, summary.Counters().ContainsUpdates())
}

func updateNodeWork(ctx context.Context, t *testing.T, label string, newProps map[string]any) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		var (
			err    error
			result neo4j.ResultWithContext
		)

		if len(newProps) == 0 {
			t.Fatal("newProps is empty")
		}

		result, err = tx.Run(ctx, fmt.Sprintf("MATCH (n:%s) SET n = $props", label), map[string]any{"props": newProps})
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}
}

func listTransactionsAndMatchMetadataWork(ctx context.Context, version dbserver.Version, metadata map[string]any) neo4j.ManagedTransactionWork {
	query := "CALL dbms.listTransactions()"
	if version.GreaterThanOrEqual(dbserver.VersionOf("4.4.0")) {
		query = "SHOW TRANSACTIONS YIELD metaData RETURN metaData"
	}
	return func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, nil)
		if err != nil {
			return nil, err
		}

		var matched = false
		for result.Next(ctx) {
			if txMetadataInt, ok := result.Record().Get("metaData"); ok {
				if txMetadata, ok := txMetadataInt.(map[string]any); ok {
					if reflect.DeepEqual(metadata, txMetadata) {
						matched = true
						break
					}
				}
			}
		}
		if err = result.Err(); err != nil {
			return nil, err
		}

		return matched, nil
	}
}

func sortedKeys(m map[string]any) []string {
	result := make([]string, len(m))
	i := 0
	for k := range m {
		result[i] = k
		i++
	}
	sort.Strings(result)
	return result
}
