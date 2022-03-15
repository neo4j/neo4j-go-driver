/*
 * Copyright (c) "Neo4j"
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

package test_integration

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func intReturningWork(t *testing.T, query string, params map[string]interface{}) neo4j.TransactionWork {
	return func(tx neo4j.Transaction) (interface{}, error) {
		create, err := tx.Run(query, params)
		assertNil(t, err)

		returnValue := int64(0)
		if create.Next() {
			returnValue = create.Record().Values[0].(int64)
		}
		assertFalse(t, create.Next())
		assertNil(t, create.Err())

		return returnValue, nil
	}
}

func transactionWithIntWork(t *testing.T, tx neo4j.Transaction, work neo4j.TransactionWork) int64 {
	result, err := work(tx)
	assertNil(t, err)

	return result.(int64)
}

func readTransactionWithIntWork(t *testing.T, session neo4j.Session, work neo4j.TransactionWork, configurers ...func(*neo4j.TransactionConfig)) int64 {
	result, err := session.ReadTransaction(work, configurers...)
	assertNil(t, err)

	return result.(int64)
}

func writeTransactionWithIntWork(t *testing.T, session neo4j.Session, work neo4j.TransactionWork, configurers ...func(*neo4j.TransactionConfig)) int64 {
	result, err := session.WriteTransaction(work, configurers...)
	assertNil(t, err)

	return result.(int64)
}

func newSessionAndTx(t *testing.T, driver neo4j.Driver, mode neo4j.AccessMode, configurers ...func(*neo4j.TransactionConfig)) (neo4j.Session, neo4j.Transaction) {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: mode})

	tx, err := session.BeginTransaction(configurers...)
	assertNil(t, err)

	return session, tx
}

func createNode(t *testing.T, session neo4j.Session, label string, props map[string]interface{}) {
	var (
		err     error
		result  neo4j.Result
		summary neo4j.ResultSummary
	)

	if len(props) > 0 {
		result, err = session.Run(fmt.Sprintf("CREATE (n:%s) SET n = $props", label), map[string]interface{}{"props": props})
	} else {
		result, err = session.Run(fmt.Sprintf("CREATE (n:%s)", label), nil)
	}
	assertNil(t, err)

	summary, err = result.Consume()
	assertNil(t, err)

	assertEquals(t, summary.Counters().NodesCreated(), 1)
}

func updateNodeInTx(t *testing.T, tx neo4j.Transaction, label string, newProps map[string]interface{}) {
	var (
		err     error
		result  neo4j.Result
		summary neo4j.ResultSummary
	)

	if len(newProps) == 0 {
		t.Fatal("newProps is empty")
	}

	result, err = tx.Run(fmt.Sprintf("MATCH (n:%s) SET n = $props", label), map[string]interface{}{"props": newProps})
	assertNil(t, err)

	summary, err = result.Consume()
	assertNil(t, err)

	assertTrue(t, summary.Counters().ContainsUpdates())
}

func updateNodeWork(t *testing.T, label string, newProps map[string]interface{}) neo4j.TransactionWork {
	return func(tx neo4j.Transaction) (interface{}, error) {
		var (
			err    error
			result neo4j.Result
		)

		if len(newProps) == 0 {
			t.Fatal("newProps is empty")
		}

		result, err = tx.Run(fmt.Sprintf("MATCH (n:%s) SET n = $props", label), map[string]interface{}{"props": newProps})
		if err != nil {
			return nil, err
		}

		return result.Consume()
	}
}

func listTransactionsAndMatchMetadataWork(metadata map[string]interface{}) neo4j.TransactionWork {
	return func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("CALL dbms.listTransactions()", nil)
		if err != nil {
			return nil, err
		}

		var matched = false
		for result.Next() {
			if txMetadataInt, ok := result.Record().Get("metaData"); ok {
				if txMetadata, ok := txMetadataInt.(map[string]interface{}); ok {
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
