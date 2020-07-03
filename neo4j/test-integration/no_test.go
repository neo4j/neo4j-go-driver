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

package test_integration

import (
	"crypto/rand"
	"math"
	"math/big"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/db"
)

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Expected no error but got: %s", err)
	}
}

func assertDbRecord(t *testing.T, rec *db.Record, sum *db.Summary, err error) {
	if rec == nil || err != nil || sum != nil {
		t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
	}
}

func assertDbSummary(t *testing.T, rec *db.Record, sum *db.Summary, err error) {
	if rec != nil || err != nil || sum == nil {
		t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
	}
}

func assertDbError(t *testing.T, rec *db.Record, sum *db.Summary, err error) {
	if rec != nil || err == nil || sum != nil {
		t.Fatalf("Should only be an error, %+v, %+v, %+v", rec, sum, err)
	}
}

func randomInt() int64 {
	bid, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return bid.Int64()
}

func createRandomNode(t *testing.T, sess neo4j.Session) int64 {
	nodex, err := sess.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		res, err := tx.Run("CREATE (n:RandomNode{val: $r}) RETURN n", map[string]interface{}{"r": randomInt()})
		if err != nil {
			return nil, err
		}
		res.Next()
		return res.Record().GetByIndex(0), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	node := nodex.(neo4j.Node)
	return node.Props()["val"].(int64)
}

func findRandomNode(t *testing.T, sess neo4j.Session, randomId int64) neo4j.Node {
	nodex, err := sess.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		res, err := tx.Run("MATCH (n:RandomNode{val: $r}) RETURN n", map[string]interface{}{"r": randomId})
		if err != nil {
			return nil, err
		}
		if !res.Next() {
			return nil, nil
		}
		return res.Record().GetByIndex(0), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if nodex == nil {
		return nil
	}
	return nodex.(neo4j.Node)
}

func assertRandomNode(t *testing.T, sess neo4j.Session, randomId int64) {
	node := findRandomNode(t, sess, randomId)
	if node == nil {
		t.Error("Should have found random node but didn't")
	}
}

func assertNoRandomNode(t *testing.T, sess neo4j.Session, randomId int64) {
	node := findRandomNode(t, sess, randomId)
	if node != nil {
		t.Error("Shouldn't find random node but did")
	}
}
