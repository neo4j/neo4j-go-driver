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
	"crypto/rand"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/dbtype"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/test-integration/dbserver"

	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j"
)

// Not the best place for this...
var (
	V340 = dbserver.VersionOf("3.4.0")
	V350 = dbserver.VersionOf("3.5.0")
	V4   = dbserver.VersionOf("4.0.0")
	V42  = dbserver.VersionOf("4.2.0")
)

type contextCloser interface {
	Close(ctx context.Context) error
}

func assertCloses(ctx context.Context, t *testing.T, closer contextCloser) {
	t.Helper()
	assertNil(t, closer.Close(ctx))
}

func assertAssignableToTypeOf(t *testing.T, x, y any) {
	t.Helper()
	xType := reflect.TypeOf(x)
	yType := reflect.TypeOf(y)
	if !xType.AssignableTo(yType) {
		t.Fatalf("expected %v to be assignable to type of %v, but was not", x, y)
	}
}

func assertNil(t *testing.T, v any) {
	t.Helper()
	if !isNil(v) {
		t.Fatalf("expected nil (or default value), got %+v", v)
	}
}

func assertNotNil(t *testing.T, v any) {
	t.Helper()
	if isNil(v) {
		t.Fatalf("expected not nil, got nil")
	}
}

func assertEquals(t *testing.T, a, b any) {
	t.Helper()

	if reflect.TypeOf(a).Kind() == reflect.Slice && reflect.TypeOf(b).Kind() == reflect.Slice {
		assertSliceEquals(t, a, b)
		return
	}
	if reflect.TypeOf(a).Kind() == reflect.Array && reflect.TypeOf(b).Kind() == reflect.Array {
		assertArrayEquals(t, a, b)
		return
	}
	convertedA := a
	if a != nil && b != nil && reflect.TypeOf(a).ConvertibleTo(reflect.TypeOf(b)) {
		convertedA = reflect.ValueOf(a).Convert(reflect.TypeOf(b)).Interface()
	}
	if !reflect.DeepEqual(convertedA, b) {
		t.Fatalf("expected %+v to equal %+v, but did not", a, b)
	}
}

func assertSliceEquals(t *testing.T, a, b any) {
	t.Helper()

	valueA := reflect.ValueOf(a)
	valueB := reflect.ValueOf(b)
	lengthA := valueA.Len()
	if lengthA != valueB.Len() {
		t.Fatalf("expected %+v to equal %+v, but did not", a, b)
	}
	for i := 0; i < lengthA; i++ {
		assertEquals(t,
			valueA.Index(i).Interface(),
			valueB.Index(i).Interface(),
		)
	}
}

func assertArrayEquals(t *testing.T, a, b any) {
	t.Helper()

	valueA := reflect.ValueOf(a)
	valueB := reflect.ValueOf(b)
	lengthA := reflect.TypeOf(a).Len()
	if lengthA != reflect.TypeOf(b).Len() {
		t.Fatalf("expected %+v to equal %+v, but did not", a, b)
	}
	for i := 0; i < lengthA; i++ {
		assertEquals(t,
			valueA.Index(i).Interface(),
			valueB.Index(i).Interface(),
		)
	}
}

func assertNotEquals(t *testing.T, a, b any) {
	t.Helper()

	convertedA := a
	if a != nil && b != nil && reflect.TypeOf(a).ConvertibleTo(reflect.TypeOf(b)) {
		convertedA = reflect.ValueOf(a).Convert(reflect.TypeOf(b)).Interface()
	}
	if reflect.DeepEqual(convertedA, b) {
		t.Fatalf("expected %+v to not equal %+v, but did", a, b)
	}
}

func assertStringsNotEmpty(t *testing.T, xs []string) {
	t.Helper()
	if len(xs) == 0 {
		t.Fatalf("expected %+v to be not empty, but was empty", xs)
	}
}

func assertStringsHas(t *testing.T, xs []string, x string) {
	t.Helper()
	for _, s := range xs {
		if s == x {
			return
		}
	}
	t.Fatalf("expected %+v to contain %s but did not", xs, x)
}

func assertNodesHas(t *testing.T, xs []dbtype.Node, x dbtype.Node) {
	t.Helper()
	for _, s := range xs {
		if reflect.DeepEqual(x, s) {
			return
		}
	}
	t.Fatalf("expected %+v to contain %v but did not", xs, x)
}

func assertRelationshipsHas(t *testing.T, xs []dbtype.Relationship, x dbtype.Relationship) {
	t.Helper()
	for _, s := range xs {
		if reflect.DeepEqual(x, s) {
			return
		}
	}
	t.Fatalf("expected %+v to contain %v but did not", xs, x)
}

func assertStringsEmpty(t *testing.T, xs []string) {
	t.Helper()
	if len(xs) != 0 {
		t.Fatalf("expected %+v to be empty, but was", xs)
	}
}

func assertTrue(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Fatalf("expected true but was false")
	}
}

func assertFalse(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Fatalf("expected false but was true")
	}
}

func assertStringContains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected %q to contain %q but did not", s, sub)
	}
}

func assertMapHas(t *testing.T, m map[string]any, k string, v any) {
	t.Helper()
	value, found := m[k]
	if !found {
		t.Fatalf("map %v does not have key %s", m, k)
	}
	if !reflect.DeepEqual(v, value) {
		t.Fatalf("map %v value %v at key %s does not equal %v", m, value, k, v)
	}
}

func randomInt() int64 {
	bid, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return bid.Int64()
}

func createRandomNode(ctx context.Context, t *testing.T, sess neo4j.SessionWithContext) int64 {
	nodex, err := sess.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, "CREATE (n:RandomNode{val: $r}) RETURN n", map[string]any{"r": randomInt()})
		if err != nil {
			return nil, err
		}
		res.Next(ctx)
		return res.Record().Values[0], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	node := nodex.(neo4j.Node)
	return node.Props["val"].(int64)
}

func findRandomNode(ctx context.Context, t *testing.T, sess neo4j.SessionWithContext, randomId int64) *neo4j.Node {
	nodex, err := sess.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, "MATCH (n:RandomNode{val: $r}) RETURN n", map[string]any{"r": randomId})
		if err != nil {
			return nil, err
		}
		if !res.Next(ctx) {
			return nil, nil
		}
		return res.Record().Values[0], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if nodex == nil {
		return nil
	}
	node := nodex.(neo4j.Node)
	return &node
}

func assertRandomNode(ctx context.Context, t *testing.T, sess neo4j.SessionWithContext, randomId int64) {
	node := findRandomNode(ctx, t, sess, randomId)
	if node == nil {
		t.Error("Should have found random node but didn't")
	}
}

func assertNoRandomNode(ctx context.Context, t *testing.T, sess neo4j.SessionWithContext, randomId int64) {
	node := findRandomNode(ctx, t, sess, randomId)
	if node != nil {
		t.Error("Shouldn't find random node but did")
	}
}

// from https://github.com/onsi/gomega
func isNil(a any) bool {
	if a == nil {
		return true
	}
	switch reflect.TypeOf(a).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflect.ValueOf(a).IsNil()
	}
	return false
}
