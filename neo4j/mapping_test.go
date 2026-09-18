/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

package neo4j

import (
	"context"
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/db"
	. "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

type person struct {
	Name string `neo4j:"name"`
	Age  int64  `neo4j:"age"`
}

func TestAs(t *testing.T) {
	t.Parallel()
	props := map[string]any{"name": "Alice", "age": int64(30)}
	alice := person{Name: "Alice", Age: 30}

	t.Run("single node column unwraps to its properties", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"p"}, Values: []any{Node{Props: props}}}
		got, err := As[person](rec)
		assertMapped(t, got, err, alice)
	})
	t.Run("single relationship column unwraps to its properties", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"r"}, Values: []any{Relationship{Props: props}}}
		got, err := As[person](rec)
		assertMapped(t, got, err, alice)
	})
	t.Run("single map column unwraps directly", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"m"}, Values: []any{props}}
		got, err := As[person](rec)
		assertMapped(t, got, err, alice)
	})
	t.Run("multiple columns map by name", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"name", "age"}, Values: []any{"Alice", int64(30)}}
		got, err := As[person](rec)
		assertMapped(t, got, err, alice)
	})
	t.Run("single scalar column maps by column name", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"name"}, Values: []any{"Alice"}}
		got, err := As[person](rec)
		assertMapped(t, got, err, person{Name: "Alice"})
	})
	t.Run("pointer target is allocated", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"p"}, Values: []any{Node{Props: props}}}
		got, err := As[*person](rec)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || *got != alice {
			t.Fatalf("As[*person] = %#v, want %#v", got, &alice)
		}
	})
	t.Run("multi-hop pointer target is allocated", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"p"}, Values: []any{Node{Props: props}}}
		got, err := As[**person](rec)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || **got != alice {
			t.Fatalf("As[**person] = %#v, want %#v", got, &alice)
		}
	})
	t.Run("nil record returns a usage error", func(t *testing.T) {
		t.Parallel()
		_, err := As[person](nil)
		if _, ok := err.(*UsageError); !ok {
			t.Fatalf("expected *UsageError, got %T", err)
		}
	})
	t.Run("mapping error is propagated", func(t *testing.T) {
		t.Parallel()
		rec := &db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"age": "thirty"}}}}
		if _, err := As[person](rec); err == nil {
			t.Fatal("expected error for type mismatch")
		}
	})
}

func TestCollectAs(t *testing.T) {
	t.Parallel()
	result := recordResult(
		&db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Alice", "age": int64(30)}}}},
		&db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Bob", "age": int64(40)}}}},
	)
	got, err := CollectAs[person](context.Background(), result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []person{{Name: "Alice", Age: 30}, {Name: "Bob", Age: 40}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CollectAs = %#v, want %#v", got, want)
	}
}

func TestCollectAsPropagatesMappingError(t *testing.T) {
	t.Parallel()
	result := recordResult(
		&db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"age": "thirty"}}}},
	)
	if _, err := CollectAs[person](context.Background(), result); err == nil {
		t.Fatal("expected mapping error to propagate")
	}
}

func TestCollectRecordsAs(t *testing.T) {
	t.Parallel()
	records := []*db.Record{
		{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Alice", "age": int64(30)}}}},
		{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Bob", "age": int64(40)}}}},
	}
	got, err := CollectRecordsAs[person](records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []person{{Name: "Alice", Age: 30}, {Name: "Bob", Age: 40}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CollectRecordsAs = %#v, want %#v", got, want)
	}
}

func TestCollectRecordsAsPropagatesMappingError(t *testing.T) {
	t.Parallel()
	records := []*db.Record{
		{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"age": "thirty"}}}},
	}
	if _, err := CollectRecordsAs[person](records); err == nil {
		t.Fatal("expected mapping error to propagate")
	}
}

func TestSingleAs(t *testing.T) {
	t.Parallel()
	result := recordResult(
		&db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Alice", "age": int64(30)}}}},
	)
	got, err := SingleAs[person](context.Background(), result)
	assertMapped(t, got, err, person{Name: "Alice", Age: 30})
}

func TestSingleAsErrorsWithoutExactlyOneRecord(t *testing.T) {
	t.Parallel()
	result := recordResult(
		&db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Alice"}}}},
		&db.Record{Keys: []string{"p"}, Values: []any{Node{Props: map[string]any{"name": "Bob"}}}},
	)
	if _, err := SingleAs[person](context.Background(), result); err == nil {
		t.Fatal("expected error when more than one record is present")
	}
}

func assertMapped(t *testing.T, got person, err error, want person) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("mapped = %#v, want %#v", got, want)
	}
}

// recordResult builds a Result that streams the given records followed by a
// summary, matching what CollectAs and SingleAs consume.
func recordResult(records ...*db.Record) Result {
	nexts := make([]Next, 0, len(records)+1)
	for _, r := range records {
		nexts = append(nexts, Next{Record: r})
	}
	nexts = append(nexts, Next{Summary: &idb.Summary{}})
	conn := &ConnFake{Nexts: nexts}
	return newResult(conn, idb.StreamHandle(0), "", map[string]any{}, &transactionState{}, nil)
}
