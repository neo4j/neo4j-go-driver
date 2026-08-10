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

package test_integration

import (
	"context"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/test-integration/dbserver"
)

type omPerson struct {
	Name string `neo4j:"name"`
	Age  int64  `neo4j:"age"`
}

// TestObjectMapping round-trips through a real server: nodes are created from a
// struct via the input mapping and read back through the output mapping.
func TestObjectMapping(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)
	driver := server.Driver()
	defer func() { _ = driver.Close(ctx) }()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() { _ = session.Close(ctx) }()

	cleanup := func(t *testing.T) {
		result, err := session.Run(ctx, "MATCH (n:OMTest) DETACH DELETE n", nil)
		assertNil(t, err)
		_, err = result.Consume(ctx)
		assertNil(t, err)
	}
	create := func(t *testing.T, props any) {
		result, err := session.Run(ctx, "CREATE (n:OMTest) SET n = $props", map[string]any{"props": props})
		assertNil(t, err)
		_, err = result.Consume(ctx)
		assertNil(t, err)
	}
	defer cleanup(outer)

	outer.Run("CollectAs maps a node column into structs", func(t *testing.T) {
		cleanup(t)
		create(t, omPerson{Name: "Alice", Age: 30})
		create(t, omPerson{Name: "Bob", Age: 40})

		result, err := session.Run(ctx, "MATCH (n:OMTest) RETURN n ORDER BY n.name", nil)
		assertNil(t, err)
		people, err := neo4j.CollectAs[omPerson](ctx, result)
		assertNil(t, err)
		assertEquals(t, len(people), 2)
		assertEquals(t, people[0], omPerson{Name: "Alice", Age: 30})
		assertEquals(t, people[1], omPerson{Name: "Bob", Age: 40})
	})

	outer.Run("SingleAs maps a single node", func(t *testing.T) {
		cleanup(t)
		create(t, omPerson{Name: "Alice", Age: 30})

		result, err := session.Run(ctx, "MATCH (n:OMTest) RETURN n", nil)
		assertNil(t, err)
		person, err := neo4j.SingleAs[omPerson](ctx, result)
		assertNil(t, err)
		assertEquals(t, person, omPerson{Name: "Alice", Age: 30})
	})

	outer.Run("As maps scalar columns by name", func(t *testing.T) {
		cleanup(t)
		create(t, omPerson{Name: "Alice", Age: 30})

		result, err := session.Run(ctx, "MATCH (n:OMTest) RETURN n.name AS name, n.age AS age", nil)
		assertNil(t, err)
		record, err := result.Single(ctx)
		assertNil(t, err)
		person, err := neo4j.As[omPerson](record)
		assertNil(t, err)
		assertEquals(t, person, omPerson{Name: "Alice", Age: 30})
	})

	outer.Run("CollectRecordsAs maps ExecuteQuery results", func(t *testing.T) {
		cleanup(t)
		create(t, omPerson{Name: "Alice", Age: 30})

		result, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n:OMTest) RETURN n", nil, neo4j.EagerResultTransformer)
		assertNil(t, err)
		people, err := neo4j.CollectRecordsAs[omPerson](result.Records)
		assertNil(t, err)
		assertEquals(t, len(people), 1)
		assertEquals(t, people[0], omPerson{Name: "Alice", Age: 30})
	})

	outer.Run("time.Time property round-trips", func(t *testing.T) {
		type event struct {
			Name string    `neo4j:"name"`
			At   time.Time `neo4j:"at"`
		}
		cleanup(t)
		at := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
		create(t, event{Name: "launch", At: at})

		result, err := session.Run(ctx, "MATCH (n:OMTest) RETURN n", nil)
		assertNil(t, err)
		got, err := neo4j.SingleAs[event](ctx, result)
		assertNil(t, err)
		assertEquals(t, got.Name, "launch")
		assertTrue(t, got.At.Equal(at))
	})

	outer.Run("nested map maps into a nested struct", func(t *testing.T) {
		type director struct {
			Name string `neo4j:"name"`
		}
		type movie struct {
			Title    string   `neo4j:"title"`
			Director director `neo4j:"director"`
		}
		result, err := session.Run(ctx, "RETURN {title: 'Heat', director: {name: 'Mann'}} AS m", nil)
		assertNil(t, err)
		got, err := neo4j.SingleAs[movie](ctx, result)
		assertNil(t, err)
		assertEquals(t, got, movie{Title: "Heat", Director: director{Name: "Mann"}})
	})

	outer.Run("DST-ambiguous local time round-trips both instants", func(t *testing.T) {
		type event struct {
			Name string    `neo4j:"name"`
			At   time.Time `neo4j:"at"`
		}
		berlin, err := time.LoadLocation("Europe/Berlin")
		assertNil(t, err)
		// On 2025-10-26 the Berlin clock falls back, so 02:30 local occurs twice.
		pre := time.Date(2025, 10, 26, 0, 30, 0, 0, time.UTC).In(berlin)  // 02:30 CEST (UTC+2)
		post := time.Date(2025, 10, 26, 1, 30, 0, 0, time.UTC).In(berlin) // 02:30 CET (UTC+1)

		cleanup(t)
		create(t, event{Name: "post", At: post})
		create(t, event{Name: "pre", At: pre})

		result, err := session.Run(ctx, "MATCH (n:OMTest) RETURN n ORDER BY n.name", nil)
		assertNil(t, err)
		events, err := neo4j.CollectAs[event](ctx, result)
		assertNil(t, err)
		assertEquals(t, len(events), 2)
		assertTrue(t, events[0].At.Equal(post))
		assertTrue(t, events[1].At.Equal(pre))
		assertFalse(t, events[0].At.Equal(events[1].At)) // genuinely two different instants
	})

	outer.Run("type mismatch returns an error", func(t *testing.T) {
		cleanup(t)
		create(t, omPerson{Name: "Alice", Age: 30})

		type bad struct {
			Age string `neo4j:"age"` // age is stored as an integer
		}
		result, err := session.Run(ctx, "MATCH (n:OMTest) RETURN n", nil)
		assertNil(t, err)
		_, err = neo4j.SingleAs[bad](ctx, result)
		assertNotNil(t, err)
	})
}
