//go:build internal_neo4j_go_driver_time_mock

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

package router

import (
	"context"
	"sync"
	"testing"
	"time"

	iauth "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/db"
	pool2 "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/log"
)

// homeDbPool resolves an empty database to the user's home database, as the
// server does. Borrows block until release is closed.
type homeDbPool struct {
	homeDb  map[string]string
	release chan struct{}
	entered chan struct{}
	once    sync.Once

	mut     sync.Mutex
	borrows int
	users   []string
}

func (p *homeDbPool) Borrow(_ context.Context, _ func() []string, _ bool, _ log.BoltLogger, _ time.Duration, auth *db.ReAuthToken) (db.Connection, error) {
	p.mut.Lock()
	p.borrows++
	p.mut.Unlock()
	p.once.Do(func() { close(p.entered) })
	<-p.release

	principal := principalOf(auth)
	return &testutil.ConnFake{
		GetRoutingTableHook: func(database, impersonatedUser string) (*db.RoutingTable, error) {
			user := impersonatedUser
			if user == "" {
				user = principal
			}
			p.mut.Lock()
			p.users = append(p.users, user)
			p.mut.Unlock()

			resolved := database
			if resolved == "" {
				resolved = p.homeDb[user]
			}
			return &db.RoutingTable{
				DatabaseName: resolved,
				TimeToLive:   100,
				Readers:      []string{resolved + "-reader"},
			}, nil
		},
	}, nil
}

func (p *homeDbPool) Return(context.Context, db.Connection) {}

func (p *homeDbPool) recorded() []string {
	p.mut.Lock()
	defer p.mut.Unlock()
	return append([]string(nil), p.users...)
}

func principalOf(auth *db.ReAuthToken) string {
	if auth == nil || auth.Manager == nil {
		return ""
	}
	token, err := auth.Manager.GetAuthToken(context.Background())
	if err != nil {
		return ""
	}
	principal, _ := token.Tokens["principal"].(string)
	return principal
}

func sessionAuth(principal string) *db.ReAuthToken {
	return &db.ReAuthToken{
		Manager:     iauth.Token{Tokens: map[string]any{"principal": principal}},
		FromSession: true,
	}
}

type routerCall struct {
	selection db.DatabaseSelection
	auth      *db.ReAuthToken
}

type routerResult struct {
	readers []string
	pinned  string
	err     error
}

// awaitSecondCaller blocks until the second caller has either queued behind the
// first or started its own read.
func awaitSecondCaller(t *testing.T, r *Router, pool *homeDbPool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.dbRoutersMut.Lock()
		waiting := 0
		for _, waiters := range r.updating {
			waiting += len(waiters)
		}
		r.dbRoutersMut.Unlock()

		pool.mut.Lock()
		borrows := pool.borrows
		pool.mut.Unlock()

		if waiting >= 1 || borrows >= 2 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("second caller never queued or started its own fetch")
}

// runConcurrently holds first inside its routing table read until second has
// committed to a path, then releases both.
func runConcurrently(t *testing.T, homeDb map[string]string, first, second routerCall) ([]routerResult, []string) {
	t.Helper()

	pool := &homeDbPool{homeDb: homeDb, release: make(chan struct{}), entered: make(chan struct{})}
	router := New("router", func() []string { return []string{} }, nil, pool, pool2.DefaultConnectionLivenessCheckTimeout, logger, "routerid")

	results := make([]routerResult, 2)
	var wg sync.WaitGroup
	start := func(i int, call routerCall) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].readers, results[i].err = router.GetOrUpdateReaders(
				context.Background(), nilBookmarks, call.selection, call.auth, nil,
				func(database string) { results[i].pinned = database },
			)
		}()
	}

	start(0, first)
	<-pool.entered
	start(1, second)
	awaitSecondCaller(t, router, pool)
	close(pool.release)
	wg.Wait()

	for i, result := range results {
		if result.err != nil {
			t.Fatalf("caller %d: unexpected error: %v", i, result.err)
		}
	}
	return results, pool.recorded()
}

// TestConcurrentHomeDbResolutionIsPerUser covers two callers sharing a cached
// home database guess that is stale for the second.
func TestConcurrentHomeDbResolutionIsPerUser(t *testing.T) {
	homeDb := map[string]string{"alice": "shared-db", "bob": "bob-db"}
	guess := db.DatabaseSelection{Name: "shared-db", IsHomeDbGuess: true}

	// The home database cache key covers impersonation and session auth, so both cases
	// key on it.
	cases := []struct {
		name      string
		first     routerCall
		second    routerCall
		wantUsers []string
	}{
		{
			name:      "impersonated user",
			first:     routerCall{selection: impersonating(guess, "alice")},
			second:    routerCall{selection: impersonating(guess, "bob")},
			wantUsers: []string{"alice", "bob"},
		},
		{
			name:      "session auth",
			first:     routerCall{selection: withHomeDbCacheKey(guess, "alice"), auth: sessionAuth("alice")},
			second:    routerCall{selection: withHomeDbCacheKey(guess, "bob"), auth: sessionAuth("bob")},
			wantUsers: []string{"alice", "bob"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			results, requests := runConcurrently(t, homeDb, c.first, c.second)

			assertUsers(t, requests, c.wantUsers)
			assertResolved(t, results[0], "shared-db")
			assertResolved(t, results[1], "bob-db")
		})
	}
}

// TestConcurrentReadsShareOneRequest covers the cases where one read must still serve
// both callers: a named database, which is not a home database resolution, and two
// callers that are the same user.
func TestConcurrentReadsShareOneRequest(t *testing.T) {
	named := db.DatabaseSelection{Name: "named-db"}
	guess := db.DatabaseSelection{Name: "shared-db", IsHomeDbGuess: true}

	cases := []struct {
		name     string
		first    routerCall
		second   routerCall
		database string
	}{
		{
			name:     "named database",
			first:    routerCall{selection: impersonating(named, "alice")},
			second:   routerCall{selection: impersonating(named, "bob")},
			database: "named-db",
		},
		{
			name:     "same user",
			first:    routerCall{selection: impersonating(guess, "alice")},
			second:   routerCall{selection: impersonating(guess, "alice")},
			database: "shared-db",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			homeDb := map[string]string{"alice": "shared-db"}
			results, requests := runConcurrently(t, homeDb, c.first, c.second)

			if len(requests) != 1 {
				t.Errorf("read was not shared, got %d requests: %v", len(requests), requests)
			}
			for _, result := range results {
				assertReaders(t, result, c.database)
			}
		})
	}
}

func withHomeDbCacheKey(selection db.DatabaseSelection, user string) db.DatabaseSelection {
	selection.HomeDbCacheKey = user
	return selection
}

func impersonating(selection db.DatabaseSelection, user string) db.DatabaseSelection {
	selection = withHomeDbCacheKey(selection, user)
	selection.ImpersonatedUser = user
	return selection
}

func assertUsers(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d routing table requests %v, want %d %v", len(got), got, len(want), want)
	}
	seen := make(map[string]bool, len(got))
	for _, user := range got {
		seen[user] = true
	}
	for _, user := range want {
		if !seen[user] {
			t.Errorf("no routing table request was made for %q, got %v", user, got)
		}
	}
}

func assertReaders(t *testing.T, result routerResult, database string) {
	t.Helper()
	reader := database + "-reader"
	if len(result.readers) != 1 || result.readers[0] != reader {
		t.Errorf("got readers %v, want [%s]", result.readers, reader)
	}
}

func assertResolved(t *testing.T, result routerResult, database string) {
	t.Helper()
	assertReaders(t, result, database)
	if result.pinned != database {
		t.Errorf("pinned %q, want %q", result.pinned, database)
	}
}
