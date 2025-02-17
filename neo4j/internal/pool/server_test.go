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

package pool

import (
	"context"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func closeConnection(ctx context.Context, conn db.Connection) {
	conn.Close(ctx)
}

func TestServer(ot *testing.T) {

	assertSize := func(t *testing.T, s *server, expected int) {
		t.Helper()
		actual := s.size()
		if actual != expected {
			t.Errorf("Size of connection pool was %d but %d expected", actual, expected)
		}
	}

	assertConnection := func(t *testing.T, conn db.Connection) {
		t.Helper()
		if conn == nil {
			t.Fatal("Expected connection")
		}
	}

	assertNilConnection := func(t *testing.T, conn db.Connection) {
		t.Helper()
		if conn != nil {
			t.Fatal("Expected nil connection")
		}
	}

	ot.Run("registerBusy/unregisterBusy/size", func(t *testing.T) {
		s := NewServer()
		assertSize(t, s, 0)

		// Register should increase size
		c1 := &testutil.ConnFake{}
		s.registerBusy(c1)
		assertSize(t, s, 1)
		c2 := &testutil.ConnFake{}
		s.registerBusy(c2)
		assertSize(t, s, 2)

		// Unregister should decrease size
		s.unregisterBusy(c2)
		assertSize(t, s, 1)
		s.unregisterBusy(c1)
		assertSize(t, s, 0)
	})

	ot.Run("getIdle/returnBusy", func(t *testing.T) {
		s := NewServer()
		c1 := &testutil.ConnFake{Alive: true}
		registerIdle(s, c1)

		c2 := s.getIdle()
		assertConnection(t, c2)
		c3 := s.getIdle()
		assertNilConnection(t, c3)

		s.returnBusy(context.Background(), c2, closeConnection)
		c3 = s.getIdle()
		assertConnection(t, c3)
	})

	ot.Run("removeIdleOlderThan", func(t *testing.T) {
		s := NewServer()
		// Register and return three connections
		now := time.Now()
		conns, _ := populateServer(s, now, 3, 0)

		// Let the connection in the middle be too old
		conns[1].Birth = now.Add(-20 * time.Second)
		s.removeIdleOlderThan(context.Background(), now, 10*time.Second, closeConnection)
		assertSize(t, s, 2)

		// Should be able to borrow twice
		b1 := s.getIdle()
		assertConnection(t, b1)
		b2 := s.getIdle()
		assertConnection(t, b2)
		b3 := s.getIdle()
		assertNilConnection(t, b3)

		// Return the connections and let all of them be too old
		s.returnBusy(context.Background(), b1, closeConnection)
		s.returnBusy(context.Background(), b2, closeConnection)
		conns[0].Birth = now.Add(-20 * time.Second)
		conns[2].Birth = now.Add(-20 * time.Second)
		s.removeIdleOlderThan(context.Background(), now, 10*time.Second, closeConnection)

		// Shouldn't be able to borrow anything and size should be zero
		b1 = s.getIdle()
		assertNilConnection(t, b1)
		assertSize(t, s, 0)
	})

	ot.Run("startClosing clears idle connections", func(t *testing.T) {
		s := NewServer()
		// Register and return three connections
		_, _ = populateServer(s, time.Now(), 3, 3)
		s.startClosing(context.Background(), closeConnection)
		if s.idle.Len() != 0 {
			t.Errorf("Expected 0 idle connections, found %d", s.idle.Len())
		}
		if s.busy.Len() != 3 {
			t.Errorf("Expected 3 busy connections, found %d", s.busy.Len())
		}
	})

	ot.Run("closeAll clears all connections", func(t *testing.T) {
		s := NewServer()
		// Register and return three connections
		_, _ = populateServer(s, time.Now(), 3, 3)
		s.closeAll(context.Background(), closeConnection)
		if s.idle.Len() != 0 {
			t.Errorf("Expected 0 idle connections, found %d", s.idle.Len())
		}
		if s.busy.Len() != 0 {
			t.Errorf("Expected 0 busy connections, found %d", s.busy.Len())
		}
	})
}

func TestServerPenalty(t *testing.T) {
	assertPenaltiesGreaterThan := func(s1, s2 *server, now time.Time) {
		t.Helper()
		p1 := s1.calculatePenalty(now)
		p2 := s2.calculatePenalty(now)
		if p1 <= p2 {
			t.Errorf("Expected penalty for first server to be higher than second server, 0x%x vs 0x%x", p1, p2)
		}
	}

	now := time.Now()
	srv1 := NewServer()
	srv2 := NewServer()

	// Add one busy connection to srv1
	// Higher penalty to srv1 since it is in use
	c11 := &testutil.ConnFake{Id: 11, Alive: true}
	srv1.registerBusy(c11)
	assertPenaltiesGreaterThan(srv1, srv2, now)

	// Return the busy connection to srv1
	// Now srv2 should have higher penalty than srv1 since using srv2 would require a new
	// connection.
	srv1.returnBusy(context.Background(), c11, closeConnection)
	assertPenaltiesGreaterThan(srv2, srv1, now)

	// Add an idle connection to srv2 to make both servers have one idle connection each.
	c21 := &testutil.ConnFake{Id: 21, Alive: true}
	registerIdle(srv2, c21)

	// At this point round-robin should kick in to even out what server to use, since
	// srv2 was last in use, srv1 should have lower penalty at this point.
	assertPenaltiesGreaterThan(srv2, srv1, now)

	// Get the connection from srv1 and return it, now srv1 should have higher penalty.
	ctx := context.Background()
	idle := srv1.getIdle()
	_, _ = srv1.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	testutil.AssertDeepEquals(t, idle, c11)
	srv1.returnBusy(context.Background(), c11, closeConnection)
	assertPenaltiesGreaterThan(srv1, srv2, now)

	// Add one more connection each to the servers
	c12 := &testutil.ConnFake{Id: 12, Alive: true}
	registerIdle(srv1, c12)
	c22 := &testutil.ConnFake{Id: 22, Alive: true}
	registerIdle(srv2, c22)

	// Both servers have two idle connections, srv2 was last used, so it should have higher penalty.
	assertPenaltiesGreaterThan(srv2, srv1, now)
	// Get both idle connections from srv1
	idle = srv1.getIdle()
	_, _ = srv1.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	idle = srv1.getIdle()
	_, _ = srv1.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	// Get one idle connection from srv2
	idle = srv2.getIdle()
	_, _ = srv2.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	// Since more connections are in use on srv1, it should have higher penalty even though
	// srv2 was last used
	assertPenaltiesGreaterThan(srv1, srv2, now)
	// Return the connections
	idle = srv2.getIdle()
	_, _ = srv2.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	srv2.returnBusy(context.Background(), c21, closeConnection)
	srv2.returnBusy(context.Background(), c22, closeConnection)
	srv1.returnBusy(context.Background(), c11, closeConnection)
	srv1.returnBusy(context.Background(), c12, closeConnection)
	// Everything returned, srv2 should have higher penalty since it was last used
	assertPenaltiesGreaterThan(srv2, srv1, now)

	// Punish srv1 by faking that a connection to it failed, after that it should have much higher
	// penalty than srv2
	srv1.notifyFailedConnect(now)
	assertPenaltiesGreaterThan(srv1, srv2, now)
	testutil.AssertTrue(t, srv1.hasFailedConnect(now))
	testutil.AssertFalse(t, srv2.hasFailedConnect(now))
	// Use srv2 to the max
	idle = srv2.getIdle()
	_, _ = srv2.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	idle = srv2.getIdle()
	_, _ = srv2.healthCheck(ctx, idle, DefaultConnectionLivenessCheckTimeout, nil, nil)
	// Even at this point we should prefer srv2
	assertPenaltiesGreaterThan(srv1, srv2, now)

	// Emulate that enough time has passed to have forgotten about the failure, now we
	// should prefer srv1
	assertPenaltiesGreaterThan(srv2, srv1, now.Add(3*time.Hour))

	// Alternatively a successful connect should clear the problem
	srv1.notifySuccessfulConnect()
	assertPenaltiesGreaterThan(srv2, srv1, now)
}

func TestIdlenessThreshold(outer *testing.T) {

	outer.Run("does not reset connections below idleness threshold", func(t *testing.T) {
		resetCalled := false
		connection := &testutil.ConnFake{
			Alive: true,
			ForceResetHook: func() {
				resetCalled = true
			},
		}
		srv := NewServer()
		registerIdle(srv, connection)

		idleConnection := srv.getIdle()

		testutil.AssertFalse(t, resetCalled)
		testutil.AssertDeepEquals(t, connection, idleConnection)
		testutil.AssertIntEqual(t, srv.size(), 1)
		testutil.AssertIntEqual(t, srv.numIdle(), 0)
		testutil.AssertIntEqual(t, srv.numBusy(), 1)
	})

	outer.Run("resets connections idle for too long", func(t *testing.T) {
		resetCalled := false
		connection := &testutil.ConnFake{
			Alive: true,
			Idle:  time.Now().Add(-2 * time.Hour),
			ForceResetHook: func() {
				resetCalled = true
			},
		}
		srv := NewServer()
		registerIdle(srv, connection)

		idleConnection := srv.getIdle()
		testutil.AssertNotNil(t, idleConnection)
		healthy, err := srv.healthCheck(context.Background(), idleConnection, 1*time.Hour, nil, nil)

		testutil.AssertNil(t, err)
		testutil.AssertTrue(t, healthy)
		testutil.AssertTrue(t, resetCalled)
		testutil.AssertDeepEquals(t, connection, idleConnection)
		testutil.AssertIntEqual(t, srv.size(), 1)
		testutil.AssertIntEqual(t, srv.numIdle(), 0)
		testutil.AssertIntEqual(t, srv.numBusy(), 1)
	})

	outer.Run("purges long-idle connections when reset fails", func(t *testing.T) {
		connection := &testutil.ConnFake{
			Alive: true,
			Idle:  time.Now().Add(-2 * time.Hour),
		}
		connection.ForceResetHook = func() {
			connection.Alive = false
		}
		srv := NewServer()
		registerIdle(srv, connection)

		idleConnection := srv.getIdle()
		testutil.AssertNotNil(t, idleConnection)
		healthy, err := srv.healthCheck(context.Background(), idleConnection, 1*time.Hour, nil, nil)

		testutil.AssertNil(t, err)
		testutil.AssertFalse(t, healthy)
		testutil.AssertFalse(t, connection.IsAlive())
		testutil.AssertIntEqual(t, srv.size(), 1)
		testutil.AssertIntEqual(t, srv.numIdle(), 0)
		testutil.AssertIntEqual(t, srv.numBusy(), 1)
	})
}

func populateServer(s *server, birth time.Time, idle, busy int) ([]*testutil.ConnFake, []*testutil.ConnFake) {
	idleConns := make([]*testutil.ConnFake, idle)
	for i := range idleConns {
		c := &testutil.ConnFake{Birth: birth, Alive: true}
		idleConns[i] = c
		registerIdle(s, c)
	}
	busyConns := make([]*testutil.ConnFake, busy)
	for i := range busyConns {
		c := &testutil.ConnFake{Birth: birth, Alive: true}
		busyConns[i] = c
		s.registerBusy(c)
	}
	return idleConns, busyConns
}

func registerIdle(srv *server, connection db.Connection) {
	srv.registerBusy(connection)
	srv.returnBusy(context.Background(), connection, closeConnection)
}
