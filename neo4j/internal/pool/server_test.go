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

package pool

import (
	"testing"
	"time"
)

func assertTrue(t *testing.T, v bool) {
	t.Helper()
	if !v {
		t.Error("Expected true")
	}
}

func assertFalse(t *testing.T, v bool) {
	t.Helper()
	if v {
		t.Error("Expected false")
	}
}

func TestServer(ot *testing.T) {
	assertSize := func(t *testing.T, s *server, expected int) {
		t.Helper()
		actual := s.size()
		if actual != expected {
			t.Errorf("Size of connection pool was %d but %d expected", actual, expected)
		}
	}

	assertConnection := func(t *testing.T, conn Connection) {
		t.Helper()
		if conn == nil {
			t.Fatal("Expected connection")
		}
	}

	assertNilConnection := func(t *testing.T, conn Connection) {
		t.Helper()
		if conn != nil {
			t.Fatal("Expected nil connection")
		}
	}

	ot.Run("registerBusy/unregisterBusy/size", func(t *testing.T) {
		s := &server{}
		assertSize(t, s, 0)

		// Register should increase size
		c1 := &fakeConn{}
		s.registerBusy(c1)
		assertSize(t, s, 1)
		c2 := &fakeConn{}
		s.registerBusy(c2)
		assertSize(t, s, 2)

		// Unregister should decrease size
		s.unregisterBusy(c2)
		assertSize(t, s, 1)
		s.unregisterBusy(c1)
		assertSize(t, s, 0)
	})

	ot.Run("getIdle/returnBusy", func(t *testing.T) {
		s := &server{}
		c1 := &fakeConn{}
		s.registerBusy(c1)
		s.returnBusy(c1)

		c2 := s.getIdle()
		assertConnection(t, c2)
		c3 := s.getIdle()
		assertNilConnection(t, c3)

		s.returnBusy(c2)
		c3 = s.getIdle()
		assertConnection(t, c3)
	})

	ot.Run("removeIdleOlderThan", func(t *testing.T) {
		s := &server{}
		// Register and return three connections
		conns := make([]*fakeConn, 3)
		now := time.Now()
		for i := range conns {
			c := &fakeConn{birthdate: now}
			conns[i] = c
			s.registerBusy(c)
			s.returnBusy(c)
		}

		// Let the conn in the middle be too old
		conns[1].birthdate = now.Add(-20 * time.Second)
		s.removeIdleOlderThan(now, 10*time.Second)
		assertSize(t, s, 2)

		// Should be able to borrow twice
		b1 := s.getIdle()
		assertConnection(t, b1)
		b2 := s.getIdle()
		assertConnection(t, b2)
		b3 := s.getIdle()
		assertNilConnection(t, b3)

		// Return the connections and let all of them be too old
		s.returnBusy(b1)
		s.returnBusy(b2)
		conns[0].birthdate = now.Add(-20 * time.Second)
		conns[2].birthdate = now.Add(-20 * time.Second)
		s.removeIdleOlderThan(now, 10*time.Second)

		// Shouldn't be able to borrow anything and size should be zero
		b1 = s.getIdle()
		assertNilConnection(t, b1)
		assertSize(t, s, 0)
	})
}

func TestServerPenalty(t *testing.T) {
	assertGt := func(s1, s2 *server, now time.Time) {
		t.Helper()
		p1 := s1.calculatePenalty(now)
		p2 := s2.calculatePenalty(now)
		if p1 <= p2 {
			t.Errorf("Expected penalty for first server to be higher than second server, 0x%x vs 0x%x", p1, p2)
		}
	}

	now := time.Now()
	srv1 := &server{}
	srv2 := &server{}

	// Add one busy connection to srv1
	// Higher penalty to srv1 since it is in use
	c11 := &fakeConn{id: 11}
	srv1.registerBusy(c11)
	assertGt(srv1, srv2, now)

	// Return the busy connection to srv1
	// Now srv2 should have higher penalty than srv1 since using srv2 would require a new
	// connection.
	srv1.returnBusy(c11)
	assertGt(srv2, srv1, now)

	// Add an idle connection to srv2 to make both servers have one idle connection each.
	c21 := &fakeConn{id: 21}
	srv2.registerBusy(c21)
	srv2.returnBusy(c21)

	// At this point round-robin should kick in to even out what server to use, since
	// srv2 was last in use, srv1 should have lower penalty at this point.
	assertGt(srv2, srv1, now)

	// Get the connection from srv1 and return it, now srv1 should have higher penalty.
	srv1.getIdle()
	srv1.returnBusy(c11)
	assertGt(srv1, srv2, now)

	// Add one more connection each to the servers
	c12 := &fakeConn{id: 12}
	srv1.registerBusy(c12)
	srv1.returnBusy(c12)
	c22 := &fakeConn{id: 22}
	srv2.registerBusy(c22)
	srv2.returnBusy(c22)

	// Both servers have two idle connections, srv2 was last used so it should have higher penalty.
	assertGt(srv2, srv1, now)
	// Get both idle connections from srv1
	srv1.getIdle()
	srv1.getIdle()
	// Get one idle connection from srv2
	srv2.getIdle()
	// Since more connections are in use on srv1, it should have higher penalty even though
	// srv2 was last used
	assertGt(srv1, srv2, now)
	// Return the connections
	srv2.getIdle()
	srv2.returnBusy(c21)
	srv2.returnBusy(c22)
	srv1.returnBusy(c11)
	srv1.returnBusy(c12)
	// Everything returned, srv2 should have higher penalty since it was last used
	assertGt(srv2, srv1, now)

	// Punish srv1 by faking that a connect to it failed, after that it should have much higher
	// penalty than srv2
	srv1.notifyFailedConnect(now)
	assertGt(srv1, srv2, now)
	assertTrue(t, srv1.hasFailedConnect(now))
	assertFalse(t, srv2.hasFailedConnect(now))
	// Use srv2 to the max
	srv2.getIdle()
	srv2.getIdle()
	// Even at this point we should prefer srv2
	assertGt(srv1, srv2, now)

	// Emulate that enough time has passed to have forgotten about the failure, now we
	// should prefer srv1
	assertGt(srv2, srv1, now.Add(3*time.Hour))

	// Alternatively a succesful connect should clear the problem
	srv1.notifySuccesfulConnect()
	assertGt(srv2, srv1, now)
}
