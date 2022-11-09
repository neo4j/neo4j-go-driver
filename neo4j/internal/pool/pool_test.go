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

package pool

import (
	"context"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

var logger = &log.Void{}
var ctx = context.Background()

func TestPoolBorrowReturn(outer *testing.T) {
	maxAge := 1 * time.Second
	birthdate := time.Now()

	succeedingConnect := func(_ context.Context, s string, _ log.BoltLogger, _ []string) (db.Connection, error) {
		return &testutil.ConnFake{Name: s, Alive: true, Birth: birthdate}, nil
	}

	failingError := errors.New("whatever")
	failingConnect := func(_ context.Context, s string, _ log.BoltLogger, _ []string) (db.Connection, error) {
		return nil, failingError
	}

	outer.Run("Single thread borrow+return", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srv1"}
		conn, err := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, conn, err)
		if err := p.Return(ctx, conn); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}

		// Make sure that connection actually returned
		servers, err := p.getServers(ctx)
		if err != nil {
			t.Errorf("Should not fail retrieving servers, got: %v", err)
		}
		if servers[serverNames[0]].numIdle() != 1 {
			t.Fatal("Should be one ready connection in server")
		}
	})

	outer.Run("First thread borrows, second thread blocks on borrow", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srv1"}
		wg := sync.WaitGroup{}
		wg.Add(1)

		// First thread borrows
		c1, err1 := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, c1, err1)

		// Second thread tries to borrow the only allowed connection on the same server
		go func() {
			// Will block here until first thread detects me in the queue and returns the
			// connection which will unblock here.
			c2, err2 := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
			assertConnection(t, c2, err2)
			wg.Done()
		}()

		// Wait until entered queue
		for {
			if size, err := p.queueSize(ctx); err != nil {
				t.Errorf("should not fail computing queue size, got: %v", err)
			} else if size > 0 {
				break
			}
		}

		// Give back the connection
		if err := p.Return(ctx, c1); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		wg.Wait()
	})

	outer.Run("First thread borrows, second thread should not block on borrow without wait", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srv1"}

		// First thread borrows
		c1, err1 := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, c1, err1)

		// Actually don't need a thread here since we shouldn't block
		c2, err2 := p.Borrow(ctx, serverNames, false, nil, DefaultLivenessCheckThreshold)
		assertNoConnection(t, c2, err2)
		// Error should be pool full
		_ = err2.(*PoolFull)
	})

	outer.Run("Multiple threads borrows and returns randomly", func(t *testing.T) {
		maxConnections := 2
		p := New(maxConnections, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		serverNames := []string{"srv1"}
		numWorkers := 5
		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		worker := func() {
			for i := 0; i < 5; i++ {
				c, err := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
				assertConnection(t, c, err)
				time.Sleep(time.Duration(rand.Int()%7) * time.Millisecond)
				if err := p.Return(ctx, c); err != nil {
					t.Errorf("Should not fail returning connection to pool, but got: %v", err)
				}
			}
			wg.Done()
		}

		for i := 0; i < numWorkers; i++ {
			go worker()
		}
		wg.Wait()

		// Everything should be freed up, it's ok if there isn't a server as well...
		servers, err := p.getServers(ctx)
		if err != nil {
			t.Errorf("Should not fail retrieving server, but got: %v", err)
		}
		for _, v := range servers {
			if v.numIdle() != maxConnections {
				t.Error("A connection is still in use in the server")
			}
		}
	})

	outer.Run("Failing connect", func(t *testing.T) {
		p := New(2, maxAge, failingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		serverNames := []string{"srv1"}
		c, err := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		assertNoConnection(t, c, err)
		// Should get the connect error back
		if err != failingError {
			t.Errorf("Should get connect error back but got: %s", err)
		}
	})

	outer.Run("Cancel Borrow", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		c1, _ := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		cancelableCtx, cancel := context.WithCancel(ctx)
		wg := sync.WaitGroup{}
		var err error
		wg.Add(1)
		go func() {
			_, err = p.Borrow(cancelableCtx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
			wg.Done()
		}()

		// Wait until entered queue
		for {
			if size, err := p.queueSize(cancelableCtx); err != nil {
				t.Errorf("should not fail computing queue size, got: %v", err)
			} else if size > 0 {
				break
			}
		}
		cancel()
		wg.Wait()
		if err := p.Return(ctx, c1); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		if err == nil {
			t.Error("There should be an error due to cancelling")
		}
		// Should be a pool error with the cancellation error in it
		_ = err.(*PoolTimeout)
	})

	outer.Run("Borrows the first successfully reset long-idle connection", func(t *testing.T) {
		idlenessThreshold := 1 * time.Hour
		idleness := time.Now().Add(-2 * idlenessThreshold)
		deadAfterReset := deadConnectionAfterForceReset("deadAfterReset", idleness)
		stayingAlive := &testutil.ConnFake{Alive: true, Idle: idleness, Name: "stayingAlive", ForceResetHook: func() {}}
		whatATimeToBeAlive := &testutil.ConnFake{Alive: true, Idle: idleness, Name: "whatATimeToBeAlive", ForceResetHook: func() {
			t.Errorf("y u call me?")
		}}
		pool := New(1, maxAge, nil, logger, "pool id", nil)
		setIdleConnections(pool, map[string][]db.Connection{"a server": {
			deadAfterReset,
			stayingAlive,
			whatATimeToBeAlive,
		}})

		result, err := pool.tryBorrow(ctx, "a server", nil, idlenessThreshold)

		testutil.AssertNil(t, err)
		testutil.AssertDeepEquals(t, result, stayingAlive)
	})

	outer.Run("Borrows new connection if resets of all long-idle connections fail", func(t *testing.T) {
		idlenessThreshold := 1 * time.Hour
		idleness := time.Now().Add(-2 * idlenessThreshold)
		deadAfterReset1 := deadConnectionAfterForceReset("deadAfterReset1", idleness)
		deadAfterReset2 := deadConnectionAfterForceReset("deadAfterReset2", idleness)
		healthyConnection := &testutil.ConnFake{Name: "healthy", ForceResetHook: func() {
			t.Errorf("force reset should not be called on new connections")
		}}
		pool := New(1, maxAge, connectTo(healthyConnection), logger, "pool id", nil)
		setIdleConnections(pool, map[string][]db.Connection{"a server": {deadAfterReset1, deadAfterReset2}})

		result, err := pool.tryBorrow(ctx, "a server", nil, idlenessThreshold)

		testutil.AssertNil(t, err)
		testutil.AssertDeepEquals(t, result, healthyConnection)
	})
}

// Resource usage scenarios
func TestPoolResourceUsage(ot *testing.T) {
	maxAge := 1 * time.Second
	birthdate := time.Now()

	succeedingConnect := func(_ context.Context, s string, _ log.BoltLogger, _ []string) (db.Connection, error) {
		return &testutil.ConnFake{Name: s, Alive: true, Birth: birthdate}, nil
	}

	ot.Run("Use order of named servers as priority when creating new servers", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srvA", "srvB", "srvC", "srvD"}
		c, _ := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		if c.ServerName() != serverNames[0] {
			t.Errorf("Should have created server for first server but created for %s", c.ServerName())
		}
	})

	ot.Run("Do not put dead connection back to server", func(t *testing.T) {
		p := New(2, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		c.(*testutil.ConnFake).Alive = false
		if err := p.Return(ctx, c); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		servers, err := p.getServers(ctx)
		if err != nil {
			t.Errorf("Should not fail retrieving server but got: %v", err)
		}
		if len(servers) > 0 && servers[serverNames[0]].size() > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Do not put too old connection back to server", func(t *testing.T) {
		p := New(2, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate.Add(maxAge * 2) }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		if err := p.Return(ctx, c); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		servers, err := p.getServers(ctx)
		if err != nil {
			t.Errorf("Should not fail retrieving server but got: %v", err)
		}
		if len(servers) > 0 && servers[serverNames[0]].size() > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Returning dead connection to server should remove older idle connections", func(t *testing.T) {
		p := New(3, 0, succeedingConnect, logger, "pool id", nil)
		// Trigger creation of three connections on the same server
		c1, _ := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		c2, _ := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		c3, _ := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		// Manipulate birthdate on the connections
		now := time.Now()
		c1.(*testutil.ConnFake).Birth = now.Add(-1 * time.Second)
		c1.(*testutil.ConnFake).Id = 1
		c2.(*testutil.ConnFake).Birth = now
		c2.(*testutil.ConnFake).Id = 2
		c3.(*testutil.ConnFake).Birth = now.Add(1 * time.Second)
		c3.(*testutil.ConnFake).Id = 3
		// Return the old and young connections to make them idle
		if err := p.Return(ctx, c1); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		if err := p.Return(ctx, c3); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 1)
		assertNumberOfIdle(t, ctx, p, "A", 2)
		// Kill the middle-aged connection and return it
		c2.(*testutil.ConnFake).Alive = false
		if err := p.Return(ctx, c2); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		assertNumberOfIdle(t, ctx, p, "A", 1)
	})

	ot.Run("Do not borrow too old connections", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		nowMut := sync.Mutex{}
		now := birthdate
		p.now = func() time.Time {
			nowMut.Lock()
			defer nowMut.Unlock()
			return now
		}
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		serverNames := []string{"srvA"}
		c1, _ := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		c1.(*testutil.ConnFake).Id = 123
		// It's alive when returning it
		if err := p.Return(ctx, c1); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		nowMut.Lock()
		now = now.Add(2 * maxAge)
		nowMut.Unlock()
		// Shouldn't get the same one back!
		c2, _ := p.Borrow(ctx, serverNames, true, nil, DefaultLivenessCheckThreshold)
		if c2.(*testutil.ConnFake).Id == 123 {
			t.Errorf("Got the old connection back!")
		}
	})

	ot.Run("Add servers when existing servers are full", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger, "pool id", nil)
		p.now = func() time.Time { return birthdate }
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		c1, err := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, c1, err)
		c2, err := p.Borrow(ctx, []string{"B"}, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, c2, err)
		assertNumberOfServers(t, ctx, p, 2)
	})
}

func TestPoolCleanup(ot *testing.T) {
	birthdate := time.Now()
	maxLife := 1 * time.Second
	succeedingConnect := func(_ context.Context, s string, _ log.BoltLogger, _ []string) (db.Connection, error) {
		return &testutil.ConnFake{Name: s, Alive: true, Birth: birthdate}, nil
	}

	// Borrows a connection in server A and another in server B
	borrowConnections := func(t *testing.T, p *Pool) (db.Connection, db.Connection) {
		c1, err := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, c1, err)
		c2, err := p.Borrow(ctx, []string{"B"}, true, nil, DefaultLivenessCheckThreshold)
		assertConnection(t, c2, err)
		return c1, c2
	}

	ot.Run("Should remove servers with only idle too old connections", func(t *testing.T) {
		p := New(0, maxLife, succeedingConnect, logger, "pool id", nil)
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		p.now = func() time.Time { return birthdate }
		c1, c2 := borrowConnections(t, p)
		if err := p.Return(ctx, c1); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		if err := p.Return(ctx, c2); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 2)
		assertNumberOfIdle(t, ctx, p, "A", 1)
		assertNumberOfIdle(t, ctx, p, "B", 1)

		// Now go into the future and cleanup, should remove both servers and close the connections
		p.now = func() time.Time { return birthdate.Add(maxLife).Add(1 * time.Second) }
		if err := p.CleanUp(ctx); err != nil {
			t.Errorf("Should not fail cleaning up the pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 0)
	})

	ot.Run("Should not remove servers with busy connections", func(t *testing.T) {
		p := New(0, maxLife, succeedingConnect, logger, "pool id", nil)
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		p.now = func() time.Time { return birthdate }
		_, c2 := borrowConnections(t, p)
		if err := p.Return(ctx, c2); err != nil {
			t.Errorf("Should not fail returning connection to pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 2)
		assertNumberOfIdle(t, ctx, p, "A", 0)
		assertNumberOfIdle(t, ctx, p, "B", 1)

		// Now go into the future and cleanup, should only remove B
		p.now = func() time.Time { return birthdate.Add(maxLife).Add(1 * time.Second) }
		if err := p.CleanUp(ctx); err != nil {
			t.Errorf("Should not fail cleaning up the pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 1)
	})

	ot.Run("Should not remove servers with only idle connections but with recent connect failures ", func(t *testing.T) {
		failingConnect := func(_ context.Context, s string, _ log.BoltLogger, _ []string) (db.Connection, error) {
			return nil, errors.New("an error")
		}
		p := New(0, maxLife, failingConnect, logger, "pool id", nil)
		defer func() {
			if err := p.Close(ctx); err != nil {
				t.Errorf("Should not fail closing the pool, but got: %v", err)
			}
		}()
		c1, err := p.Borrow(ctx, []string{"A"}, true, nil, DefaultLivenessCheckThreshold)
		assertNoConnection(t, c1, err)
		assertNumberOfServers(t, ctx, p, 1)
		assertNumberOfIdle(t, ctx, p, "A", 0)

		// Now go into the future and cleanup, should not remove server A even if it has no connections since
		// we should remember the failure a bit longer
		p.now = func() time.Time { return birthdate.Add(maxLife).Add(1 * time.Second) }
		if err := p.CleanUp(ctx); err != nil {
			t.Errorf("Should not fail cleaning up the pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 1)

		// Further in the future, the failure should have been forgotten
		p.now = func() time.Time {
			return birthdate.Add(maxLife).Add(rememberFailedConnectDuration).Add(1 * time.Second)
		}
		if err := p.CleanUp(ctx); err != nil {
			t.Errorf("Should not fail cleaning up the pool, but got: %v", err)
		}
		assertNumberOfServers(t, ctx, p, 0)
	})
}

func connectTo(singleConnection *testutil.ConnFake) func(ctx context.Context, name string, _ log.BoltLogger, _ []string) (db.Connection, error) {
	return func(ctx context.Context, name string, _ log.BoltLogger, _ []string) (db.Connection, error) {
		return singleConnection, nil
	}
}

func setIdleConnections(pool *Pool, servers map[string][]db.Connection) {
	poolServers := make(map[string]*server, len(servers))
	for serverName, connections := range servers {
		srv := NewServer()
		// iterate in reverse order since registerIdle uses PushFront
		// we want connections to be tried in the slice order
		for i := len(connections) - 1; i >= 0; i-- {
			registerIdle(srv, connections[i])
		}
		poolServers[serverName] = srv
	}
	pool.servers = poolServers
}

func deadConnectionAfterForceReset(name string, idleness time.Time) *testutil.ConnFake {
	result := &testutil.ConnFake{Alive: true, Idle: idleness, Name: name}
	result.ForceResetHook = func() {
		result.Alive = false
	}
	return result
}
