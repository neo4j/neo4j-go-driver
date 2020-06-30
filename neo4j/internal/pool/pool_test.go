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
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/log"
)

var logger = &log.ConsoleLogger{Errors: true, Infos: true, Warns: true}

// Scenarios for Borrow and Return
func TestPoolBorrowReturn(ot *testing.T) {
	maxAge := 1 * time.Second
	birthdate := time.Now()

	succeedingConnect := func(s string) (Connection, error) {
		return &fakeConn{serverName: s, isAlive: true, birthdate: birthdate}, nil
	}

	failingError := errors.New("whatever")
	failingConnect := func(s string) (Connection, error) {
		return nil, failingError
	}

	ot.Run("Single thread borrow+return", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		defer p.Close()
		serverNames := []string{"srv1"}
		conn, err := p.Borrow(context.Background(), serverNames, true)
		assertConnection(t, conn, err)
		p.Return(conn)

		// Make sure that connection actually returned
		servers := p.getServers()
		if servers[serverNames[0]].numIdle() != 1 {
			t.Fatal("Should be one ready connection in server")
		}
	})

	ot.Run("First thread borrows, second thread blocks on borrow", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		defer p.Close()
		serverNames := []string{"srv1"}
		wg := sync.WaitGroup{}
		wg.Add(1)

		// First thread borrows
		ctx1 := context.Background()
		c1, err1 := p.Borrow(ctx1, serverNames, true)
		assertConnection(t, c1, err1)

		// Second thread tries to borrow the only allowed connection on the same server
		go func() {
			ctx2 := context.Background()
			// Will block here until first thread detects me in the queue and returns the
			// connection which will unblock here.
			c2, err2 := p.Borrow(ctx2, serverNames, true)
			assertConnection(t, c2, err2)
			wg.Done()
		}()

		// Wait until entered queue
		for {
			if p.queueSize() > 0 {
				break
			}
		}

		// Give back the connection
		p.Return(c1)
		wg.Wait()
	})

	ot.Run("First thread borrows, second thread should not block on borrow without wait", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		defer p.Close()
		serverNames := []string{"srv1"}

		// First thread borrows
		ctx1 := context.Background()
		c1, err1 := p.Borrow(ctx1, serverNames, true)
		assertConnection(t, c1, err1)

		// Actually don't need a thread here since we shouldn't block
		ctx2 := context.Background()
		c2, err2 := p.Borrow(ctx2, serverNames, false)
		assertNoConnection(t, c2, err2)
		// Error should be pool full
		_ = err2.(*PoolFull)
	})

	ot.Run("Multiple threads borrows and returns randomly", func(t *testing.T) {
		maxConns := 2
		p := New(maxConns, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		serverNames := []string{"srv1"}
		numWorkers := 5
		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		worker := func() {
			for i := 0; i < 5; i++ {
				c, err := p.Borrow(context.Background(), serverNames, true)
				assertConnection(t, c, err)
				time.Sleep(time.Duration((rand.Int() % 7)) * time.Millisecond)
				p.Return(c)
			}
			wg.Done()
		}

		for i := 0; i < numWorkers; i++ {
			go worker()
		}
		wg.Wait()

		// Everything should be freed up, it's ok if there isn't a server as well...
		servers := p.getServers()
		for _, v := range servers {
			if v.numIdle() != maxConns {
				t.Error("A connection is still in use in the server")
			}
		}
	})

	ot.Run("Failing connect", func(t *testing.T) {
		p := New(2, maxAge, failingConnect, logger)
		p.now = func() time.Time { return birthdate }
		serverNames := []string{"srv1"}
		c, err := p.Borrow(context.Background(), serverNames, true)
		assertNoConnection(t, c, err)
		// Should get the connect error back
		if err != failingError {
			t.Errorf("Should get connect error back but got: %s", err)
		}
	})

	ot.Run("Cancel Borrow", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		c1, _ := p.Borrow(context.Background(), []string{"A"}, true)
		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		var err error
		wg.Add(1)
		go func() {
			_, err = p.Borrow(ctx, []string{"A"}, true)
			wg.Done()
		}()

		// Wait until entered queue
		for {
			if p.queueSize() > 0 {
				break
			}
		}
		cancel()
		wg.Wait()
		p.Return(c1)
		if err == nil {
			t.Error("There should be an error due to cancelling")
		}
		// Should be a pool error with the cancellation error in it
		_ = err.(*PoolTimeout)
	})
}

// Resource usage scenarios
func TestPoolResourceUsage(ot *testing.T) {
	maxAge := 1 * time.Second
	birthdate := time.Now()

	succeedingConnect := func(s string) (Connection, error) {
		return &fakeConn{serverName: s, isAlive: true, birthdate: birthdate}, nil
	}

	ot.Run("Use order of named servers as priority when creating new servers", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		defer p.Close()
		serverNames := []string{"srvA", "srvB", "srvC", "srvD"}
		c, _ := p.Borrow(context.Background(), serverNames, true)
		if c.ServerName() != serverNames[0] {
			t.Errorf("Should have created server for first server but created for %s", c.ServerName())
		}
	})

	ot.Run("Do not put dead connection back to server", func(t *testing.T) {
		p := New(2, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		defer p.Close()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(context.Background(), serverNames, true)
		c.(*fakeConn).isAlive = false
		p.Return(c)
		servers := p.getServers()
		if len(servers) > 0 && servers[serverNames[0]].size() > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Do not put too old connection back to server", func(t *testing.T) {
		p := New(2, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate.Add(maxAge * 2) }
		defer p.Close()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(context.Background(), serverNames, true)
		p.Return(c)
		servers := p.getServers()
		if len(servers) > 0 && servers[serverNames[0]].size() > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Returning dead connection to server should remove older idle connections", func(t *testing.T) {
		p := New(3, 0, succeedingConnect, logger)
		// Trigger creation of three connections on the same server
		c1, _ := p.Borrow(context.Background(), []string{"A"}, true)
		c2, _ := p.Borrow(context.Background(), []string{"A"}, true)
		c3, _ := p.Borrow(context.Background(), []string{"A"}, true)
		// Manipulate birthdate on the connections
		now := time.Now()
		c1.(*fakeConn).birthdate = now.Add(-1 * time.Second)
		c1.(*fakeConn).id = 1
		c2.(*fakeConn).birthdate = now
		c2.(*fakeConn).id = 2
		c3.(*fakeConn).birthdate = now.Add(1 * time.Second)
		c3.(*fakeConn).id = 3
		// Return the old and young connections to make them idle
		p.Return(c1)
		p.Return(c3)
		assertNumberOfServers(t, p, 1)
		assertNumberOfIdle(t, p, "A", 2)
		// Kill the middle aged connection and return it
		c2.(*fakeConn).isAlive = false
		p.Return(c2)
		assertNumberOfIdle(t, p, "A", 1)
	})

	ot.Run("Do not borrow too old connections", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		nowMut := sync.Mutex{}
		now := birthdate
		p.now = func() time.Time {
			nowMut.Lock()
			defer nowMut.Unlock()
			return now
		}
		defer p.Close()
		serverNames := []string{"srvA"}
		c1, _ := p.Borrow(context.Background(), serverNames, true)
		c1.(*fakeConn).id = 123
		// It's alive when returning it
		p.Return(c1)
		nowMut.Lock()
		now = now.Add(2 * maxAge)
		nowMut.Unlock()
		// Shouldn't get the same one back!
		c2, _ := p.Borrow(context.Background(), serverNames, true)
		if c2.(*fakeConn).id == 123 {
			t.Errorf("Got the old connection back!")
		}
	})

	ot.Run("Add servers when existing servers are full", func(t *testing.T) {
		p := New(1, maxAge, succeedingConnect, logger)
		p.now = func() time.Time { return birthdate }
		defer p.Close()
		c1, err := p.Borrow(context.Background(), []string{"A"}, true)
		assertConnection(t, c1, err)
		c2, err := p.Borrow(context.Background(), []string{"B"}, true)
		assertConnection(t, c2, err)
		assertNumberOfServers(t, p, 2)
	})
}

func TestPoolCleanup(ot *testing.T) {
	birthdate := time.Now()
	maxLife := 1 * time.Second
	succeedingConnect := func(s string) (Connection, error) {
		return &fakeConn{serverName: s, isAlive: true, birthdate: birthdate}, nil
	}

	// Borrows a connection in server A and another in server B
	borrowConnections := func(t *testing.T, p *Pool) (Connection, Connection) {
		c1, err := p.Borrow(context.Background(), []string{"A"}, true)
		assertConnection(t, c1, err)
		c2, err := p.Borrow(context.Background(), []string{"B"}, true)
		assertConnection(t, c2, err)
		return c1, c2
	}

	ot.Run("Should remove servers with only idle too old connections", func(t *testing.T) {
		p := New(0, maxLife, succeedingConnect, logger)
		defer p.Close()
		p.now = func() time.Time { return birthdate }
		c1, c2 := borrowConnections(t, p)
		p.Return(c1)
		p.Return(c2)
		assertNumberOfServers(t, p, 2)
		assertNumberOfIdle(t, p, "A", 1)
		assertNumberOfIdle(t, p, "B", 1)

		// Now go into the future and cleanup, should remove both servers and close the connections
		p.now = func() time.Time { return birthdate.Add(maxLife).Add(1 * time.Second) }
		p.CleanUp()
		assertNumberOfServers(t, p, 0)
	})

	ot.Run("Should not remove servers with busy connections", func(t *testing.T) {
		p := New(0, maxLife, succeedingConnect, logger)
		defer p.Close()
		p.now = func() time.Time { return birthdate }
		_, c2 := borrowConnections(t, p)
		p.Return(c2)
		assertNumberOfServers(t, p, 2)
		assertNumberOfIdle(t, p, "A", 0)
		assertNumberOfIdle(t, p, "B", 1)

		// Now go into the future and cleanup, should only remove B
		p.now = func() time.Time { return birthdate.Add(maxLife).Add(1 * time.Second) }
		p.CleanUp()
		assertNumberOfServers(t, p, 1)
	})

	ot.Run("Should not remove servers with only idle connections but with recent connect failures ", func(t *testing.T) {
		failingConnect := func(s string) (Connection, error) {
			return nil, errors.New("an error")
		}
		p := New(0, maxLife, failingConnect, logger)
		defer p.Close()
		c1, err := p.Borrow(context.Background(), []string{"A"}, true)
		assertNoConnection(t, c1, err)
		assertNumberOfServers(t, p, 1)
		assertNumberOfIdle(t, p, "A", 0)

		// Now go into the future and cleanup, should not remove A even if has no connections since
		// we should remember the failure a bit longer
		p.now = func() time.Time { return birthdate.Add(maxLife).Add(1 * time.Second) }
		p.CleanUp()
		assertNumberOfServers(t, p, 1)

		// Further into the future, the failure should have been forgotten
		p.now = func() time.Time {
			return birthdate.Add(maxLife).Add(rememberFailedConnectDuration).Add(1 * time.Second)
		}
		p.CleanUp()
		assertNumberOfServers(t, p, 0)
	})
}
