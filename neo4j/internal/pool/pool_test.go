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

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

// Scenarios for Borrow and Return
func TestPoolBorrowReturn(ot *testing.T) {
	succeedingConnect := func(s string) (conn.Connection, error) {
		return &fakeConn{serverName: s, isAlive: true}, nil
	}

	failingConnect := func(s string) (conn.Connection, error) {
		return nil, errors.New("whatever")
	}

	ot.Run("Single thread borrow+return", func(t *testing.T) {
		p := New(1, succeedingConnect)
		defer p.Close()
		serverNames := []string{"srv1"}
		conn, err := p.Borrow(context.Background(), serverNames)
		if err != nil || conn == nil {
			t.Fatalf("Should have received conn and no error: %s", err)
		}
		p.Return(conn)

		// Make sure that connection actually returned
		servers := p.getServers()
		if servers[serverNames[0]].num() != 1 {
			t.Fatal("Should be one ready connection in server")
		}
	})

	ot.Run("First thread borrows, second thread blocks on borrow", func(t *testing.T) {
		p := New(1, succeedingConnect)
		defer p.Close()
		serverNames := []string{"srv1"}
		wg := sync.WaitGroup{}
		wg.Add(1)

		// First thread borrows
		ctx1 := context.Background()
		c1, err1 := p.Borrow(ctx1, serverNames)
		if c1 == nil || err1 != nil {
			t.Fatal()
		}

		// Second thread tries to borrow the only allowed connection on the same server
		go func() {
			ctx2 := context.Background()
			// Will block here until first thread detects me in the queue and returns the
			// connection which will unblock here.
			c2, err2 := p.Borrow(ctx2, serverNames)
			if c2 == nil || err2 != nil {
				t.Fatal()
			}
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

	ot.Run("Multiple threads borrows and returns randomly", func(t *testing.T) {
		maxConns := 2
		p := New(maxConns, succeedingConnect)
		serverNames := []string{"srv1"}
		numWorkers := 5
		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		worker := func() {
			for i := 0; i < 5; i++ {
				c, err := p.Borrow(context.Background(), serverNames)
				if c == nil || err != nil {
					t.Fatal()
				}
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
			if v.num() != maxConns {
				t.Error("A connection is still in use in the server")
			}
		}
	})

	ot.Run("Failing connect", func(t *testing.T) {
		p := New(2, failingConnect)
		serverNames := []string{"srv1"}
		c, err := p.Borrow(context.Background(), serverNames)
		if c != nil || err == nil {
			t.Error("Should have failed")
		}
	})

	ot.Run("Cancel Borrow", func(t *testing.T) {
		p := New(1, succeedingConnect)
		c1, _ := p.Borrow(context.Background(), []string{"A"})
		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		var err error
		wg.Add(1)
		go func() {
			_, err = p.Borrow(ctx, []string{"A"})
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
			t.Error("There should be an error due to canelling")
		}
	})
}

// Resource usage scenarios
func TestPoolResourceUsage(ot *testing.T) {
	succeedingConnect := func(s string) (conn.Connection, error) {
		return &fakeConn{serverName: s, isAlive: true}, nil
	}

	ot.Run("Use order of named servers as priority when creating new servers", func(t *testing.T) {
		p := New(1, succeedingConnect)
		defer p.Close()
		serverNames := []string{"srvA", "srvB", "srvC", "srvD"}
		c, _ := p.Borrow(context.Background(), serverNames)
		if c.ServerName() != serverNames[0] {
			t.Errorf("Should have created server for first server but created for %s", c.ServerName())
		}
	})

	ot.Run("Do not put dead connection back to server", func(t *testing.T) {
		p := New(2, succeedingConnect)
		defer p.Close()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(context.Background(), serverNames)
		c.(*fakeConn).isAlive = false
		p.Return(c)
		servers := p.getServers()
		if len(servers) > 0 && servers[serverNames[0]].size > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Returning last dead connection to server should remove server", func(t *testing.T) {
		p := New(2, succeedingConnect)
		defer p.Close()
		serverNames := []string{"srvA"}
		c1, _ := p.Borrow(context.Background(), serverNames)
		c2, _ := p.Borrow(context.Background(), serverNames)
		c1.(*fakeConn).isAlive = false
		p.Return(c1)
		servers := p.getServers()
		if len(servers) != 1 {
			t.Errorf("Should still be a server")
		}
		c2.(*fakeConn).isAlive = false
		p.Return(c2)
		servers = p.getServers()
		if len(servers) != 0 {
			t.Errorf("Should be no servers")
		}
	})

	ot.Run("Do not borrow dead connections", func(t *testing.T) {
		p := New(1, succeedingConnect)
		defer p.Close()
		serverNames := []string{"srvA"}
		c1, _ := p.Borrow(context.Background(), serverNames)
		// It's alive when returning it
		p.Return(c1)
		// Now kill it
		c1.(*fakeConn).isAlive = false
		// Shouldn't get the same one back!
		c2, _ := p.Borrow(context.Background(), serverNames)
		if !c2.IsAlive() {
			t.Errorf("Got dead connection back!")
		}
	})

	ot.Run("Add servers when existing servers are full", func(t *testing.T) {
		p := New(1, succeedingConnect)
		defer p.Close()
		c1, _ := p.Borrow(context.Background(), []string{"A"})
		c2, _ := p.Borrow(context.Background(), []string{"B"})
		if c1 == nil || c2 == nil {
			t.Error("Didn't get connections")
		}
		servers := p.getServers()
		if len(servers) != 2 {
			t.Error("Should be two servers, one overflowed?")
		}
	})
}
