//go:build internal_time_mock

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
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/homedb"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

var logger = log.ToVoid()
var ctx = context.Background()
var reAuthToken = &idb.ReAuthToken{FromSession: false, Manager: iauth.Token{Tokens: map[string]any{"scheme": "none"}}}

func TestPoolBorrowReturn(outer *testing.T) {
	maxAge := 1 * time.Second
	birthdate := time.Now()

	succeedingConnect := func(_ context.Context, s string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
		return &ConnFake{Name: s, Alive: true, Birth: birthdate}, nil
	}

	failingError := errors.New("whatever")
	failingConnect := func(_ context.Context, s string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
		return nil, failingError
	}

	outer.Run("Single thread borrow+return", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srv1"}
		conn, err := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, conn, err)
		p.Return(ctx, conn)

		// Make sure that connection actually returned
		servers := p.getServers()
		if err != nil {
			t.Errorf("Should not fail retrieving servers, got: %v", err)
		}
		if servers[serverNames[0]].numIdle() != 1 {
			t.Fatal("Should be one ready connection in server")
		}
	})

	outer.Run("First thread borrows, second thread blocks on borrow", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srv1"}
		wg := sync.WaitGroup{}
		wg.Add(1)

		// First thread borrows
		c1, err1 := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c1, err1)

		// Second thread tries to borrow the only allowed connection on the same server
		go func() {
			// Will block here until first thread detects me in the queue and returns the
			// connection which will unblock here.
			c2, err2 := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
			assertConnection(t, c2, err2)
			wg.Done()
		}()

		waitForBorrowers(p, 1)

		// Give back the connection
		p.Return(ctx, c1)
		wg.Wait()
	})

	outer.Run("First thread borrows, second thread should not block on borrow without wait", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srv1"}

		// First thread borrows
		c1, err1 := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c1, err1)

		// Actually don't need a thread here since we shouldn't block
		c2, err2 := p.Borrow(ctx, getServers(serverNames), false, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertNoConnection(t, c2, err2)
		// Error should be pool full
		_ = err2.(*errorutil.PoolFull)
	})

	outer.Run("Multiple threads borrows and returns randomly", func(t *testing.T) {
		maxConnections := 2
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: maxConnections}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		serverNames := []string{"srv1"}
		numWorkers := 5
		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		worker := func() {
			for i := 0; i < 5; i++ {
				c, err := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
				assertConnection(t, c, err)
				time.Sleep(time.Duration(rand.Int()%7) * time.Millisecond)
				p.Return(ctx, c)
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
			if v.numIdle() != maxConnections {
				t.Error("A connection is still in use in the server")
			}
		}
	})

	outer.Run("Failing connect", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 2}
		cache := &homedb.Cache{}
		p := New(&conf, failingConnect, logger, "pool id", cache)
		p.SetRouter(&RouterFake{})
		serverNames := []string{"srv1"}
		c, err := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertNoConnection(t, c, err)
		// Should get the connect error back
		if err != failingError {
			t.Errorf("Should get connect error back but got: %s", err)
		}
	})

	outer.Run("Cancel Borrow", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		c1, _ := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		cancelableCtx, cancel := context.WithCancel(ctx)
		wg := sync.WaitGroup{}
		var err error
		wg.Add(1)
		go func() {
			_, err = p.Borrow(cancelableCtx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
			wg.Done()
		}()

		waitForBorrowers(p, 1)
		cancel()
		wg.Wait()
		p.Return(ctx, c1)
		if err == nil {
			t.Error("There should be an error due to cancelling")
		}
		// Should be a pool error with the cancellation error in it
		_ = err.(*errorutil.PoolTimeout)
	})

	outer.Run("Borrows the first successfully reset long-idle connection", func(t *testing.T) {
		idlenessThreshold := 1 * time.Hour
		idleness := time.Now().Add(-2 * idlenessThreshold)
		deadAfterReset := deadConnectionAfterForceReset("deadAfterReset", idleness)
		stayingAlive := &ConnFake{Alive: true, Idle: idleness, Name: "stayingAlive", ForceResetHook: func() {}}
		whatATimeToBeAlive := &ConnFake{Alive: true, Idle: idleness, Name: "whatATimeToBeAlive", ForceResetHook: func() {
			t.Errorf("y u call me?")
		}}
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		pool := New(&conf, nil, logger, "pool id", cache)
		setIdleConnections(pool, map[string][]idb.Connection{"a server": {
			deadAfterReset,
			stayingAlive,
			whatATimeToBeAlive,
		}})

		result, err := pool.tryBorrow(ctx, "a server", nil, idlenessThreshold, reAuthToken)

		AssertNil(t, err)
		AssertDeepEquals(t, result, stayingAlive)
	})

	outer.Run("Borrows new connection if resets of all long-idle connections fail", func(t *testing.T) {
		serverName := "a server"
		idlenessThreshold := 1 * time.Hour
		idleness := time.Now().Add(-2 * idlenessThreshold)
		deadAfterReset1 := deadConnectionAfterForceReset("deadAfterReset1", idleness)
		deadAfterReset2 := deadConnectionAfterForceReset("deadAfterReset2", idleness)
		healthyConnection := &ConnFake{Name: "healthy", ForceResetHook: func() {
			t.Errorf("force reset should not be called on new connections")
		}}
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		pool := New(&conf, connectTo(healthyConnection), logger, "pool id", cache)
		setIdleConnections(pool, map[string][]idb.Connection{serverName: {deadAfterReset1, deadAfterReset2}})

		result, err := pool.tryBorrow(ctx, serverName, nil, idlenessThreshold, reAuthToken)

		AssertNil(t, err)
		AssertDeepEquals(t, result, healthyConnection)
		AssertIntEqual(t, pool.servers[serverName].numIdle(), 0)
		AssertIntEqual(t, pool.servers[serverName].numBusy(), 1)
	})

	outer.Run("Waiting borrow does not receive returned broken connection", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		c1, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c1, err)
		ctx = context.Background()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			c2, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
			assertConnection(t, c2, err)
			AssertNotDeepEquals(t, c1, c2)
			wg.Done()
		}()

		waitForBorrowers(p, 1)
		// break the connection. then it shouldn't be picked up by the waiting borrow
		c1.(*ConnFake).Alive = false
		p.Return(ctx, c1)
		AssertNoError(t, err)
		wg.Wait()
	})

	outer.Run("Waiting borrow does re-auth", func(t *testing.T) {
		token2 := iauth.Token{Tokens: map[string]any{"scheme": "foobar"}}
		// sanity check
		AssertNotDeepEquals(t, reAuthToken.Manager, token2)
		reAuthToken2 := &idb.ReAuthToken{FromSession: false, Manager: token2}
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		c1, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c1, err)
		ctx = context.Background()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			c2, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken2)
			assertConnection(t, c2, err)
			AssertDeepEquals(t, c1, c2)
			wg.Done()
		}()

		waitForBorrowers(p, 1)
		reAuthCalled := false
		c1.(*ConnFake).ReAuthHook = func(_ context.Context, token *idb.ReAuthToken) error {
			AssertDeepEquals(t, token.Manager, token2)
			reAuthCalled = true
			return nil
		}
		p.Return(ctx, c1)
		AssertNoError(t, err)
		wg.Wait()
		AssertTrue(t, reAuthCalled)
	})
}

// Resource usage scenarios
func TestPoolResourceUsage(ot *testing.T) {
	maxAge := 1 * time.Second
	birthdate := time.Now()

	succeedingConnect := func(_ context.Context, s string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
		return &ConnFake{Name: s, Alive: true, Birth: birthdate}, nil
	}

	ot.Run("Use order of named servers as priority when creating new servers", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srvA", "srvB", "srvC", "srvD"}
		c, _ := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		if c.ServerName() != serverNames[0] {
			t.Errorf("Should have created server for first server but created for %s", c.ServerName())
		}
	})

	ot.Run("Do not put dead connection back to server", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 2}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		c.(*ConnFake).Alive = false
		p.Return(ctx, c)
		servers := p.getServers()
		if len(servers) > 0 && servers[serverNames[0]].size() > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Do not put too old connection back to server", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 2}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srvA"}
		c, _ := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		itime.ForceTickTime(2 * maxAge)
		p.Return(ctx, c)
		servers := p.getServers()
		if len(servers) > 0 && servers[serverNames[0]].size() > 0 {
			t.Errorf("Should have either removed the server or kept it but emptied it")
		}
	})

	ot.Run("Returning dead connection to server should remove older idle connections", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: 1<<63 - 1, MaxConnectionPoolSize: 3}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		// Trigger creation of three connections on the same server
		c1, _ := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		c2, _ := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		c3, _ := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		// Manipulate birthdate on the connections
		nowTime := itime.Now()
		c1.(*ConnFake).Birth = nowTime.Add(-1 * time.Second)
		c1.(*ConnFake).Id = 1
		c2.(*ConnFake).Birth = nowTime
		c2.(*ConnFake).Id = 2
		c3.(*ConnFake).Birth = nowTime.Add(1 * time.Second)
		c3.(*ConnFake).Id = 3
		// Return the old and young connections to make them idle
		p.Return(ctx, c1)
		p.Return(ctx, c3)
		assertNumberOfServers(t, p, 1)
		assertNumberOfIdle(t, p, "A", 2)
		// Kill the middle-aged connection and return it
		c2.(*ConnFake).Alive = false
		p.Return(ctx, c2)
		assertNumberOfIdle(t, p, "A", 1)
	})

	ot.Run("Do not borrow too old connections", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		serverNames := []string{"srvA"}
		c1, _ := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		c1.(*ConnFake).Id = 123
		// It's alive when returning it
		p.Return(ctx, c1)
		itime.ForceTickTime(2 * maxAge)
		// Shouldn't get the same one back!
		c2, _ := p.Borrow(ctx, getServers(serverNames), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		if c2.(*ConnFake).Id == 123 {
			t.Errorf("Got the old connection back!")
		}
	})

	ot.Run("Add servers when existing servers are full", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxAge, MaxConnectionPoolSize: 1}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		c1, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c1, err)
		c2, err := p.Borrow(ctx, getServers([]string{"B"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c2, err)
		assertNumberOfServers(t, p, 2)
	})
}

func TestPoolCleanup(ot *testing.T) {
	birthdate := time.Now()
	maxLife := 1 * time.Second
	succeedingConnect := func(_ context.Context, s string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
		return &ConnFake{Name: s, Alive: true, Birth: birthdate}, nil
	}

	// Borrows a connection in server A and another in server B
	borrowConnections := func(t *testing.T, p *Pool) (idb.Connection, idb.Connection) {
		c1, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c1, err)
		c2, err := p.Borrow(ctx, getServers([]string{"B"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, c2, err)
		return c1, c2
	}

	ot.Run("Should remove servers with only idle too old connections", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxLife, MaxConnectionPoolSize: 0}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		c1, c2 := borrowConnections(t, p)
		p.Return(ctx, c1)
		p.Return(ctx, c2)
		assertNumberOfServers(t, p, 2)
		assertNumberOfIdle(t, p, "A", 1)
		assertNumberOfIdle(t, p, "B", 1)

		// Now go into the future and cleanup, should remove both servers and close the connections
		itime.ForceTickTime(maxLife + 1*time.Second)
		p.CleanUp(ctx)
		assertNumberOfServers(t, p, 0)
	})

	ot.Run("Should not remove servers with busy connections", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{MaxConnectionLifetime: maxLife, MaxConnectionPoolSize: 0}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		defer func() {
			p.Close(ctx)
		}()
		_, c2 := borrowConnections(t, p)
		p.Return(ctx, c2)
		assertNumberOfServers(t, p, 2)
		assertNumberOfIdle(t, p, "A", 0)
		assertNumberOfIdle(t, p, "B", 1)

		// Now go into the future and cleanup, should only remove B
		itime.ForceTickTime(maxLife + 1*time.Second)
		p.CleanUp(ctx)
		assertNumberOfServers(t, p, 1)
	})

	ot.Run("Should not remove servers with only idle connections but with recent connect failures ", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		failingConnect := func(_ context.Context, s string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
			return nil, errors.New("an error")
		}
		conf := config.Config{MaxConnectionLifetime: maxLife, MaxConnectionPoolSize: 0}
		cache := &homedb.Cache{}
		p := New(&conf, failingConnect, logger, "pool id", cache)
		p.SetRouter(&RouterFake{})
		defer func() {
			p.Close(ctx)
		}()
		c1, err := p.Borrow(ctx, getServers([]string{"A"}), true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertNoConnection(t, c1, err)
		assertNumberOfServers(t, p, 1)
		assertNumberOfIdle(t, p, "A", 0)

		// Now go into the future and cleanup, should not remove server A even if it has no connections since
		// we should remember the failure a bit longer
		itime.ForceTickTime(maxLife + 1*time.Second)
		p.CleanUp(ctx)
		assertNumberOfServers(t, p, 1)

		// Further in the future, the failure should have been forgotten
		itime.ForceTickTime(rememberFailedConnectDuration)
		p.CleanUp(ctx)
		assertNumberOfServers(t, p, 0)
	})

	ot.Run("wakes up borrowers when closing", func(t *testing.T) {
		itime.ForceFreezeTime()
		defer itime.ForceUnfreezeTime()
		conf := config.Config{
			ConnectionAcquisitionTimeout: 10 * time.Second,
			MaxConnectionLifetime:        maxLife,
			MaxConnectionPoolSize:        1,
		}
		cache := &homedb.Cache{}
		p := New(&conf, succeedingConnect, logger, "pool id", cache)
		servers := getServers([]string{"example.com"})
		conn, err := p.Borrow(ctx, servers, false, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
		assertConnection(t, conn, err)
		borrowErrChan := make(chan error)
		go func() {
			_, err := p.Borrow(ctx, servers, true, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
			borrowErrChan <- err
		}()
		waitForBorrowers(p, 1)

		p.Close(ctx)

		select {
		case err := <-borrowErrChan:
			AssertErrorMessageContains(t, err, "Pool closed")
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for borrow error")
		}
	})
}

func TestPoolErrorHanding(ot *testing.T) {
	const ServerName = "A"
	const DbName = "some database"

	type TestCase struct {
		name                      string
		errorCall                 func(bolt.ConnectionErrorListener, idb.Connection) error
		expectedInvalidateMode    string
		expectedInvalidatedServer string
		expectedInvalidatedDb     string
	}

	dbUnavailableErr := db.Neo4jError{Code: "Neo.TransientError.General.DatabaseUnavailable"}
	notALeaderErr := db.Neo4jError{Code: "Neo.ClientError.Cluster.NotALeader"}
	forbiddenRoDbError := db.Neo4jError{Code: "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase"}

	cases := []TestCase{
		{
			name: "should invalidate server on io error",
			errorCall: func(listener bolt.ConnectionErrorListener, conn idb.Connection) error {
				listener.OnIoError(ctx, conn, errors.New("an error"))
				return nil
			},
			expectedInvalidatedServer: ServerName,
		},
		{
			name: "should invalidate server on dial error",
			errorCall: func(listener bolt.ConnectionErrorListener, _ idb.Connection) error {
				listener.OnDialError(ctx, "what ever server", errors.New("an error"))
				return nil
			},
			expectedInvalidatedServer: "what ever server",
		},
		{
			name: "should invalidate server on dial error",
			errorCall: func(listener bolt.ConnectionErrorListener, _ idb.Connection) error {
				listener.OnDialError(ctx, ServerName, errors.New("an error"))
				return nil
			},
			expectedInvalidatedServer: ServerName,
		},
		{
			name: "should invalidate server on db unavailable error",
			errorCall: func(listener bolt.ConnectionErrorListener, conn idb.Connection) error {
				return listener.OnNeo4jError(ctx, conn, &dbUnavailableErr)
			},
			expectedInvalidatedServer: ServerName,
		},
		{
			name: "should invalidate writer for db on not a leader error",
			errorCall: func(listener bolt.ConnectionErrorListener, conn idb.Connection) error {
				return listener.OnNeo4jError(ctx, conn, &notALeaderErr)
			},
			expectedInvalidateMode:    "writer",
			expectedInvalidatedServer: ServerName,
			expectedInvalidatedDb:     DbName,
		},
		{
			name: "should invalidate writer for db on forbidden error",
			errorCall: func(listener bolt.ConnectionErrorListener, conn idb.Connection) error {
				return listener.OnNeo4jError(ctx, conn, &forbiddenRoDbError)
			},
			expectedInvalidateMode:    "writer",
			expectedInvalidatedServer: ServerName,
			expectedInvalidatedDb:     DbName,
		},
	}

	for _, testCase := range cases {
		ot.Run(testCase.name, func(t *testing.T) {
			errorListeners := make([]bolt.ConnectionErrorListener, 0)
			connections := make([]*ConnFake, 0)
			succeedingConnect := func(_ context.Context, s string, _ *idb.ReAuthToken, errorListener bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
				errorListeners = append(errorListeners, errorListener)
				connection := ConnFake{Name: s, Alive: true, DatabaseName: DbName}
				connections = append(connections, &connection)
				return &connection, nil
			}

			router := RouterFake{}
			cache := &homedb.Cache{}
			p := New(&config.Config{}, succeedingConnect, logger, "pool id", cache)
			p.SetRouter(&router)
			defer p.Close(ctx)
			conn, err := p.Borrow(ctx, getServers([]string{ServerName}), false, nil, DefaultConnectionLivenessCheckTimeout, reAuthToken)
			assertConnection(t, conn, err)
			AssertLen(t, errorListeners, 1)
			AssertLen(t, connections, 1)
			errorListener := errorListeners[0]
			connection := connections[0]
			AssertFalse(t, router.Invalidated)

			err = testCase.errorCall(errorListener, connection)
			AssertNoError(t, err)
			AssertTrue(t, router.Invalidated)
			AssertStringEqual(t, router.InvalidateMode, testCase.expectedInvalidateMode)
			AssertStringEqual(t, router.InvalidatedServer, testCase.expectedInvalidatedServer)
			AssertStringEqual(t, router.InvalidatedDb, testCase.expectedInvalidatedDb)
		})
	}
}

func connectTo(singleConnection *ConnFake) func(ctx context.Context, name string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
	return func(ctx context.Context, name string, _ *idb.ReAuthToken, _ bolt.ConnectionErrorListener, _ log.BoltLogger) (idb.Connection, error) {
		return singleConnection, nil
	}
}

func setIdleConnections(pool *Pool, servers map[string][]idb.Connection) {
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

func deadConnectionAfterForceReset(name string, idleness time.Time) *ConnFake {
	result := &ConnFake{Alive: true, Idle: idleness, Name: name}
	result.ForceResetHook = func() {
		result.Alive = false
	}
	return result
}

func getServers(servers []string) func() []string {
	return func() []string {
		return servers
	}
}

func waitForBorrowers(p *Pool, minBorrowers int) {
	for p.queueSize() < minBorrowers {
		// still waiting
	}
}
