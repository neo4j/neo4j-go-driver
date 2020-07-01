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

package router

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/log"
	poolpackage "github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
)

var logger = &log.ConsoleLogger{Errors: true, Infos: true, Warns: true}

// Verifies that concurrent access works as expected relying on the race detector to
// report supicious behavior.
func TestMultithreading(t *testing.T) {

	// Setup a router that needs to read the routing table essentially on every access to
	// stress threading a bit more.
	num := 0
	table := &connection.RoutingTable{Readers: []string{"rd1", "rd2"}, Writers: []string{"wr"}, TimeToLive: 1}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
			num++
			return &connFake{table: table}, nil
		},
	}
	n := time.Now()
	router := New("router", func() []string { return []string{} }, nil, pool, logger)
	mut := sync.Mutex{}
	router.now = func() time.Time {
		// Need to lock here to make race detector happy
		mut.Lock()
		defer mut.Unlock()
		n = n.Add(time.Duration(table.TimeToLive) * time.Second * 2)
		return n
	}

	dbName := "dbname"
	wg := sync.WaitGroup{}
	wg.Add(2)
	consumer := func() {
		for i := 0; i < 30; i++ {
			readers, err := router.Readers(dbName)
			if len(readers) != 2 {
				t.Error("Wrong number of readers")
			}
			if err != nil {
				t.Error(err)
			}
			writers, err := router.Writers(dbName)
			if len(writers) != 1 {
				t.Error("Wrong number of writers")
			}
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}

	go consumer()
	go consumer()

	wg.Wait()
}

func assertNum(t *testing.T, x, y int, msg string) {
	t.Helper()
	if x != y {
		t.Error(msg)
	}
}

func TestRespectsTimeToLiveAndInvalidate(t *testing.T) {
	numfetch := 0
	table := &connection.RoutingTable{TimeToLive: 1}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
			numfetch++
			return &connFake{table: table}, nil
		},
	}
	nzero := time.Now()
	n := nzero
	router := New("router", func() []string { return []string{} }, nil, pool, logger)
	router.now = func() time.Time {
		return n
	}
	dbName := "dbname"

	// First access should trigger initial table read
	router.Readers(dbName)
	assertNum(t, numfetch, 1, "Should have fetched initial")

	// Second access with time set to same should not trigger a read
	router.Readers(dbName)
	assertNum(t, numfetch, 1, "Should not have have fetched")

	// Third access with time passed table due should trigger fetch
	n = n.Add(2 * time.Second)
	router.Readers(dbName)
	assertNum(t, numfetch, 2, "Should have have fetched")

	// Just another one to make sure we're cached
	router.Readers(dbName)
	assertNum(t, numfetch, 2, "Should not have have fetched")

	// Invalidate should force fetching
	router.Invalidate(dbName)
	router.Readers(dbName)
	assertNum(t, numfetch, 3, "Should have have fetched")
}

// Verify that when the routing table can not be retrieved from the root router, a callback
// should be invoked to get backup routers.
func TestUseGetRoutersHookWhenInitialRouterFails(t *testing.T) {
	tried := []string{}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
			tried = append(tried, names...)
			return nil, errors.New("fail")
		},
	}
	rootRouter := "rootRouter"
	backupRouters := []string{"bup1", "bup2"}
	router := New(rootRouter, func() []string { return backupRouters }, nil, pool, logger)
	dbName := "dbname"

	// Trigger read of routing table
	router.Readers(dbName)

	expected := []string{rootRouter}
	expected = append(expected, backupRouters...)

	if !reflect.DeepEqual(tried, expected) {
		t.Errorf("Didn't try the expected routers, tried: %#v", tried)
	}
}

func TestWritersFailAfterNRetries(t *testing.T) {
	numfetch := 0
	tableNoWriters := &connection.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Readers: []string{"rd1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
			// Return no writers first time and writers the second time
			numfetch++
			return &connFake{table: tableNoWriters}, nil
		},
	}
	numsleep := 0
	router := New("router", func() []string { return []string{} }, nil, pool, logger)
	router.sleep = func(time.Duration) {
		numsleep++
	}
	dbName := "dbname"

	// Should trigger a lot of retries to get a writer until it finally fails
	writers, err := router.Writers(dbName)
	if err == nil {
		t.Error("Should have failed")
	}
	if writers != nil {
		t.Error("Should not have any writers")
	}
	if numsleep <= 0 {
		t.Error("Should have slept plenty")
	}
	if numfetch <= 0 {
		t.Error("Should have fetched plenty")
	}
}

func TestWritersRetriesWhenNoWriters(t *testing.T) {
	numfetch := 0
	tableNoWriters := &connection.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Readers: []string{"rd1"}}
	tableWriters := &connection.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Readers: []string{"rd1"}, Writers: []string{"wr1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
			// Return no writers first time and writers the second time
			numfetch++
			if numfetch == 1 {
				return &connFake{table: tableNoWriters}, nil
			}
			return &connFake{table: tableWriters}, nil
		},
	}
	numsleep := 0
	router := New("router", func() []string { return []string{} }, nil, pool, logger)
	router.sleep = func(time.Duration) {
		numsleep++
	}
	dbName := "dbname"

	// Should trigger initial table read that contains no writers and a second table read
	// that gets the writers
	writers, err := router.Writers(dbName)
	if err != nil {
		t.Errorf("Got error: %s", err)
	}
	if len(writers) != 1 {
		t.Error("Didn't get expected writer")
	}
	if numfetch != 2 {
		t.Error("Should have fetched two times")
	}
	if numsleep != 1 {
		t.Error("Should have slept once")
	}
}

func TestCleanUp(t *testing.T) {
	table := &connection.RoutingTable{TimeToLive: 1}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc) (poolpackage.Connection, error) {
			return &connFake{table: table}, nil
		},
	}
	now := time.Now()
	router := New("router", func() []string { return []string{} }, nil, pool, logger)
	router.now = func() time.Time { return now }

	router.Readers("db1")
	router.Readers("db2")

	// Should be a router for each requested database
	if len(router.dbRouters) != 2 {
		t.Fatal("Should be two routing tables, one for each database")
	}

	// Should not remove these since they still have time to live
	router.CleanUp()
	if len(router.dbRouters) != 2 {
		t.Fatal("Should not have removed routing tables")
	}

	router.now = func() time.Time { return now.Add(1 * time.Minute) }
	router.CleanUp()
	if len(router.dbRouters) != 0 {
		t.Fatal("Should have cleaned up")
	}
}
