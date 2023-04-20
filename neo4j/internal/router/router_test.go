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

// Package router handles routing of commands to different database servers part of a cluster.
package router

import (
	"context"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

var logger = &log.Void{}

// Verifies that concurrent access works as expected relying on the race detector to
// report suspicious behavior.
func TestMultithreading(t *testing.T) {

	// Set up a router that needs to read the routing table essentially on every access to
	// stress threading a bit more.
	num := 0
	table := &db.RoutingTable{Readers: []string{"rd1", "rd2"}, Writers: []string{"wr"}, TimeToLive: 1}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			num++
			return &testutil.ConnFake{Table: table}, nil
		},
	}
	n := time.Now()
	mut := sync.Mutex{}
	timer := func() time.Time {
		// Need to lock here to make race detector happy
		mut.Lock()
		defer mut.Unlock()
		n = n.Add(time.Duration(table.TimeToLive) * time.Second * 2)
		return n
	}
	router := New("router", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)

	dbName := "dbname"
	wg := sync.WaitGroup{}
	wg.Add(2)
	consumer := func() {
		for i := 0; i < 30; i++ {
			readers, err := router.Readers(context.Background(), nilBookmarks, dbName, nil, nil)
			if len(readers) != 2 {
				t.Error("Wrong number of readers")
			}
			if err != nil {
				t.Error(err)
			}
			writers, err := router.Writers(context.Background(), nilBookmarks, dbName, nil, nil)
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
	table := &db.RoutingTable{TimeToLive: 1, Readers: []string{"router1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			numfetch++
			return &testutil.ConnFake{Table: table}, nil
		},
	}
	nzero := time.Now()
	n := nzero
	timer := func() time.Time {
		return n
	}
	router := New("router", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)
	dbName := "dbname"

	// First access should trigger initial table read
	ctx := context.Background()
	if _, err := router.Readers(ctx, nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	assertNum(t, numfetch, 1, "Should have fetched initial")

	// Second access with time set to same should not trigger a read
	if _, err := router.Readers(ctx, nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	assertNum(t, numfetch, 1, "Should not have have fetched")

	// Third access with time passed table due should trigger fetch
	n = n.Add(2 * time.Second)
	if _, err := router.Readers(ctx, nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	assertNum(t, numfetch, 2, "Should have have fetched")

	// Just another one to make sure we're cached
	if _, err := router.Readers(ctx, nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	assertNum(t, numfetch, 2, "Should not have have fetched")

	// Invalidate should force fetching
	if err := router.Invalidate(ctx, dbName); err != nil {
		testutil.AssertNoError(t, err)
	}
	if _, err := router.Readers(ctx, nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	assertNum(t, numfetch, 3, "Should have have fetched")
}

func TestUsesRootRouterWhenPreviousRoutersFails(t *testing.T) {
	var borrows [][]string

	conn := &testutil.ConnFake{Table: &db.RoutingTable{
		TimeToLive: 1,
		Routers:    []string{"otherRouter"},
		Readers:    []string{"router1"},
	}}
	var err error
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			borrows = append(borrows, names)
			return conn, err
		},
	}
	nzero := time.Now()
	n := nzero
	timer := func() time.Time {
		return n
	}
	router := New("rootRouter", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)
	dbName := "dbname"

	// First access should trigger initial table read from root router
	if _, err := router.Readers(context.Background(), nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	if borrows[0][0] != "rootRouter" {
		t.Errorf("Should have connected to root upon first router request")
	}
	// Next access should go to otherRouter
	n = n.Add(2 * time.Second)
	if _, err := router.Readers(context.Background(), nilBookmarks, dbName, nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	if borrows[1][0] != "otherRouter" {
		t.Errorf("Should have queried other router")
	}
	// Let the next access first fail when requesting otherRouter and then succeed requesting
	// rootRouter
	requestedOther := false
	requestedRoot := false
	pool.borrow = func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
		if !requestedOther {
			if names[0] != "otherRouter" {
				t.Errorf("Expected request for otherRouter")
				return nil, errors.New("wrong")
			}
			requestedOther = true
			return nil, errors.New("some err")
		}
		if names[0] != "rootRouter" {
			t.Errorf("Expected request for rootRouter")
			return nil, errors.New("oh")
		}
		requestedRoot = true
		return &testutil.ConnFake{Table: &db.RoutingTable{TimeToLive: 1, Readers: []string{"aReader"}}}, nil
	}
	n = n.Add(2 * time.Second)
	readers, err := router.Readers(context.Background(), nilBookmarks, dbName, nil, nil)
	if err != nil {
		t.Error(err)
	}
	if readers[0] != "aReader" {
		t.Errorf("Didn't get the expected reader")
	}
	if !requestedOther || !requestedRoot {
		t.Errorf("Should have requested both other and root routers")
	}
}

// Verify that when the routing table can not be retrieved from the root router, a callback
// should be invoked to get backup routers.
func TestUseGetRoutersHookWhenInitialRouterFails(t *testing.T) {
	var tried []string
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			tried = append(tried, names...)
			return nil, errors.New("fail")
		},
	}
	rootRouter := "rootRouter"
	backupRouters := []string{"bup1", "bup2"}
	timer := time.Now
	router := New(rootRouter, func() []string { return backupRouters }, nil, pool, logger, "routerid", &timer)
	dbName := "dbname"

	// Trigger read of routing table
	_, err := router.Readers(context.Background(), nilBookmarks, dbName, nil, nil)
	testutil.AssertStringContain(t, err.Error(), "Unable to retrieve routing table")

	expected := []string{rootRouter}
	expected = append(expected, backupRouters...)

	if !reflect.DeepEqual(tried, expected) {
		t.Errorf("Didn't try the expected routers, tried: %#v", tried)
	}
}

func TestWritersFailAfterNRetries(t *testing.T) {
	numfetch := 0
	tableNoWriters := &db.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Readers: []string{"rd1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			// Return no writers first time and writers the second time
			numfetch++
			return &testutil.ConnFake{Table: tableNoWriters}, nil
		},
	}
	numsleep := 0
	timer := time.Now
	router := New("router", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)
	router.sleep = func(time.Duration) {
		numsleep++
	}
	dbName := "dbname"

	// Should trigger a lot of retries to get a writer until it finally fails
	writers, err := router.Writers(context.Background(), nilBookmarks, dbName, nil, nil)
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
	tableNoWriters := &db.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Readers: []string{"rd1"}}
	tableWriters := &db.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Readers: []string{"rd1"}, Writers: []string{"wr1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			// Return no writers first time and writers the second time
			numfetch++
			if numfetch == 1 {
				return &testutil.ConnFake{Table: tableNoWriters}, nil
			}
			return &testutil.ConnFake{Table: tableWriters}, nil
		},
	}
	numsleep := 0
	timer := time.Now
	router := New("router", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)
	router.sleep = func(time.Duration) {
		numsleep++
	}
	dbName := "dbname"

	// Should trigger initial table read that contains no writers and a second table read
	// that gets the writers
	writers, err := router.Writers(context.Background(), nilBookmarks, dbName, nil, nil)
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

func TestReadersRetriesWhenNoReaders(t *testing.T) {
	numfetch := 0
	tableNoReaders := &db.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Writers: []string{"wd1"}}
	tableReaders := &db.RoutingTable{TimeToLive: 1, Routers: []string{"rt1", "rt2"}, Writers: []string{"wd1"}, Readers: []string{"wr1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			// Return no readers first time and readers the second time
			numfetch++
			if numfetch == 1 {
				return &testutil.ConnFake{Table: tableNoReaders}, nil
			}
			return &testutil.ConnFake{Table: tableReaders}, nil
		},
	}
	numsleep := 0
	timer := time.Now
	router := New("router", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)
	router.sleep = func(time.Duration) {
		numsleep++
	}
	dbName := "dbname"

	// Should trigger initial table read that contains no readers and a second table read
	// that gets the readers
	readers, err := router.Readers(context.Background(), nilBookmarks, dbName, nil, nil)
	if err != nil {
		t.Errorf("Got error: %s", err)
	}
	if len(readers) != 1 {
		t.Error("Didn't get expected reader")
	}
	if numfetch != 2 {
		t.Error("Should have fetched two times")
	}
	if numsleep != 1 {
		t.Error("Should have slept once")
	}
}

// TODO: Tests here

func TestCleanUp(t *testing.T) {
	table := &db.RoutingTable{TimeToLive: 1, Readers: []string{"router1"}}
	pool := &poolFake{
		borrow: func(names []string, cancel context.CancelFunc, _ log.BoltLogger) (db.Connection, error) {
			return &testutil.ConnFake{Table: table}, nil
		},
	}
	now := time.Now()
	timer := func() time.Time { return now }
	router := New("router", func() []string { return []string{} }, nil, pool, logger, "routerid", &timer)

	ctx := context.Background()
	if _, err := router.Readers(ctx, nilBookmarks, "db1", nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}
	if _, err := router.Readers(ctx, nilBookmarks, "db2", nil, nil); err != nil {
		testutil.AssertNoError(t, err)
	}

	// Should be a router for each requested database
	if len(router.dbRouters) != 2 {
		t.Fatal("Should be two routing tables, one for each database")
	}

	// Should not remove these since they still have time to live
	if err := router.CleanUp(ctx); err != nil {
		testutil.AssertNoError(t, err)
	}
	if len(router.dbRouters) != 2 {
		t.Fatal("Should not have removed routing tables")
	}

	timer = func() time.Time { return now.Add(1 * time.Minute) }
	if err := router.CleanUp(ctx); err != nil {
		testutil.AssertNoError(t, err)
	}
	if len(router.dbRouters) != 0 {
		t.Fatal("Should have cleaned up")
	}
}

func nilBookmarks(context.Context) ([]string, error) { return nil, nil }
