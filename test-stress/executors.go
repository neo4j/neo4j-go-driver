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

package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// TestContext provides state data shared across tests
type TestContext struct {
	driver   neo4j.Driver
	stop     int32
	bookmark atomic.Value

	readNodeCountsByServer sync.Map

	readNodeCount       int32
	createdNodeCount    int32
	failedBookmarkCount int32
	leaderSwitchCount   int32
}

// NewTestContext returns a new TestContext
func NewTestContext(driver neo4j.Driver) *TestContext {
	result := &TestContext{
		driver:                 driver,
		stop:                   0,
		bookmark:               atomic.Value{},
		readNodeCountsByServer: sync.Map{},
		readNodeCount:          0,
		createdNodeCount:       0,
		failedBookmarkCount:    0,
		leaderSwitchCount:      0,
	}

	result.bookmark.Store("")

	return result
}

// ShouldStop returns whether a stop is signalled
func (ctx *TestContext) ShouldStop() bool {
	return atomic.LoadInt32(&ctx.stop) > 0
}

// Stop signals a stop
func (ctx *TestContext) Stop() {
	atomic.CompareAndSwapInt32(&ctx.stop, 0, 1)
}

func (ctx *TestContext) addCreated() {
	atomic.AddInt32(&ctx.createdNodeCount, 1)
}

func (ctx *TestContext) addRead() {
	atomic.AddInt32(&ctx.readNodeCount, 1)
}

func (ctx *TestContext) addBookmarkFailure() {
	atomic.AddInt32(&ctx.failedBookmarkCount, 1)
}

func (ctx *TestContext) getBookmark() string {
	return ctx.bookmark.Load().(string)
}

func (ctx *TestContext) setBookmark(bookmark string) {
	ctx.bookmark.Store(bookmark)
}

func (ctx *TestContext) processSummary(summary neo4j.ResultSummary) {
	ctx.addRead()

	if summary == nil {
		return
	}

	var count int32
	lastCountInt, _ := ctx.readNodeCountsByServer.LoadOrStore(summary.Server().Address(), &count)
	lastCount := lastCountInt.(*int32)

	atomic.AddInt32(lastCount, 1)
}

func (ctx *TestContext) handleFailure(err error) bool {
	if err != nil && strings.Contains(err.Error(), "no longer accepts writes") {
		atomic.AddInt32(&ctx.leaderSwitchCount, 1)
		return true
	}

	return false
}

// PrintStats writes summary information about the completed test
func (ctx *TestContext) PrintStats() {
	fmt.Printf("Stats:\n")
	fmt.Printf("\tNodes Created: %d\n", ctx.createdNodeCount)
	fmt.Printf("\tNodes Read: %d\n", ctx.readNodeCount)
	fmt.Printf("\tBookmarks Failed: %d\n", ctx.failedBookmarkCount)
	fmt.Printf("\tLeader Switches: %d\n", ctx.leaderSwitchCount)
	fmt.Printf("\tRead Counts By Server:\n")

	ctx.readNodeCountsByServer.Range(func(key, value interface{}) bool {
		fmt.Printf("\t\t%s: %d\n", key.(string), *(value.(*int32)))
		return true
	})
}

func ExpectNoError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Expected no error: %s", err.Error()))
	}
}

func ExpectNotNil(x interface{}) {
	if x == nil {
		panic("Expected not nil")
	}
}

func ExpectNil(x interface{}) {
	if x != nil {
		panic(fmt.Sprintf("Expected nil but was %+v (%T)", x, x))
	}
}

func ExpectTrue(b bool) {
	if !b {
		panic("Expected true")
	}
}

func ExpectFalse(b bool) {
	if b {
		panic("Expected false")
	}
}

func ExpectInt(a, b int) {
	if a != b {
		panic(fmt.Sprintf("%d != %d", a, b))
	}
}

func newStressSession(driver neo4j.Driver, useBookmark bool, accessMode neo4j.AccessMode, ctx *TestContext) neo4j.Session {
	var session neo4j.Session
	var err error

	if useBookmark {
		session, err = driver.Session(accessMode, ctx.getBookmark())
	} else {
		session, err = driver.Session(accessMode)
	}

	ExpectNoError(err)
	ExpectNotNil(session)

	return session
}

// ReadQueryExecutor returns a new test executor which reads using Session.Run
func ReadQueryExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		result, err := session.Run("MATCH (n) RETURN n LIMIT 1", nil)
		ExpectNoError(err)

		if result.Next() {
			nodeInt := result.Record().Values[0]
			ExpectNotNil(nodeInt)

			_, ok := nodeInt.(neo4j.Node)
			ExpectTrue(ok)
		}
		ExpectNil(result.Err())
		ExpectFalse(result.Next())

		summary, err := result.Consume()
		ExpectNoError(err)
		ExpectNotNil(summary)

		ctx.processSummary(summary)
	}
}

// ReadQueryInTxExecutor returns a new test executor which reads using Transaction.Run
func ReadQueryInTxExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		tx, err := session.BeginTransaction()
		ExpectNoError(err)
		defer tx.Close()

		result, err := tx.Run("MATCH (n) RETURN n LIMIT 1", nil)
		ExpectNoError(err)

		if result.Next() {
			nodeInt := result.Record().Values[0]
			ExpectNotNil(nodeInt)

			_, ok := nodeInt.(neo4j.Node)
			ExpectTrue(ok)
		}
		ExpectNil(result.Err())
		ExpectFalse(result.Next())

		summary, err := result.Consume()
		ExpectNoError(err)
		ExpectNotNil(summary)

		err = tx.Commit()
		ExpectNoError(err)

		ctx.processSummary(summary)
	}
}

// ReadQueryWithReadTransactionExecutor returns a new test executor which reads using Session.ReadTransaction
func ReadQueryWithReadTransactionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		summary, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("MATCH (n) RETURN n LIMIT 1", nil)
			if err != nil {
				return nil, err
			}

			if result.Next() {
				nodeInt := result.Record().Values[0]
				ExpectNotNil(nodeInt)

				_, ok := nodeInt.(neo4j.Node)
				ExpectTrue(ok)
			}

			return result.Consume()
		})

		ExpectNoError(err)
		ExpectNotNil(summary)

		ctx.processSummary(summary.(neo4j.ResultSummary))
	}
}

// WriteQueryExecutor returns a new test executor which writes using Session.Run
func WriteQueryExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeWrite, ctx)
		defer session.Close()

		result, err := session.Run("CREATE ()", nil)
		ExpectNoError(err)

		summary, err := result.Consume()
		if !ctx.handleFailure(err) {
			ExpectNoError(err)
			ExpectInt(summary.Counters().NodesCreated(), 1)

			ctx.setBookmark(session.LastBookmark())

			ctx.addCreated()
		}
	}
}

// WriteQueryInTxExecutor returns a new test executor which writes using Transaction.Run
func WriteQueryInTxExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeWrite, ctx)
		defer session.Close()

		tx, err := session.BeginTransaction()
		ExpectNoError(err)
		defer tx.Close()

		result, err := tx.Run("CREATE ()", nil)
		ExpectNoError(err)

		summary, err := result.Consume()
		if !ctx.handleFailure(err) {
			ExpectNoError(err)
			ExpectInt(summary.Counters().NodesCreated(), 1)

			err = tx.Commit()
			ExpectNoError(err)

			ctx.setBookmark(session.LastBookmark())

			ctx.addCreated()
		}
	}
}

// WriteQueryWithWriteTransactionExecutor returns a new test executor which writes using Session.WriteTransaction
func WriteQueryWithWriteTransactionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeWrite, ctx)
		defer session.Close()

		summary, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("CREATE ()", nil)
			if err != nil {
				return nil, err
			}
			return result.Consume()
		})
		ExpectNoError(err)

		if !ctx.handleFailure(err) {
			ExpectNoError(err)
			ExpectNotNil(summary)
			ExpectInt(summary.(neo4j.ResultSummary).Counters().NodesCreated(), 1)

			ctx.setBookmark(session.LastBookmark())

			ctx.addCreated()
		}
	}
}

// WriteQueryInReadSessionExecutor returns a new test executor which tries writes using Session.Run with read access mode
func WriteQueryInReadSessionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		_, err := session.Run("CREATE ()", nil)
		ExpectNotNil(err)
	}
}

// WriteQueryInTxInReadSessionExecutor returns a new test executor which tries writes using Transaction.Run with read access mode
func WriteQueryInTxInReadSessionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		tx, err := session.BeginTransaction()
		ExpectNoError(err)
		defer tx.Close()

		_, err = tx.Run("CREATE ()", nil)
		ExpectNotNil(err)
	}
}

// FailingQueryExecutor returns a new test executor which fails in streaming using Session.Run
func FailingQueryExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		result, err := session.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
		ExpectNoError(err)

		summary, err := result.Consume()
		ExpectNotNil(err)
		//Expect(err).To(BeArithmeticError())
		ExpectNil(summary)
	}
}

// FailingQueryInTxExecutor returns a new test executor which fails in streaming using Transaction.Run
func FailingQueryInTxExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		tx, err := session.BeginTransaction()
		ExpectNoError(err)
		defer tx.Close()

		result, err := tx.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
		ExpectNoError(err)

		summary, err := result.Consume()
		ExpectNotNil(err)
		//Expect(err).To(BeArithmeticError())
		ExpectNil(summary)
	}
}

// FailingQueryWithReadTransactionExecutor returns a new test executor which fails in streaming using Session.ReadTransaction
func FailingQueryWithReadTransactionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		summary, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
			if err != nil {
				return nil, err
			}

			return result.Consume()
		})
		if !neo4j.IsNeo4jError(err) {
			panic(err)
		}

		ExpectNil(summary)
	}
}

// FailingQueryWithWriteTransactionExecutor returns a new test executor which fails in streaming using Session.WriteTransaction
func FailingQueryWithWriteTransactionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		summary, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
			if err != nil {
				return nil, err
			}

			return result.Consume()
		})

		if !neo4j.IsNeo4jError(err) {
			panic(err)
		}

		ExpectNil(summary)
	}
}

// WrongQueryExecutor returns a new test executor which fails using Session.Run
func WrongQueryExecutor(driver neo4j.Driver) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, false, neo4j.AccessModeRead, ctx)
		defer session.Close()

		_, err := session.Run("RETURN wrongThing", nil)
		ExpectNotNil(err)
	}
}

// WrongQueryInTxExecutor returns a new test executor which fails using Transaction.Run
func WrongQueryInTxExecutor(driver neo4j.Driver) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, false, neo4j.AccessModeWrite, ctx)
		defer session.Close()

		tx, err := session.BeginTransaction()
		ExpectNoError(err)
		defer tx.Close()

		_, err = tx.Run("RETURN wrongThing", nil)
		ExpectNotNil(err)
	}
}
