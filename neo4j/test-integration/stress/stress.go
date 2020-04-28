/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

package stress

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	//. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/gomega"
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

func newStressSession(driver neo4j.Driver, useBookmark bool, accessMode neo4j.AccessMode, ctx *TestContext) neo4j.Session {
	var session neo4j.Session
	var err error

	if useBookmark {
		session, err = driver.Session(accessMode, ctx.getBookmark())
	} else {
		session, err = driver.Session(accessMode)
	}

	Expect(err).To(BeNil())
	Expect(session).NotTo(BeNil())

	return session
}

func newStressTransaction(session neo4j.Session, useBookmark bool, ctx *TestContext) neo4j.Transaction {
	var tx neo4j.Transaction
	var err error

	if useBookmark {
		for {
			if tx, err = session.BeginTransaction(); err == nil {
				return tx
			}

			if neo4j.IsTransientError(err) {
				ctx.addBookmarkFailure()
			} else {
				Expect(neo4j.IsTransientError(err)).To(BeTrue())
				//Expect(err).To(BeTransientError(nil, nil))
			}
		}
	}

	tx, err = session.BeginTransaction()
	Expect(err).To(BeNil())
	Expect(tx).NotTo(BeNil())
	return tx
}

// ReadQueryExecutor returns a new test executor which reads using Session.Run
func ReadQueryExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		result, err := session.Run("MATCH (n) RETURN n LIMIT 1", nil)
		Expect(err).To(BeNil())

		if result.Next() {
			nodeInt := result.Record().GetByIndex(0)
			Expect(nodeInt).NotTo(BeNil())

			_, ok := nodeInt.(neo4j.Node)
			Expect(ok).To(BeTrue())
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())

		summary, err := result.Summary()
		Expect(err).To(BeNil())
		Expect(summary).NotTo(BeNil())

		ctx.processSummary(summary)
	}
}

// ReadQueryInTxExecutor returns a new test executor which reads using Transaction.Run
func ReadQueryInTxExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		tx := newStressTransaction(session, useBookmark, ctx)
		defer tx.Close()

		result, err := tx.Run("MATCH (n) RETURN n LIMIT 1", nil)
		Expect(err).To(BeNil())

		if result.Next() {
			nodeInt := result.Record().GetByIndex(0)
			Expect(nodeInt).NotTo(BeNil())

			_, ok := nodeInt.(neo4j.Node)
			Expect(ok).To(BeTrue())
		}
		Expect(result.Err()).To(BeNil())
		Expect(result.Next()).To(BeFalse())

		summary, err := result.Summary()
		Expect(err).To(BeNil())
		Expect(summary).NotTo(BeNil())

		err = tx.Commit()
		Expect(err).To(BeNil())

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
			Expect(err).To(BeNil())

			if result.Next() {
				nodeInt := result.Record().GetByIndex(0)
				Expect(nodeInt).NotTo(BeNil())

				_, ok := nodeInt.(neo4j.Node)
				Expect(ok).To(BeTrue())
			}
			Expect(result.Err()).To(BeNil())
			Expect(result.Next()).To(BeFalse())

			return result.Summary()
		})

		Expect(err).To(BeNil())
		Expect(summary).NotTo(BeNil())

		ctx.processSummary(summary.(neo4j.ResultSummary))
	}
}

// WriteQueryExecutor returns a new test executor which writes using Session.Run
func WriteQueryExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeWrite, ctx)
		defer session.Close()

		result, err := session.Run("CREATE ()", nil)
		Expect(err).To(BeNil())

		summary, err := result.Consume()
		if !ctx.handleFailure(err) {
			Expect(err).To(BeNil())
			Expect(summary.Counters().NodesCreated()).To(BeEquivalentTo(1))

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

		tx := newStressTransaction(session, useBookmark, ctx)
		defer tx.Close()

		result, err := tx.Run("CREATE ()", nil)
		Expect(err).To(BeNil())

		summary, err := result.Consume()
		if !ctx.handleFailure(err) {
			Expect(err).To(BeNil())
			Expect(summary.Counters().NodesCreated()).To(BeEquivalentTo(1))

			err = tx.Commit()
			Expect(err).To(BeNil())

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
			Expect(err).To(BeNil())

			return result.Consume()
		})

		if !ctx.handleFailure(err) {
			Expect(err).To(BeNil())
			Expect(summary).NotTo(BeNil())
			Expect(summary.(neo4j.ResultSummary).Counters().NodesCreated()).To(BeEquivalentTo(1))

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

		result, err := session.Run("CREATE ()", nil)
		Expect(err).To(BeNil())

		summary, err := result.Consume()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeGenericError(ContainSubstring("write queries cannot be performed in read access mode")))
		Expect(summary).To(BeNil())
	}
}

// WriteQueryInTxInReadSessionExecutor returns a new test executor which tries writes using Transaction.Run with read access mode
func WriteQueryInTxInReadSessionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		tx := newStressTransaction(session, useBookmark, ctx)
		defer tx.Close()

		result, err := tx.Run("CREATE ()", nil)
		Expect(err).To(BeNil())

		summary, err := result.Consume()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeGenericError(ContainSubstring("write queries cannot be performed in read access mode")))
		Expect(summary).To(BeNil())
	}
}

// FailingQueryExecutor returns a new test executor which fails in streaming using Session.Run
func FailingQueryExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		result, err := session.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
		Expect(err).To(BeNil())

		summary, err := result.Consume()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeArithmeticError())
		Expect(summary).To(BeNil())
	}
}

// FailingQueryInTxExecutor returns a new test executor which fails in streaming using Transaction.Run
func FailingQueryInTxExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		tx := newStressTransaction(session, useBookmark, ctx)
		defer tx.Close()

		result, err := tx.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
		Expect(err).To(BeNil())

		summary, err := result.Consume()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeArithmeticError())
		Expect(summary).To(BeNil())
	}
}

// FailingQueryWithReadTransactionExecutor returns a new test executor which fails in streaming using Session.ReadTransaction
func FailingQueryWithReadTransactionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		summary, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
			Expect(err).To(BeNil())

			return result.Consume()
		})

		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeArithmeticError())
		Expect(summary).To(BeNil())
	}
}

// FailingQueryWithWriteTransactionExecutor returns a new test executor which fails in streaming using Session.WriteTransaction
func FailingQueryWithWriteTransactionExecutor(driver neo4j.Driver, useBookmark bool) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, useBookmark, neo4j.AccessModeRead, ctx)
		defer session.Close()

		summary, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
			Expect(err).To(BeNil())

			return result.Consume()
		})

		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeArithmeticError())
		Expect(summary).To(BeNil())
	}
}

// WrongQueryExecutor returns a new test executor which fails using Session.Run
func WrongQueryExecutor(driver neo4j.Driver) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, false, neo4j.AccessModeRead, ctx)
		defer session.Close()

		result, err := session.Run("RETURN wrongThing", nil)
		Expect(err).To(BeNil())

		_, err = result.Consume()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeSyntaxError())
	}
}

// WrongQueryInTxExecutor returns a new test executor which fails using Transaction.Run
func WrongQueryInTxExecutor(driver neo4j.Driver) func(ctx *TestContext) {
	return func(ctx *TestContext) {
		session := newStressSession(driver, false, neo4j.AccessModeWrite, ctx)
		defer session.Close()

		tx := newStressTransaction(session, false, ctx)
		defer tx.Close()

		result, err := tx.Run("RETURN wrongThing", nil)
		Expect(err).To(BeNil())

		_, err = result.Consume()
		Expect(err).ToNot(BeNil())
		//Expect(err).To(BeSyntaxError())
	}
}
