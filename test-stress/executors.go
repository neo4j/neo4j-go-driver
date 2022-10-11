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

package main

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func ExpectNoError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Expected no error: %s", err.Error()))
	}
}

func ExpectNotNil(x any) {
	if x == nil {
		panic("Expected not nil")
	}
}

func ExpectNil(x any) {
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

func newStressSession(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool, accessMode neo4j.AccessMode, testContext *TestContext) neo4j.SessionWithContext {
	var session neo4j.SessionWithContext
	if useBookmark {
		session = driver.NewSession(ctx, neo4j.SessionConfig{
			AccessMode: accessMode,
			Bookmarks:  testContext.getBookmarks(),
		})
	} else {
		session = driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: accessMode})
	}
	ExpectNotNil(session)
	return session
}

// ReadQueryExecutor returns a new test executor which reads using Session.Run
func ReadQueryExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		result, err := session.Run(ctx, "MATCH (n) RETURN n LIMIT 1", nil)
		ExpectNoError(err)

		if result.Next(ctx) {
			nodeInt := result.Record().Values[0]
			ExpectNotNil(nodeInt)

			_, ok := nodeInt.(neo4j.Node)
			ExpectTrue(ok)
		}
		ExpectNil(result.Err())
		ExpectFalse(result.Next(ctx))

		summary, err := result.Consume(ctx)
		ExpectNoError(err)
		ExpectNotNil(summary)

		testContext.processSummary(summary)
	}
}

// ReadQueryInTxExecutor returns a new test executor which reads using Transaction.Run
func ReadQueryInTxExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		tx, err := session.BeginTransaction(ctx)
		ExpectNoError(err)
		defer tx.Close(ctx)

		result, err := tx.Run(ctx, "MATCH (n) RETURN n LIMIT 1", nil)
		ExpectNoError(err)

		if result.Next(ctx) {
			nodeInt := result.Record().Values[0]
			ExpectNotNil(nodeInt)

			_, ok := nodeInt.(neo4j.Node)
			ExpectTrue(ok)
		}
		ExpectNil(result.Err())
		ExpectFalse(result.Next(ctx))

		summary, err := result.Consume(ctx)
		ExpectNoError(err)
		ExpectNotNil(summary)

		err = tx.Commit(ctx)
		ExpectNoError(err)

		testContext.processSummary(summary)
	}
}

// ReadQueryWithReadTransactionExecutor returns a new test executor which reads using Session.ExecuteRead
func ReadQueryWithReadTransactionExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		summary, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, "MATCH (n) RETURN n LIMIT 1", nil)
			if err != nil {
				return nil, err
			}

			if result.Next(ctx) {
				nodeInt := result.Record().Values[0]
				ExpectNotNil(nodeInt)

				_, ok := nodeInt.(neo4j.Node)
				ExpectTrue(ok)
			}

			return result.Consume(ctx)
		})

		ExpectNoError(err)
		ExpectNotNil(summary)

		testContext.processSummary(summary.(neo4j.ResultSummary))
	}
}

// WriteQueryExecutor returns a new test executor which writes using Session.Run
func WriteQueryExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeWrite, testContext)
		defer session.Close(ctx)

		result, err := session.Run(ctx, "CREATE ()", nil)
		ExpectNoError(err)

		summary, err := result.Consume(ctx)
		ExpectNoError(err)
		ExpectInt(summary.Counters().NodesCreated(), 1)

		testContext.setBookmarks(session.LastBookmarks())

		testContext.addCreated()
	}
}

// WriteQueryInTxExecutor returns a new test executor which writes using Transaction.Run
func WriteQueryInTxExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeWrite, testContext)
		defer session.Close(ctx)

		tx, err := session.BeginTransaction(ctx)
		ExpectNoError(err)
		defer tx.Close(ctx)

		result, err := tx.Run(ctx, "CREATE ()", nil)
		ExpectNoError(err)

		summary, err := result.Consume(ctx)
		ExpectNoError(err)
		ExpectInt(summary.Counters().NodesCreated(), 1)

		err = tx.Commit(ctx)
		ExpectNoError(err)

		testContext.setBookmarks(session.LastBookmarks())

		testContext.addCreated()
	}
}

// WriteQueryWithWriteTransactionExecutor returns a new test executor which writes using Session.ExecuteWrite
func WriteQueryWithWriteTransactionExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeWrite, testContext)
		defer session.Close(ctx)

		summary, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, "CREATE ()", nil)
			if err != nil {
				return nil, err
			}
			return result.Consume(ctx)
		})
		ExpectNoError(err)
		ExpectNotNil(summary)
		ExpectInt(summary.(neo4j.ResultSummary).Counters().NodesCreated(), 1)
		testContext.setBookmarks(session.LastBookmarks())
		testContext.addCreated()
	}
}

// WriteQueryInReadSessionExecutor returns a new test executor which tries to perform writes using Session.Run with read access mode
func WriteQueryInReadSessionExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		_, err := session.Run(ctx, "CREATE ()", nil)
		ExpectNotNil(err)
	}
}

// WriteQueryInTxInReadSessionExecutor returns a new test executor which tries writes using Transaction.Run with read access mode
func WriteQueryInTxInReadSessionExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		tx, err := session.BeginTransaction(ctx)
		ExpectNoError(err)
		defer tx.Close(ctx)

		_, err = tx.Run(ctx, "CREATE ()", nil)
		ExpectNotNil(err)
	}
}

// FailingQueryExecutor returns a new test executor which fails in streaming using Session.Run
func FailingQueryExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		result, err := session.Run(ctx, "UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
		ExpectNoError(err)

		summary, err := result.Consume(ctx)
		ExpectNotNil(err)
		ExpectNil(summary)
	}
}

// FailingQueryInTxExecutor returns a new test executor which fails in streaming using Transaction.Run
func FailingQueryInTxExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		tx, err := session.BeginTransaction(ctx)
		ExpectNoError(err)
		defer tx.Close(ctx)

		result, err := tx.Run(ctx, "UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
		ExpectNoError(err)

		summary, err := result.Consume(ctx)
		ExpectNotNil(err)
		ExpectNil(summary)
	}
}

// FailingQueryWithReadTransactionExecutor returns a new test executor which fails in streaming using Session.ExecuteRead
func FailingQueryWithReadTransactionExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		summary, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, "UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
			if err != nil {
				return nil, err
			}

			return result.Consume(ctx)
		})
		if !neo4j.IsNeo4jError(err) {
			panic(err)
		}

		ExpectNil(summary)
	}
}

// FailingQueryWithWriteTransactionExecutor returns a new test executor which fails in streaming using Session.ExecuteWrite
func FailingQueryWithWriteTransactionExecutor(ctx context.Context, driver neo4j.DriverWithContext, useBookmark bool) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, useBookmark, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		summary, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, "UNWIND [10, 5, 0] AS x RETURN 10 / x", nil)
			if err != nil {
				return nil, err
			}

			return result.Consume(ctx)
		})

		if !neo4j.IsNeo4jError(err) {
			panic(err)
		}

		ExpectNil(summary)
	}
}

// WrongQueryExecutor returns a new test executor which fails using Session.Run
func WrongQueryExecutor(ctx context.Context, driver neo4j.DriverWithContext) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, false, neo4j.AccessModeRead, testContext)
		defer session.Close(ctx)

		_, err := session.Run(ctx, "RETURN wrongThing", nil)
		ExpectNotNil(err)
	}
}

// WrongQueryInTxExecutor returns a new test executor which fails using Transaction.Run
func WrongQueryInTxExecutor(ctx context.Context, driver neo4j.DriverWithContext) func(*TestContext) {
	return func(testContext *TestContext) {
		session := newStressSession(ctx, driver, false, neo4j.AccessModeWrite, testContext)
		defer session.Close(ctx)

		tx, err := session.BeginTransaction(ctx)
		ExpectNoError(err)
		defer tx.Close(ctx)

		_, err = tx.Run(ctx, "RETURN wrongThing", nil)
		ExpectNotNil(err)
	}
}
