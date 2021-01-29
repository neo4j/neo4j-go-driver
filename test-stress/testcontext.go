/*
 * Copyright (c) "Neo4j"
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

// PrintStats writes summary information about the completed test
func (ctx *TestContext) PrintStats() {
	fmt.Printf("Stats:\n")
	fmt.Printf("\tNodes Created: %d\n", ctx.createdNodeCount)
	fmt.Printf("\tNodes Read: %d\n", ctx.readNodeCount)
	fmt.Printf("\tRead Counts By Server:\n")

	ctx.readNodeCountsByServer.Range(func(key, value interface{}) bool {
		fmt.Printf("\t\t%s: %d\n", key.(string), *(value.(*int32)))
		return true
	})
}
