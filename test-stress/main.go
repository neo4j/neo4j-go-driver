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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Tool used for verifying driver under load.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"math/rand"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const TestNumberOfGoRoutines = 20

func stressTest(ctx *TestContext, duration time.Duration,
	successfulExecutors []func(*TestContext), failingExecutors []func(*TestContext)) {

	fmt.Printf("Stressing with random executors on %d Go routines, will run for %s\n", TestNumberOfGoRoutines, duration)
	successfulExecutorsLen := len(successfulExecutors)
	failingExecutorsLen := len(failingExecutors)

	var waiter sync.WaitGroup

	for i := 0; i < TestNumberOfGoRoutines; i++ {
		waiter.Add(1)

		go func() {
			defer waiter.Done()

			var executor func(*TestContext)
			for !ctx.ShouldStop() {
				if rand.Intn(10) < 7 {
					executor = successfulExecutors[rand.Intn(successfulExecutorsLen)]
				} else {
					executor = failingExecutors[rand.Intn(failingExecutorsLen)]
				}

				executor(ctx)
			}
		}()
	}

	time.Sleep(duration)
	ctx.Stop()
	waiter.Wait()

	if ctx.createdNodeCount == 0 {
		panic("No nodes at all were created")
	}
	if ctx.readNodeCount == 0 {
		panic("No nodes at all were read")
	}
}

func main() {
	var (
		uri              string
		user             string
		password         string
		causalClustering bool
		seconds          int
		bigDataTest      bool
	)

	flag.StringVar(&uri, "uri", "bolt://localhost:7687", "Database URI")
	flag.StringVar(&user, "user", "neo4j", "User name")
	flag.StringVar(&password, "password", "pass", "Password")
	flag.BoolVar(&causalClustering, "cluster", false, "Causal clustering")
	flag.IntVar(&seconds, "seconds", 15, "Duration in seconds")
	flag.BoolVar(&bigDataTest, "big", false, "Big data test")
	flag.Parse()

	ctx := context.Background()
	auth := neo4j.BasicAuth(user, password, "")
	driver, err := neo4j.NewDriverWithContext(uri, auth, func(conf *config.Config) {
		conf.Log = neo4j.ConsoleLogger(neo4j.WARNING)
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("Cleaning up database")
	cleanDb(ctx, driver)

	testContext := NewTestContext(driver)

	successfulQueryExecutors := []func(*TestContext){}
	failingQueryExecutors := []func(*TestContext){}

	if causalClustering {
		successfulQueryExecutors = append(successfulQueryExecutors,
			ReadQueryWithReadTransactionExecutor(ctx, driver, true),
			ReadQueryWithReadTransactionExecutor(ctx, driver, false),
			WriteQueryWithWriteTransactionExecutor(ctx, driver, true),
			WriteQueryWithWriteTransactionExecutor(ctx, driver, false),
		)
		failingQueryExecutors = append(failingQueryExecutors,
			FailingQueryWithReadTransactionExecutor(ctx, driver, true),
			FailingQueryWithReadTransactionExecutor(ctx, driver, false),
			FailingQueryWithWriteTransactionExecutor(ctx, driver, true),
			FailingQueryWithWriteTransactionExecutor(ctx, driver, false),
			WrongQueryExecutor(ctx, driver),
			WriteQueryInReadSessionExecutor(ctx, driver, true),
			WriteQueryInReadSessionExecutor(ctx, driver, false),
		)
	} else {
		successfulQueryExecutors = append(successfulQueryExecutors,
			ReadQueryWithReadTransactionExecutor(ctx, driver, true),
			ReadQueryWithReadTransactionExecutor(ctx, driver, false),
			WriteQueryWithWriteTransactionExecutor(ctx, driver, true),
			WriteQueryWithWriteTransactionExecutor(ctx, driver, false),
			WriteQueryExecutor(ctx, driver, true),
			WriteQueryExecutor(ctx, driver, false),
			ReadQueryExecutor(ctx, driver, true),
			ReadQueryExecutor(ctx, driver, false),
			WriteQueryInTxExecutor(ctx, driver, true),
			WriteQueryInTxExecutor(ctx, driver, false),
			ReadQueryInTxExecutor(ctx, driver, true),
			ReadQueryInTxExecutor(ctx, driver, false),
		)
		failingQueryExecutors = append(failingQueryExecutors,
			FailingQueryWithReadTransactionExecutor(ctx, driver, true),
			FailingQueryWithReadTransactionExecutor(ctx, driver, false),
			FailingQueryWithWriteTransactionExecutor(ctx, driver, true),
			FailingQueryWithWriteTransactionExecutor(ctx, driver, false),
			WrongQueryExecutor(ctx, driver),
			WrongQueryInTxExecutor(ctx, driver),
			FailingQueryExecutor(ctx, driver, true),
			FailingQueryExecutor(ctx, driver, false),
			FailingQueryInTxExecutor(ctx, driver, true),
			FailingQueryInTxExecutor(ctx, driver, false),
		)
	}

	if bigDataTest {
		fmt.Println("Running big data test")
		runBigDataThing(ctx, driver)
	}

	stressTest(testContext, time.Duration(seconds)*time.Second,
		successfulQueryExecutors, failingQueryExecutors)
	testContext.PrintStats()
}
