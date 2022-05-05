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

// Tool used for verifying driver under load.
package main

import (
	"flag"
	"fmt"
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

	auth := neo4j.BasicAuth(user, password, "")
	driver, err := neo4j.NewDriver(uri, auth, func(conf *neo4j.Config) {
		conf.Log = neo4j.ConsoleLogger(neo4j.WARNING)
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("Cleaning up database")
	cleanDb(driver)

	ctx := NewTestContext(driver)

	successfulQueryExecutors := []func(*TestContext){}
	failingQueryExecutors := []func(*TestContext){}

	if causalClustering {
		successfulQueryExecutors = append(successfulQueryExecutors,
			ReadQueryWithReadTransactionExecutor(driver, true),
			ReadQueryWithReadTransactionExecutor(driver, false),
			WriteQueryWithWriteTransactionExecutor(driver, true),
			WriteQueryWithWriteTransactionExecutor(driver, false),
		)
		failingQueryExecutors = append(failingQueryExecutors,
			FailingQueryWithReadTransactionExecutor(driver, true),
			FailingQueryWithReadTransactionExecutor(driver, false),
			FailingQueryWithWriteTransactionExecutor(driver, true),
			FailingQueryWithWriteTransactionExecutor(driver, false),
			WrongQueryExecutor(driver),
			WriteQueryInReadSessionExecutor(driver, true),
			WriteQueryInReadSessionExecutor(driver, false),
		)
	} else {
		successfulQueryExecutors = append(successfulQueryExecutors,
			ReadQueryWithReadTransactionExecutor(driver, true),
			ReadQueryWithReadTransactionExecutor(driver, false),
			WriteQueryWithWriteTransactionExecutor(driver, true),
			WriteQueryWithWriteTransactionExecutor(driver, false),
			WriteQueryExecutor(driver, true),
			WriteQueryExecutor(driver, false),
			ReadQueryExecutor(driver, true),
			ReadQueryExecutor(driver, false),
			WriteQueryInTxExecutor(driver, true),
			WriteQueryInTxExecutor(driver, false),
			ReadQueryInTxExecutor(driver, true),
			ReadQueryInTxExecutor(driver, false),
		)
		failingQueryExecutors = append(failingQueryExecutors,
			FailingQueryWithReadTransactionExecutor(driver, true),
			FailingQueryWithReadTransactionExecutor(driver, false),
			FailingQueryWithWriteTransactionExecutor(driver, true),
			FailingQueryWithWriteTransactionExecutor(driver, false),
			WrongQueryExecutor(driver),
			WrongQueryInTxExecutor(driver),
			FailingQueryExecutor(driver, true),
			FailingQueryExecutor(driver, false),
			FailingQueryInTxExecutor(driver, true),
			FailingQueryInTxExecutor(driver, false),
		)
	}

	if bigDataTest {
		fmt.Println("Running big data test")
		runBigDataThing(driver)
	}

	stressTest(ctx, time.Duration(seconds)*time.Second,
		successfulQueryExecutors, failingQueryExecutors)
	ctx.PrintStats()
}
