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
	"flag"
	"math/rand"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

const (
	TestDuration           = 15 * time.Second
	TestNumberOfGoRoutines = 20
)

func stressTest(ctx *TestContext, successfulExecutors []func(*TestContext), failingExecutors []func(*TestContext)) {
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

	time.Sleep(TestDuration)
	ctx.Stop()
	waiter.Wait()
}

func main() {
	var (
		uri      string
		user     string
		password string
	)

	flag.StringVar(&uri, "uri", "bolt://localhost:7687", "Database URI")
	flag.StringVar(&user, "user", "neo4j", "User name")
	flag.StringVar(&password, "password", "pass", "Password")
	flag.Parse()

	causalClustering := false

	auth := neo4j.BasicAuth(user, password, "")
	driver, err := neo4j.NewDriver(uri, auth, func(conf *neo4j.Config) {
		conf.Log = neo4j.ConsoleLogger(neo4j.WARNING)
	})
	if err != nil {
		panic(err)
	}
	ctx := NewTestContext(driver)

	successfulQueryExecutors := []func(*TestContext){
		ReadQueryExecutor(driver, true),
		ReadQueryExecutor(driver, false),
		ReadQueryInTxExecutor(driver, true),
		ReadQueryInTxExecutor(driver, false),
		ReadQueryWithReadTransactionExecutor(driver, true),
		ReadQueryWithReadTransactionExecutor(driver, false),
		WriteQueryExecutor(driver, true),
		WriteQueryExecutor(driver, false),
		WriteQueryInTxExecutor(driver, true),
		WriteQueryInTxExecutor(driver, false),
		WriteQueryWithWriteTransactionExecutor(driver, true),
		WriteQueryWithWriteTransactionExecutor(driver, false),
	}

	failingQueryExecutors := []func(*TestContext){
		FailingQueryExecutor(driver, true),
		FailingQueryExecutor(driver, false),
		FailingQueryInTxExecutor(driver, true),
		FailingQueryInTxExecutor(driver, false),
		FailingQueryWithReadTransactionExecutor(driver, true),
		FailingQueryWithReadTransactionExecutor(driver, false),
		FailingQueryWithWriteTransactionExecutor(driver, true),
		FailingQueryWithWriteTransactionExecutor(driver, false),
		WrongQueryExecutor(driver),
		WrongQueryInTxExecutor(driver),
	}

	if causalClustering {
		failingQueryExecutors = append(failingQueryExecutors,
			WriteQueryInReadSessionExecutor(driver, true),
			WriteQueryInReadSessionExecutor(driver, false),
			WriteQueryInTxInReadSessionExecutor(driver, true),
			WriteQueryInTxInReadSessionExecutor(driver, false),
		)
	}

	stressTest(ctx, successfulQueryExecutors, failingQueryExecutors)
	ctx.PrintStats()
}
