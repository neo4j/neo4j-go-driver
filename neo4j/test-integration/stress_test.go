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

package test_integration

import (
	"math/rand"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/stress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stress Test", func() {
	const TestDuration = 1 * time.Minute
	const TestNumberOfGoRoutines = 20

	stessTest := func(ctx *stress.TestContext, successfulExecutors []func(*stress.TestContext), failingExecutors []func(*stress.TestContext)) {
		successfulExecutorsLen := len(successfulExecutors)
		failingExecutorsLen := len(failingExecutors)

		var waiter sync.WaitGroup

		for i := 0; i < TestNumberOfGoRoutines; i++ {
			waiter.Add(1)

			go func() {
				defer waiter.Done()
				defer GinkgoRecover()

				var executor func(*stress.TestContext)
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

	Context("Single Instance", func() {
		var ctx *stress.TestContext
		var driver neo4j.Driver
		var server *control.SingleInstance
		var err error
		var successfulQueryExecutors []func(*stress.TestContext)
		var failingQueryExecutors []func(ctx2 *stress.TestContext)

		BeforeEach(func() {
			server, err = control.EnsureSingleInstance()
			Expect(err).To(BeNil())
			Expect(server).NotTo(BeNil())

			driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.Config())
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			ctx = stress.NewTestContext(driver)

			successfulQueryExecutors = append(successfulQueryExecutors,
				stress.ReadQueryExecutor(driver, true),
				stress.ReadQueryExecutor(driver, false),
				stress.ReadQueryInTxExecutor(driver, true),
				stress.ReadQueryInTxExecutor(driver, false),
				stress.ReadQueryWithReadTransactionExecutor(driver, true),
				stress.ReadQueryWithReadTransactionExecutor(driver, false),
				stress.WriteQueryExecutor(driver, true),
				stress.WriteQueryExecutor(driver, false),
				stress.WriteQueryInTxExecutor(driver, true),
				stress.WriteQueryInTxExecutor(driver, false),
				stress.WriteQueryWithWriteTransactionExecutor(driver, true),
				stress.WriteQueryWithWriteTransactionExecutor(driver, false),
			)

			failingQueryExecutors = append(failingQueryExecutors,
				stress.FailingQueryExecutor(driver, true),
				stress.FailingQueryExecutor(driver, false),
				stress.FailingQueryInTxExecutor(driver, true),
				stress.FailingQueryInTxExecutor(driver, false),
				stress.FailingQueryWithReadTransactionExecutor(driver, true),
				stress.FailingQueryWithReadTransactionExecutor(driver, false),
				stress.FailingQueryWithWriteTransactionExecutor(driver, true),
				stress.FailingQueryWithWriteTransactionExecutor(driver, false),
				stress.WrongQueryExecutor(driver),
				stress.WrongQueryInTxExecutor(driver),
			)
		})

		AfterEach(func() {
			ctx.PrintStats()
		})

		It("should complete without any errors", func() {
			stessTest(ctx, successfulQueryExecutors, failingQueryExecutors)
		})
	})

	Context("Causal Cluster", func() {
		var ctx *stress.TestContext
		var driver neo4j.Driver
		var cluster *control.Cluster
		var err error
		var successfulQueryExecutors []func(*stress.TestContext)
		var failingQueryExecutors []func(ctx2 *stress.TestContext)

		BeforeEach(func() {
			cluster, err = control.EnsureCluster()
			Expect(err).To(BeNil())
			Expect(cluster).NotTo(BeNil())

			driver, err = neo4j.NewDriver(cluster.AnyFollower().RoutingURI(), cluster.AuthToken(), cluster.Config())
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			ctx = stress.NewTestContext(driver)

			successfulQueryExecutors = append(successfulQueryExecutors,
				stress.ReadQueryExecutor(driver, true),
				stress.ReadQueryExecutor(driver, false),
				stress.ReadQueryInTxExecutor(driver, true),
				stress.ReadQueryInTxExecutor(driver, false),
				stress.ReadQueryWithReadTransactionExecutor(driver, true),
				stress.ReadQueryWithReadTransactionExecutor(driver, false),
				stress.WriteQueryExecutor(driver, true),
				stress.WriteQueryExecutor(driver, false),
				stress.WriteQueryInTxExecutor(driver, true),
				stress.WriteQueryInTxExecutor(driver, false),
				stress.WriteQueryWithWriteTransactionExecutor(driver, true),
				stress.WriteQueryWithWriteTransactionExecutor(driver, false),
			)

			failingQueryExecutors = append(failingQueryExecutors,
				stress.WriteQueryInReadSessionExecutor(driver, true),
				stress.WriteQueryInReadSessionExecutor(driver, false),
				stress.WriteQueryInTxInReadSessionExecutor(driver, true),
				stress.WriteQueryInTxInReadSessionExecutor(driver, false),
				stress.FailingQueryExecutor(driver, true),
				stress.FailingQueryExecutor(driver, false),
				stress.FailingQueryInTxExecutor(driver, true),
				stress.FailingQueryInTxExecutor(driver, false),
				stress.FailingQueryWithReadTransactionExecutor(driver, true),
				stress.FailingQueryWithReadTransactionExecutor(driver, false),
				stress.FailingQueryWithWriteTransactionExecutor(driver, true),
				stress.FailingQueryWithWriteTransactionExecutor(driver, false),
				stress.WrongQueryExecutor(driver),
				stress.WrongQueryInTxExecutor(driver),
			)
		})

		AfterEach(func() {
			ctx.PrintStats()
		})

		It("should complete without any errors", func() {
			stessTest(ctx, successfulQueryExecutors, failingQueryExecutors)
		})
	})

})
