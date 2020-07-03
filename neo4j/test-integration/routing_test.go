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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test_integration

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/control"
)

var _ = Describe("Routing", func() {
	var cluster *control.Cluster

	var session neo4j.Session
	var result neo4j.Result
	var summary neo4j.ResultSummary
	var err error

	BeforeEach(func() {
		cluster, err = control.EnsureCluster()
		Expect(err).To(BeNil())
		Expect(cluster).NotTo(BeNil())
	})

	getDriver := func(address string) neo4j.Driver {
		driver, err := neo4j.NewDriver(address, cluster.AuthToken(), cluster.Config())
		if err != nil {
			panic(err.Error())
		}
		return driver
	}

	Specify("should successfully execute read/write when initial address contains unusable items", func() {
		// Rely on address resolving
		driver := getDriver("bolt+routing://localhost")
		Expect(err).To(BeNil())
		defer driver.Close()

		session, err = driver.Session(neo4j.AccessModeRead)
		Expect(err).To(BeNil())
		defer session.Close()

		result, err = session.Run("RETURN 1", nil)
		Expect(err).To(BeNil())

		summary, err = result.Consume()
		Expect(err).To(BeNil())
		Expect(summary).NotTo(BeNil())
	})

	Specify("writes should be visible on followers", func() {
		var readCount, writeCount interface{}

		driver := getDriver(cluster.RoutingURI)
		Expect(err).To(BeNil())

		session, err = driver.Session(neo4j.AccessModeWrite)
		Expect(err).To(BeNil())

		writeCount, err = session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			writeResult, err := tx.Run("MERGE (n:Person {name: 'John'}) RETURN 1", nil)
			if err != nil {
				return nil, err
			}

			if writeResult.Next() {
				return writeResult.Record().GetByIndex(0), nil
			}

			if err := writeResult.Err(); err != nil {
				return nil, err
			}

			return 0, nil
		})
		Expect(err).To(BeNil())
		Expect(writeCount).To(BeNumerically("==", 1))

		readCount, err = session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			readResult, err := tx.Run("MATCH (n:Person {name: 'John'}) RETURN COUNT(*) AS count", nil)
			if err != nil {
				return nil, err
			}

			if readResult.Next() {
				return readResult.Record().GetByIndex(0), nil
			}

			if err := readResult.Err(); err != nil {
				return nil, err
			}

			return 0, nil
		})
		Expect(err).To(BeNil())
		Expect(readCount).To(BeNumerically("==", 1))
	})
})
