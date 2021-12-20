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
	"context"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Timeout and Lifetime", func() {
	server := dbserver.GetDbServer()

	It("should error when timeout is hit", func() {
		var err error
		var driver neo4j.Driver
		var session1, session2 neo4j.Session

		driver, err = neo4j.NewDriver(server.BoltURI(), server.AuthToken(), server.ConfigFunc(), func(config *neo4j.Config) {
			config.MaxConnectionPoolSize = 1
		})
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())
		defer driver.Close()

		ctx, cancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancelFunc()
		session1, _ = newSessionAndTx(ctx, driver, neo4j.AccessModeRead)
		defer session1.Close()

		session2 = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
		Expect(err).To(BeNil())
		Expect(session2).NotTo(BeNil())
		defer session2.Close()

		_, err = session2.Run(context.TODO(), "RETURN 1", nil)
		Expect(err).ToNot(BeNil())
		//Expect(err).To(test.BeConnectorErrorWithCode(0x601))
	})

})
