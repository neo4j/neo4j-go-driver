/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

package integration_tests

import (
	"fmt"
	"os"

	. "github.com/neo4j/neo4j-go-driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	singleInstanceUri string = "bolt://localhost:7687"
	username          string = "neo4j"
	password          string = "password"
)

func init() {
	boltPort := os.Getenv("BOLT_PORT")
	boltUsername := os.Getenv("BOLT_USERNAME")
	boltPassword := os.Getenv("BOLT_PASSWORD")

	if boltPort != "" {
		singleInstanceUri = fmt.Sprintf("bolt://localhost:%s", boltPort)
	}

	if boltUsername != "" {
		username = boltUsername
	}

	if boltPassword != "" {
		password = boltPassword
	}
}

var _ = BeforeSuite(func() {
	driver, err := NewDriver(singleInstanceUri, BasicAuth(username, password, ""))
	Expect(err).To(BeNil())

	session, err := driver.Session(AccessModeRead)
	Expect(err).To(BeNil())

	result, err := session.Run("MATCH (n) DETACH DELETE n", nil)
	Expect(err).To(BeNil())

	_, err = result.Consume()
	Expect(err).To(BeNil())
})
