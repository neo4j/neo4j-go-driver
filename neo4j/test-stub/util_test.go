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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test_stub

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"os"
	"strings"

	. "github.com/onsi/gomega"
)

func newDriver(uri string) neo4j.Driver {
	driver, err := neo4j.NewDriver(uri, neo4j.NoAuth(), func(config *neo4j.Config) {
		config.Encrypted = false
		config.Log = neo4j.ConsoleLogger(logLevel())
	})
	Expect(err).To(BeNil())

	return driver
}

func createSession(driver neo4j.Driver, bookmarks ...string) neo4j.Session {
	session, err := driver.Session(neo4j.AccessModeWrite, bookmarks...)
	Expect(err).To(BeNil())

	return session
}

func createTx(session neo4j.Session, configurers ...func(*neo4j.TransactionConfig)) neo4j.Transaction {
	tx, err := session.BeginTransaction(configurers...)
	Expect(err).To(BeNil())

	return tx
}

func logLevel() neo4j.LogLevel {
	if val, ok := os.LookupEnv("NEOLOGLEVEL"); ok {
		switch strings.ToLower(val) {
		case "error":
			return neo4j.ERROR
		case "warning":
			return neo4j.WARNING
		case "info":
			return neo4j.INFO
		case "debug":
			return neo4j.DEBUG
		}
	}

	return neo4j.ERROR
}