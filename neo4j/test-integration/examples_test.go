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

package test_integration

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Examples", func() {

	Context("Single Instance", func() {
		var (
			uri      string
			username string
			password string
		)

		BeforeEach(func() {
			var singleInstance *control.SingleInstance
			var err error

			if singleInstance, err = control.EnsureSingleInstance(); err != nil {
				Fail(err.Error())
			}

			uri = singleInstance.BoltUri()
			username = singleInstance.Username()
			password = singleInstance.Password()
		})

		Specify("Hello World", func() {
			greeting, err := helloWorld(uri, username, password)

			Expect(err).To(BeNil())
			Expect(greeting).To(ContainSubstring("hello, world"))
		})

		Specify("Driver Lifecycle", func() {
			driver, err := createDriver(uri, username, password)
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			err = closeDriver(driver)
			Expect(err).To(BeNil())
		})

		Specify("Basic Authentication", func() {
			driver, err := createDriverWithBasicAuth(uri, username, password)
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			err = driver.Close()
			Expect(err).To(BeNil())
		})

		Specify("Config - Without Encryption", func() {
			driver, err := createDriverWithoutEncryption(uri, username, password)
			Expect(err).To(BeNil())
			Expect(driver).NotTo(BeNil())

			err = driver.Close()
			Expect(err).To(BeNil())
		})
	})

})

// tag::hello-world[]
func helloWorld(uri, username, password string) (string, error) {
	var (
		err      error
		driver   neo4j.Driver
		session  neo4j.Session
		result   neo4j.Result
		greeting interface{}
	)

	driver, err = neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close()

	session, err = driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return "", err
	}
	defer session.Close()

	greeting, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err = transaction.Run(
			"CREATE (a:Greeting) SET a.message = $message RETURN a.message + ', from node ' + id(a)",
			map[string]interface{}{"message": "hello, world"})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return result.Record().GetByIndex(0), nil
		}

		return nil, result.Err()
	})
	if err != nil {
		return "", err
	}

	return greeting.(string), nil
}

// end::hello-world[]

// tag::driver-lifecycle[]
func createDriver(uri, username, password string) (neo4j.Driver, error) {
	return neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
}

// call on application exit
func closeDriver(driver neo4j.Driver) error {
	return driver.Close()
}

// end::driver-lifecycle[]

// tag::basic-auth[]
func createDriverWithBasicAuth(uri, username, password string) (neo4j.Driver, error) {
	return neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
}

// end::basic-auth[]

// tag::kerberos-auth[]
func createDriverWithKerberosAuth(uri, ticket string) (neo4j.Driver, error) {
	return neo4j.NewDriver(uri, neo4j.KerberosAuth(ticket))
}

// end::kerberos-auth[]

// tag::custom-auth[]
func createDriverWithCustomAuth(uri, principal, credentials, realm, scheme string, parameters map[string]interface{}) (neo4j.Driver, error) {
	return neo4j.NewDriver(uri, neo4j.CustomAuth(scheme, principal, credentials, realm, parameters))
}

// end::custom-auth[]

// tag::config-unencrypted[]
func createDriverWithoutEncryption(uri, username, password string) (neo4j.Driver, error) {
	return neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""), func(config *neo4j.Config) {
		config.Encrypted = false
	})
}

// end::config-unencrypted[]


// tag::config-trust[]

// end::config-trust[]

// tag::config-connection-pool[]

// end::config-connection-pool[]

// tag::config-connection-timeout[]

// end::config-connection-timeout[]

// tag::config-max-retry-time[]

// end::config-max-retry-time[]

// tag::service-unavailable[]

// end::service-unavailable[]

// tag::session[]

// end::session[]

// tag::autocommit-transaction[]

// end::autocommit-transaction[]

// tag::transaction-function[]

// end::transaction-function[]

// tag::pass-bookmarks[]

// end::pass-bookmarks[]

// tag::read-write-transaction[]

// end::read-write-transaction[]

// tag::result-consume[]

// end::result-consume[]

// tag::result-retain[]

// end::result-retain[]