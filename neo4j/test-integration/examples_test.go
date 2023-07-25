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

package test_integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

//lint:file-ignore U1000 Ignore all unused code, it's included in the driver manual
func TestExamples(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()

	outer.Run("Single Instance", func(inner *testing.T) {
		var (
			uri      string
			username string
			password string
		)

		server := dbserver.GetDbServer(ctx)

		uri = server.BoltURI()
		username = server.Username
		password = server.Password

		inner.Run("Hello World", func(t *testing.T) {
			greeting, err := helloWorld(ctx, uri, username, password)

			assertNil(t, err)
			assertStringContains(t, greeting, "hello, world")
		})

		inner.Run("Driver Lifecycle", func(t *testing.T) {
			driver, err := createDriver(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)

			err = closeDriver(ctx, driver)
			assertNil(t, err)
		})

		inner.Run("Basic Authentication", func(t *testing.T) {
			driver, err := createDriverWithBasicAuth(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)

			err = driver.Close(ctx)
			assertNil(t, err)
		})

		inner.Run("Config - With Max Retry Time", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)

			err = driver.Close(ctx)
			assertNil(t, err)
		})

		inner.Run("Config - With Customized Connection Pool", func(t *testing.T) {
			driver, err := createDriverWithCustomizedConnectionPool(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)

			err = driver.Close(ctx)
			assertNil(t, err)
		})

		inner.Run("Config - With Connection Timeout", func(t *testing.T) {
			driver, err := createDriverWithConnectionTimeout(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)

			err = driver.Close(ctx)
			assertNil(t, err)
		})

		inner.Run("Session", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)
			initialCount, err := countNodes(ctx, driver, "Person", "name", "Tom")
			assertNil(t, err)

			err = addPersonInSession(ctx, driver, "Tom")
			assertNil(t, err)
			count, err := countNodes(ctx, driver, "Person", "name", "Tom")
			assertNil(t, err)
			assertEquals(t, count, initialCount+int64(1))
		})

		inner.Run("Autocommit Transaction", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)
			initialCount, err := countNodes(ctx, driver, "Person", "name", "Shanon")
			assertNil(t, err)

			err = addPersonInAutoCommitTx(ctx, driver, "Shanon")
			assertNil(t, err)
			count, err := countNodes(ctx, driver, "Person", "name", "Shanon")
			assertNil(t, err)
			assertEquals(t, count, initialCount+int64(1))
		})

		inner.Run("Pass Bookmarks", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)
			initialAliceCount, err := countNodes(ctx, driver, "Person", "name", "Alice")
			assertNil(t, err)
			initialBobCount, err := countNodes(ctx, driver, "Person", "name", "Bob")
			assertNil(t, err)
			initialLexCorpCount, err := countNodes(ctx, driver, "Company", "name", "LexCorp")
			assertNil(t, err)
			initialWayneEnterpriseCount, err := countNodes(ctx, driver, "Company", "name", "Wayne Enterprises")
			assertNil(t, err)

			err = addEmployAndMakeFriends(ctx, driver)
			assertNil(t, err)

			count, err := countNodes(ctx, driver, "Person", "name", "Alice")
			assertNil(t, err)
			assertEquals(t, count, initialAliceCount+int64(1))

			count, err = countNodes(ctx, driver, "Person", "name", "Bob")
			assertNil(t, err)
			assertEquals(t, count, initialBobCount+int64(1))

			count, err = countNodes(ctx, driver, "Company", "name", "LexCorp")
			assertNil(t, err)
			assertEquals(t, count, initialLexCorpCount+int64(1))

			count, err = countNodes(ctx, driver, "Company", "name", "Wayne Enterprises")
			assertNil(t, err)
			assertEquals(t, count, initialWayneEnterpriseCount+int64(1))
		})

		inner.Run("Read/Write Transaction", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)

			id, err := addPersonNode(ctx, driver, "Jason")
			assertNil(t, err)
			assertTrue(t, id >= 0)
		})

		inner.Run("Get People", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)

			id, err := addPersonNode(ctx, driver, "Annie")
			assertNil(t, err)
			assertTrue(t, id >= 0)

			id, err = addPersonNode(ctx, driver, "Joe")
			assertNil(t, err)
			assertTrue(t, id >= 0)

			people, err := getPeople(ctx, driver)
			assertNil(t, err)
			assertStringsHas(t, people, "Annie")
			assertStringsHas(t, people, "Joe")
		})

		inner.Run("Result Retain", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)

			id, err := addPersonNode(ctx, driver, "Carl")
			assertNil(t, err)
			assertTrue(t, id >= 0)

			id, err = addPersonNode(ctx, driver, "Thomas")
			assertNil(t, err)
			assertTrue(t, id >= 0)

			count, err := addPersonsAsEmployees(ctx, driver, "Acme")
			assertNil(t, err)
			assertTrue(t, count >= 2)
		})

		inner.Run("Point2D", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)

			// tag::geospatial-types-point2d[]
			// Creating a 2D point in Cartesian space
			cartesian := dbtype.Point2D{
				X:            2.5,
				Y:            -2,
				SpatialRefId: 7203,
			}

			// Creating a 2D point in WGS84 space
			wgs84 := dbtype.Point2D{
				X:            -1.5,
				Y:            1,
				SpatialRefId: 4326,
			}
			// end::geospatial-types-point2d[]

			session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			assertNotNil(t, session)
			defer session.Close(ctx)

			recordWithCartesian, err := echo(ctx, session, cartesian)
			assertNil(t, err)
			assertNotNil(t, recordWithCartesian)

			recordWithWgs84, err := echo(ctx, session, wgs84)
			assertNil(t, err)
			assertNotNil(t, recordWithWgs84)

			// tag::geospatial-types-point2d[]

			// Reading a Cartesian point from a record
			field, _ := recordWithCartesian.Get("fieldName")
			fieldCartesian, _ := field.(dbtype.Point2D)

			// Serializing
			_ = fieldCartesian.String() // Point{SpatialReferenceId=7203, X=2.500000, Y=-2.000000}

			// Acessing members
			print(fieldCartesian.X)            // 2.500000
			print(fieldCartesian.Y)            // -2.000000
			print(fieldCartesian.SpatialRefId) // 7203

			// Reading a WGS84 point from a record
			field, _ = recordWithWgs84.Get("fieldName")
			fieldWgs84 := field.(dbtype.Point2D)

			// Serializing
			_ = fieldWgs84.String() // Point{SpatialReferenceId=4326, X=-1.500000, Y=1.00000}

			// Acessing members
			print(fieldWgs84.X)            // -1.500000
			print(fieldWgs84.Y)            // 1.000000
			print(fieldWgs84.SpatialRefId) // 4326
			// end::geospatial-types-point2d[]

			assertEquals(t, fieldCartesian.String(), "Point{SpatialReferenceId=7203, X=2.500000, Y=-2.000000}")
			assertEquals(t, fieldCartesian.X, cartesian.X)
			assertEquals(t, fieldCartesian.Y, cartesian.Y)
			assertEquals(t, fieldCartesian.SpatialRefId, cartesian.SpatialRefId)

			assertEquals(t, fieldWgs84.String(), "Point{SpatialReferenceId=4326, X=-1.500000, Y=1.000000}")
			assertEquals(t, fieldWgs84.X, wgs84.X)
			assertEquals(t, fieldWgs84.Y, wgs84.Y)
			assertEquals(t, fieldWgs84.SpatialRefId, wgs84.SpatialRefId)
		})

		inner.Run("Point3D", func(t *testing.T) {
			driver, err := createDriverWithMaxRetryTime(uri, username, password)
			assertNil(t, err)
			assertNotNil(t, driver)
			defer driver.Close(ctx)

			// tag::geospatial-types-point3d[]
			// Creating a 3D point in Cartesian space
			cartesian := dbtype.Point3D{
				X:            2.5,
				Y:            -2,
				Z:            2,
				SpatialRefId: 9157,
			}

			// Creating a 3D point in WGS84 space
			wgs84 := dbtype.Point3D{
				X:            -1.5,
				Y:            1,
				Z:            3,
				SpatialRefId: 4979,
			}
			// end::geospatial-types-point3d[]

			session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			assertNotNil(t, session)
			defer session.Close(ctx)

			recordWithCartesian, err := echo(ctx, session, cartesian)
			assertNil(t, err)
			assertNotNil(t, recordWithCartesian)

			recordWithWgs84, err := echo(ctx, session, wgs84)
			assertNil(t, err)
			assertNotNil(t, recordWithWgs84)

			// tag::geospatial-types-point3d[]

			// Reading a Cartesian point from a record
			field, _ := recordWithCartesian.Get("fieldName")
			fieldCartesian := field.(dbtype.Point3D)

			// Serializing
			_ = fieldCartesian.String() // Point{SpatialReferenceId=9157, X=2.500000, Y=-2.000000, Z=2.000000}

			// Accessing members
			print(fieldCartesian.X)            // 2.500000
			print(fieldCartesian.Y)            // -2.000000
			print(fieldCartesian.Z)            // 2.000000
			print(fieldCartesian.SpatialRefId) // 7203

			// Reading a WGS84 point from a record
			field, _ = recordWithWgs84.Get("fieldName")
			fieldWgs84 := field.(dbtype.Point3D)

			// Serializing
			_ = fieldWgs84.String() // Point{SpatialReferenceId=4979, X=-1.500000, Y=1.00000, Z=3.000000}

			// Accessing members
			print(fieldWgs84.X)            // -1.500000
			print(fieldWgs84.Y)            // 1.000000
			print(fieldWgs84.Z)            // 3.000000
			print(fieldWgs84.SpatialRefId) // 4979
			// end::geospatial-types-point3d[]

			assertEquals(t, fieldCartesian.String(), "Point{SpatialReferenceId=9157, X=2.500000, Y=-2.000000, Z=2.000000}")
			assertEquals(t, fieldCartesian.X, cartesian.X)
			assertEquals(t, fieldCartesian.Y, cartesian.Y)
			assertEquals(t, fieldCartesian.Z, cartesian.Z)
			assertEquals(t, fieldCartesian.SpatialRefId, cartesian.SpatialRefId)

			assertEquals(t, fieldWgs84.String(), "Point{SpatialReferenceId=4979, X=-1.500000, Y=1.000000, Z=3.000000}")
			assertEquals(t, fieldWgs84.X, wgs84.X)
			assertEquals(t, fieldWgs84.Y, wgs84.Y)
			assertEquals(t, fieldWgs84.Z, wgs84.Z)
			assertEquals(t, fieldWgs84.SpatialRefId, wgs84.SpatialRefId)
		})
	})
}

// tag::hello-world[]
func helloWorld(ctx context.Context, uri, username, password string) (string, error) {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	greeting, err := session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx,
			"CREATE (a:Greeting) SET a.message = $message RETURN a.message + ', from node ' + id(a)",
			map[string]any{"message": "hello, world"})
		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
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
func createDriver(uri, username, password string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
}

// call on application exit
func closeDriver(ctx context.Context, driver neo4j.DriverWithContext) error {
	return driver.Close(ctx)
}

// end::driver-lifecycle[]

// tag::basic-auth[]
func createDriverWithBasicAuth(uri, username, password string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
}

// end::basic-auth[]

// tag::kerberos-auth[]
func createDriverWithKerberosAuth(uri, ticket string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.KerberosAuth(ticket))
}

// end::kerberos-auth[]

// tag::bearer-auth[]
func createDriverWithBearerAuth(uri, token string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.BearerAuth(token))
}

// end::bearer-auth[]

// tag::custom-auth[]
func createDriverWithCustomAuth(uri, principal, credentials, realm, scheme string, parameters map[string]any) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.CustomAuth(scheme, principal, credentials, realm, parameters))
}

// end::custom-auth[]

// tag::config-unencrypted[]
// end::config-unencrypted[]

// tag::config-trust[]
// end::config-trust[]

// tag::config-custom-resolver[]
func createDriverWithAddressResolver(virtualURI, username, password string, addresses ...config.ServerAddress) (neo4j.DriverWithContext, error) {
	// Address resolver is only valid for neo4j uri
	return neo4j.NewDriverWithContext(virtualURI, neo4j.BasicAuth(username, password, ""), func(settings *config.Config) {
		settings.AddressResolver = func(address config.ServerAddress) []config.ServerAddress {
			return addresses
		}
	})
}

func addPerson(ctx context.Context, name string) error {
	const (
		username = "neo4j"
		password = "some password"
	)

	driver, err := createDriverWithAddressResolver("neo4j://x.acme.com", username, password,
		neo4j.NewServerAddress("a.acme.com", "7676"),
		neo4j.NewServerAddress("b.acme.com", "8787"),
		neo4j.NewServerAddress("c.acme.com", "9898"))
	if err != nil {
		return err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CREATE (n:Person { name: $name})", map[string]any{"name": name})
	if err != nil {
		return err
	}

	_, err = result.Consume(ctx)
	if err != nil {
		return err
	}

	return nil
}

// end::config-custom-resolver[]

// tag::config-connection-pool[]
func createDriverWithCustomizedConnectionPool(uri, username, password string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""), func(config *config.Config) {
		config.MaxConnectionLifetime = 30 * time.Minute
		config.MaxConnectionPoolSize = 50
		config.ConnectionAcquisitionTimeout = 2 * time.Minute
	})
}

// end::config-connection-pool[]

// tag::config-connection-timeout[]
func createDriverWithConnectionTimeout(uri, username, password string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""), func(config *config.Config) {
		config.SocketConnectTimeout = 15 * time.Second
	})
}

// end::config-connection-timeout[]

// tag::config-max-retry-time[]
// This driver is used to run queries, needs actual TLS configuration as well.
func createDriverWithMaxRetryTime(uri, username, password string) (neo4j.DriverWithContext, error) {
	return neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""), func(config *config.Config) {
		config.MaxTransactionRetryTime = 15 * time.Second
	})
}

// end::config-max-retry-time[]

// tag::service-unavailable[]
func createItem(ctx context.Context, driver neo4j.DriverWithContext) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "CREATE (a:Item)", nil)
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	})

	return err
}

// end::service-unavailable[]

func countNodes(ctx context.Context, driver neo4j.DriverWithContext, label string, property string, value string) (int64, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, fmt.Sprintf("MATCH (a:%s {%s: $value}) RETURN count(a)", label, property), map[string]any{"value": value})
	if err != nil {
		return -1, err
	}

	countRecord, err := result.Single(ctx)
	if err != nil {
		return -1, err
	}
	return countRecord.Values[0].(int64), nil
}

// tag::session[]
func addPersonInSession(ctx context.Context, driver neo4j.DriverWithContext, name string) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})
	if err != nil {
		return err
	}

	if _, err = result.Consume(ctx); err != nil {
		return err
	}

	return nil
}

// end::session[]

// tag::autocommit-transaction[]
func addPersonInAutoCommitTx(ctx context.Context, driver neo4j.DriverWithContext, name string) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})
	if err != nil {
		return err
	}

	if _, err = result.Consume(ctx); err != nil {
		return err
	}

	return nil
}

// end::autocommit-transaction[]

// tag::transaction-function[]
func addPersonInTxFunc(ctx context.Context, driver neo4j.DriverWithContext, name string) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	})

	return err
}

// end::transaction-function[]

// tag::transaction-timeout-config[]
func configTxTimeout(ctx context.Context, driver neo4j.DriverWithContext, name string) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}, neo4j.WithTxTimeout(5*time.Second))

	return err
}

// end::transaction-timeout-config[]

// tag::transaction-metadata-config[]
func configTxMetadata(ctx context.Context, driver neo4j.DriverWithContext, name string) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}, neo4j.WithTxMetadata(map[string]any{"applicationId": 123}))

	return err
}

// end::transaction-metadata-config[]

// tag::pass-bookmarks[]
func addCompanyTxFunc(ctx context.Context, name string) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		var result, err = tx.Run(ctx, "CREATE (a:Company {name: $name})", map[string]any{"name": name})

		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}
}

func addPersonTxFunc(ctx context.Context, name string) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		var result, err = tx.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})

		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}
}

func employTxFunc(ctx context.Context, person string, company string) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		var result, err = tx.Run(ctx,
			"MATCH (person:Person {name: $personName}) "+
				"MATCH (company:Company {name: $companyName}) "+
				"CREATE (person)-[:WORKS_FOR]->(company)", map[string]any{"personName": person, "companyName": company})

		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}
}

func makeFriendTxFunc(ctx context.Context, person1 string, person2 string) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		var result, err = tx.Run(ctx,
			"MATCH (a:Person {name: $name1}) "+
				"MATCH (b:Person {name: $name2}) "+
				"MERGE (a)-[:KNOWS]->(b)", map[string]any{"name1": person1, "name2": person2})

		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}
}

func printFriendsTxFunc(ctx context.Context) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name", nil)
		if err != nil {
			return nil, err
		}

		for result.Next(ctx) {
			fmt.Printf("%s knows %s\n", result.Record().Values[0], result.Record().Values[1])
		}

		return result.Consume(ctx)
	}
}

func addAndEmploy(ctx context.Context, driver neo4j.DriverWithContext, person string, company string) (neo4j.Bookmarks, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	if _, err := session.ExecuteWrite(ctx, addCompanyTxFunc(ctx, company)); err != nil {
		return nil, err
	}
	if _, err := session.ExecuteWrite(ctx, addPersonTxFunc(ctx, person)); err != nil {
		return nil, err
	}
	if _, err := session.ExecuteWrite(ctx, employTxFunc(ctx, person, company)); err != nil {
		return nil, err
	}

	return session.LastBookmarks(), nil
}

func makeFriend(ctx context.Context, driver neo4j.DriverWithContext, person1 string, person2 string, bookmarks neo4j.Bookmarks) (neo4j.Bookmarks, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite, Bookmarks: bookmarks})
	defer session.Close(ctx)

	if _, err := session.ExecuteWrite(ctx, makeFriendTxFunc(ctx, person1, person2)); err != nil {
		return nil, err
	}

	return session.LastBookmarks(), nil
}

func addEmployAndMakeFriends(ctx context.Context, driver neo4j.DriverWithContext) error {
	var bookmarks1, bookmarks2, bookmarks3 neo4j.Bookmarks
	var err error

	if bookmarks1, err = addAndEmploy(ctx, driver, "Alice", "Wayne Enterprises"); err != nil {
		return err
	}

	if bookmarks2, err = addAndEmploy(ctx, driver, "Bob", "LexCorp"); err != nil {
		return err
	}

	if bookmarks3, err = makeFriend(ctx, driver, "Bob", "Alice", neo4j.CombineBookmarks(bookmarks1, bookmarks2)); err != nil {
		return err
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
		Bookmarks:  neo4j.CombineBookmarks(bookmarks1, bookmarks2, bookmarks3),
	})
	defer session.Close(ctx)

	if _, err = session.ExecuteRead(ctx, printFriendsTxFunc(ctx)); err != nil {
		return err
	}

	return nil
}

// end::pass-bookmarks[]

// tag::read-write-transaction[]
func addPersonNodeTxFunc(ctx context.Context, name string) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "CREATE (a:Person {name: $name})", map[string]any{"name": name})
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	}
}

func matchPersonNodeTxFunc(ctx context.Context, name string) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "MATCH (a:Person {name: $name}) RETURN id(a)", map[string]any{"name": name})
		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}

		return nil, errors.New("one record was expected")
	}
}

func addPersonNode(ctx context.Context, driver neo4j.DriverWithContext, name string) (int64, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	if _, err := session.ExecuteWrite(ctx, addPersonNodeTxFunc(ctx, name)); err != nil {
		return -1, err
	}

	var id any
	var err error
	if id, err = session.ExecuteRead(ctx, matchPersonNodeTxFunc(ctx, name)); err != nil {
		return -1, err
	}

	return id.(int64), nil
}

// end::read-write-transaction[]

func TestExamplesDatabaseSelection(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	driver := dbserver.GetDbServer(ctx).Driver()
	defer driver.Close(ctx)
	// tag::database-selection[]
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "example"})
	// end::database-selection[]
	defer session.Close(ctx)
}

// tag::result-consume[]
func getPeople(ctx context.Context, driver neo4j.DriverWithContext) ([]string, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	people, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var list []string

		result, err := tx.Run(ctx, "MATCH (a:Person) RETURN a.name ORDER BY a.name", nil)
		if err != nil {
			return nil, err
		}

		for result.Next(ctx) {
			list = append(list, result.Record().Values[0].(string))
		}

		if err = result.Err(); err != nil {
			return nil, err
		}

		return list, nil
	})
	if err != nil {
		return nil, err
	}

	return people.([]string), nil
}

// end::result-consume[]

// tag::result-retain[]
func addPersonsAsEmployees(ctx context.Context, driver neo4j.DriverWithContext, companyName string) (int, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	results, err := session.Run(ctx, "MATCH (a:Person) RETURN a.name AS name", nil)
	persons, err := neo4j.CollectWithContext(ctx, results, err)
	if err != nil {
		return 0, err
	}

	employees := 0
	for _, person := range persons {
		_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			var result, err = tx.Run(ctx, "MATCH (emp:Person {name: $person_name}) "+
				"MERGE (com:Company {name: $company_name}) "+
				"MERGE (emp)-[:WORKS_FOR]->(com)", map[string]any{"person_name": person.Values[0], "company_name": companyName})
			if err != nil {
				return nil, err
			}

			return result.Consume(ctx)
		})
		if err != nil {
			return 0, err
		}

		employees++
	}

	return employees, nil
}

// end::result-retain[]

func echo(ctx context.Context, session neo4j.SessionWithContext, value any) (*neo4j.Record, error) {
	record, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "RETURN $value as fieldName", map[string]any{"value": value})

		if err != nil {
			return nil, err
		}

		return result.Single(ctx)
	})
	return record.(*neo4j.Record), err
}
