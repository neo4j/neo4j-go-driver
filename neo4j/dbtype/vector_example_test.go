/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

package dbtype_test

import (
	"context"
	"fmt"
	"os"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// ExampleVector demonstrates how to use Vector with the Neo4j Go driver.
func ExampleVector() {
	// Create a float64 vector
	vec := dbtype.Vector[float64]{1.0, 2.0, 3.0}

	// Connect to Neo4j
	driver, err := neo4j.NewDriverWithContext(getUrl(), neo4j.BasicAuth("neo4j", "password", ""))
	if err != nil {
		panic(err)
	}
	defer driver.Close(context.Background())

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{})
	defer session.Close(context.Background())

	// Write the vector to the database
	_, err = session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(context.Background(),
			"CREATE (n:VectorExample {vec: $vec}) RETURN n", map[string]any{"vec": vec})
		return nil, err
	})
	if err != nil {
		panic(err)
	}

	// Read the vector back from the database
	result, err := session.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (any, error) {
		records, err := tx.Run(context.Background(),
			"MATCH (n:VectorExample) RETURN n.vec LIMIT 1", nil)
		if err != nil {
			return nil, err
		}
		if records.Next(context.Background()) {
			if v, ok := records.Record().Values[0].(dbtype.Vector[float64]); ok {
				return v, nil
			}
		}
		return nil, nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Read vector: %v\n", result)
}

func getUrl() string {
	return fmt.Sprintf("%s://%s:%s", os.Getenv("TEST_NEO4J_SCHEME"), os.Getenv("TEST_NEO4J_HOST"), os.Getenv("TEST_NEO4J_PORT"))
}
