/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

package dbtype_test

import (
	"context"
	"fmt"
	"os"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// ExampleVector demonstrates how to use Vector with GetRecordValue and GetProperty.
func ExampleVector() {
	driver, err := neo4j.NewDriver(getUrl(), neo4j.BasicAuth("neo4j", "password", ""))
	if err != nil {
		panic(err)
	}
	defer driver.Close(context.Background())

	ctx := context.Background()
	vec := neo4j.Vector[float64]{Elems: []float64{1.0, 2.0, 3.0}}

	// Create a node with a vector property
	result, err := neo4j.ExecuteQuery(ctx, driver,
		"CREATE (n:VectorExample {vec: $vec}) RETURN n, n.vec AS vec",
		map[string]any{"vec": vec},
		neo4j.EagerResultTransformer)
	if err != nil {
		panic(err)
	}

	record := result.Records[0]

	// Direct map access with explicit type assertion
	rawRecordVec := record.AsMap()["vec"].(neo4j.Vector[float64])

	// Typed access with GetRecordValue for clearer errors
	recordVec, _, err := neo4j.GetRecordValue[neo4j.Vector[float64]](record, "vec")
	if err != nil {
		panic(err)
	}

	// Direct property map access with explicit type assertion
	node := record.Values[0].(neo4j.Node)
	rawPropVec := node.GetProperties()["vec"].(neo4j.Vector[float64])

	// Typed access with GetProperty for clearer errors
	propVec, err := neo4j.GetProperty[neo4j.Vector[float64]](node, "vec")
	if err != nil {
		panic(err)
	}

	fmt.Printf("record raw=%v, record typed=%v, node raw=%v, node typed=%v\n",
		rawRecordVec, recordVec, rawPropVec, propVec)
}

func getUrl() string {
	return fmt.Sprintf("%s://%s:%s", os.Getenv("TEST_NEO4J_SCHEME"), os.Getenv("TEST_NEO4J_HOST"), os.Getenv("TEST_NEO4J_PORT"))
}
