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

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
)

// ExampleUUID demonstrates how to use UUID with GetRecordValue and GetProperty.
func ExampleUUID() {
	driver, err := neo4j.NewDriver(getUrl(), neo4j.BasicAuth("neo4j", "password", ""))
	if err != nil {
		panic(err)
	}
	defer driver.Close(context.Background())

	ctx := context.Background()
	uid, err := dbtype.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		panic(err)
	}

	// Create a node with a UUID property
	result, err := neo4j.ExecuteQuery(ctx, driver,
		"CREATE (n:UUIDExample {uid: $uid}) RETURN n, n.uid AS uid",
		map[string]any{"uid": uid},
		neo4j.EagerResultTransformer)
	if err != nil {
		panic(err)
	}

	record := result.Records[0]

	// Direct map access with explicit type assertion
	rawRecordUid := record.AsMap()["uid"].(dbtype.UUID)

	// Typed access with GetRecordValue for clearer errors
	recordUid, _, err := neo4j.GetRecordValue[dbtype.UUID](record, "uid")
	if err != nil {
		panic(err)
	}

	// Direct property map access with explicit type assertion
	node := record.Values[0].(neo4j.Node)
	rawPropUid := node.GetProperties()["uid"].(dbtype.UUID)

	// Typed access with GetProperty for clearer errors
	propUid, err := neo4j.GetProperty[dbtype.UUID](node, "uid")
	if err != nil {
		panic(err)
	}

	fmt.Printf("record raw=%v, record typed=%v, node raw=%v, node typed=%v\n",
		rawRecordUid, recordUid, rawPropUid, propUid)
}
