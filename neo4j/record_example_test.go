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

package neo4j_test

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

func ExampleGetRecordValue() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, config.SessionConfig{})
	defer handleClose(ctx, session)

	result, err := session.Run(ctx, "MATCH (p:Person) RETURN p LIMIT 1", nil)
	handleError(err)
	record, err := result.Single(ctx)
	handleError(err)

	// GetRecordValue extracts the record value by the specified name
	// it also makes sure the value conforms to the specified type parameter
	// if a particular value is not present for the current record, isNil will be true
	personNode, isNil, err := neo4j.GetRecordValue[neo4j.Node](record, "p")
	handleError(err)
	if isNil {
		fmt.Println("no person found")
	} else {
		fmt.Println(personNode)
	}
}
