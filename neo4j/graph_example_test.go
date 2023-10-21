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
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j"
)

func ExampleGetProperty() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer handleClose(ctx, session)

	result, err := session.Run(ctx, "MATCH (p:Person) RETURN p LIMIT 1", nil)
	handleError(err)
	personNode, err := getPersonNode(ctx, result)
	handleError(err)

	// GetProperty extracts the node's or relationship's property by the specified name
	// it also makes sure it matches the specified type parameter
	name, err := neo4j.GetProperty[string](personNode, "name")
	handleError(err)
	login, err := neo4j.GetProperty[string](personNode, "login")
	person := Person{
		Name:  name,
		Login: login,
	}
	fmt.Println(person)
}

func getPersonNode(ctx context.Context, result neo4j.ResultWithContext) (neo4j.Node, error) {
	record, err := result.Single(ctx)
	handleError(err)

	rawPersonNode, found := record.Get("p")
	if !found {
		return neo4j.Node{}, fmt.Errorf("person record not found")
	}
	personNode, ok := rawPersonNode.(neo4j.Node)
	if !ok {
		return neo4j.Node{}, fmt.Errorf("expected person record to be a node, but was %T", rawPersonNode)
	}
	return personNode, nil
}
