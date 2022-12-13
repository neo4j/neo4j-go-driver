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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"os"
)

type Person struct {
	Name  string
	Login string
}

func (p *Person) String() string {
	return fmt.Sprintf("name: %s, login: %s", p.Name, p.Login)
}

func (p *Person) asNodeProps() map[string]any {
	return map[string]any{
		"name":  p.Name,
		"login": p.Login,
	}
}

func ExampleExecuteWrite() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer handleClose(ctx, session)

	person := readPersonFromRequest()

	// with neo4j.ExecuteWrite, `count` is guaranteed to be of the declared type if no error occurs
	// note: if an error occurs, the default value (sometimes called "zero value") of the type is returned
	count, err := neo4j.ExecuteWrite[int64](ctx, session, func(tx neo4j.ManagedTransaction) (int64, error) {
		if err := insertPerson(ctx, tx, person); err != nil {
			return 0, err
		}
		result, err := tx.Run(ctx, "MATCH (p:Person) RETURN count(p) AS count", nil)
		if err != nil {
			return 0, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return 0, err
		}
		key := "count"
		rawCount, found := record.Get(key)
		if !found {
			return 0, fmt.Errorf("record %q not found", key)
		}
		count, ok := rawCount.(int64)
		if !ok {
			return 0, fmt.Errorf("expected result to be an int64 but was %T", rawCount)
		}
		return count, nil
	})
	handleError(err)
	fmt.Printf("There are %d people in the database\n", count)
}

func ExampleExecuteRead() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer handleClose(ctx, session)

	personLogin := readPersonLoginFromRequest()

	// with neo4j.ExecuteRead, `person` is guaranteed to be of the declared type if no error occurs
	// note: if an error occurs, the default value (sometimes called "zero value") of the type is returned
	person, err := neo4j.ExecuteRead[*Person](ctx, session, func(tx neo4j.ManagedTransaction) (*Person, error) {
		result, err := tx.Run(ctx, "MATCH (p:Person) WHERE p.login = $login RETURN p AS person", map[string]any{
			"login": personLogin,
		})
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, err
		}
		key := "person"
		rawPerson, found := record.Get(key)
		if !found {
			return nil, fmt.Errorf("record %q not found", key)
		}
		personNode, ok := rawPerson.(neo4j.Node)
		if !ok {
			return nil, fmt.Errorf("expected result to be a map but was %T", rawPerson)
		}
		name, err := getPropertyValue[string](personNode, "name")
		if err != nil {
			return nil, err
		}
		login, err := getPropertyValue[string](personNode, "login")
		if err != nil {
			return nil, err
		}
		return &Person{
			Name:  name,
			Login: login,
		}, nil
	})
	handleError(err)
	fmt.Printf("Found person %v in the database\n", person)
}

func getPropertyValue[T any](node neo4j.Node, key string) (T, error) {
	zeroValue := *new(T)
	rawValue, found := node.Props[key]
	if !found {
		return zeroValue, fmt.Errorf("could not find property named %q", key)
	}
	value, ok := rawValue.(T)
	if !ok {
		return zeroValue, fmt.Errorf("expected property to be of type %T but was %T", zeroValue, rawValue)
	}
	return value, nil
}

// readPersonLoginFromRequest returns a hardcoded login
// imagine instead an implementation reading the data from an HTTP payload for instance
func readPersonLoginFromRequest() string {
	return "fbiville"
}

// readPersonFromRequest returns a hardcoded user
// imagine instead an implementation reading the data from an HTTP payload for instance
func readPersonFromRequest() *Person {
	return &Person{
		Name:  "Jane Doe",
		Login: "@janedoe",
	}
}

func insertPerson(ctx context.Context, tx neo4j.ManagedTransaction, newPerson *Person) error {
	result, err := tx.Run(ctx, "CREATE (p:Person) SET p = $props", map[string]any{
		"props": newPerson.asNodeProps(),
	})
	if err != nil {
		return err
	}
	_, err = result.Consume(ctx)
	return err
}

func createDriver() (neo4j.DriverWithContext, error) {
	credentials := neo4j.BasicAuth(os.Getenv("USERNAME"), os.Getenv("PASSWORD"), "")
	return neo4j.NewDriverWithContext(os.Getenv("URL"), credentials)
}

type ctxCloser interface {
	Close(context.Context) error
}

func handleClose(ctx context.Context, closer ctxCloser) {
	handleError(closer.Close(ctx))
}

func handleError(err error) {
	//lint:ignore SA9003 empty branch allowed since it's demo code
	if err != nil {
		// do something
	}
}
