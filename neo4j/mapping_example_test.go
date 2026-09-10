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

package neo4j_test

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func ExampleAs() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer handleClose(ctx, session)

	type Movie struct {
		Title    string `neo4j:"title"`
		Released int64  `neo4j:"released"`
	}

	// The record holds a single node column, so As maps its properties onto the
	// struct fields named by the neo4j tags.
	movie, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (Movie, error) {
		result, err := tx.Run(ctx, "MATCH (m:Movie {title: $title}) RETURN m", map[string]any{"title": "The Matrix"})
		if err != nil {
			return Movie{}, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return Movie{}, err
		}
		return neo4j.As[Movie](record)
	})
	handleError(err)
	fmt.Println(movie.Title)
}

func ExampleCollectAs() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer handleClose(ctx, session)

	type Movie struct {
		Title    string `neo4j:"title"`
		Released int64  `neo4j:"released"`
	}

	movies, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) ([]Movie, error) {
		result, err := tx.Run(ctx, "MATCH (m:Movie) RETURN m", nil)
		if err != nil {
			return nil, err
		}
		return neo4j.CollectAs[Movie](ctx, result)
	})
	handleError(err)
	fmt.Printf("mapped %d movies\n", len(movies))
}

func ExampleSingleAs() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer handleClose(ctx, session)

	type Movie struct {
		Title    string `neo4j:"title"`
		Released int64  `neo4j:"released"`
	}

	movie, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (Movie, error) {
		result, err := tx.Run(ctx, "MATCH (m:Movie {title: $title}) RETURN m", map[string]any{"title": "The Matrix"})
		if err != nil {
			return Movie{}, err
		}
		return neo4j.SingleAs[Movie](ctx, result)
	})
	handleError(err)
	fmt.Println(movie.Title)
}

func ExampleCollectRecordsAs() {
	ctx := context.Background()
	driver, err := createDriver()
	handleError(err)
	defer handleClose(ctx, driver)

	type Movie struct {
		Title    string `neo4j:"title"`
		Released int64  `neo4j:"released"`
	}

	result, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (m:Movie) RETURN m", nil, neo4j.EagerResultTransformer)
	handleError(err)

	// ExecuteQuery returns eager records; CollectRecordsAs maps them in one call.
	movies, err := neo4j.CollectRecordsAs[Movie](result.Records)
	handleError(err)
	fmt.Printf("mapped %d movies\n", len(movies))
}
