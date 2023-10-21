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

package main

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func cleanDb(ctx context.Context, driver neo4j.DriverWithContext) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	batch := 1000
	for {
		x, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, "MATCH (n) WITH n LIMIT $batch DETACH DELETE n RETURN count(n)",
				map[string]any{"batch": batch})
			if err != nil {
				return nil, err
			}
			record, err := result.Single(ctx)
			if err != nil {
				return nil, err
			}
			return record.Values[0], nil
		})
		if err != nil {
			panic(err)
		}
		count := x.(int64)
		if count < int64(batch) {
			return
		}
	}
}
