/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

// A router implementation that never routes
type directRouter struct {
	address string
}

func (r *directRouter) Readers(ctx context.Context, bookmarks []string, database string, boltLogger log.BoltLogger) ([]string, error) {
	return []string{r.address}, nil
}

func (r *directRouter) Writers(ctx context.Context, bookmarks []string, database string, boltLogger log.BoltLogger) ([]string, error) {
	return []string{r.address}, nil
}

func (r *directRouter) GetNameOfDefaultDatabase(ctx context.Context, bookmarks []string, user string, boltLogger log.BoltLogger) (string, error) {
	return db.DefaultDatabase, nil
}

func (r *directRouter) Invalidate(database string) {
}

func (r *directRouter) CleanUp() {
}
