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

func (r *directRouter) InvalidateWriter(string, string) {}

func (r *directRouter) InvalidateReader(string, string) {}

func (r *directRouter) InvalidateServer(string) {}

func (r *directRouter) GetOrUpdateReaders(context.Context, func(context.Context) ([]string, error), string, *db.ReAuthToken, log.BoltLogger) ([]string, error) {
	return []string{r.address}, nil
}

func (r *directRouter) Readers(string) []string {
	return []string{r.address}
}

func (r *directRouter) GetOrUpdateWriters(context.Context, func(context.Context) ([]string, error), string, *db.ReAuthToken, log.BoltLogger) ([]string, error) {
	return []string{r.address}, nil
}

func (r *directRouter) Writers(string) []string {
	return []string{r.address}
}

func (r *directRouter) GetNameOfDefaultDatabase(context.Context, []string, string, *db.ReAuthToken, log.BoltLogger) (string, error) {
	return db.DefaultDatabase, nil
}

func (r *directRouter) Invalidate(string) {}

func (r *directRouter) CleanUp() {}
