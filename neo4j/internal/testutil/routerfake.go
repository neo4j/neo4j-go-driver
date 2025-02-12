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

package testutil

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type RouterFake struct {
	Invalidated            bool
	InvalidatedDb          string
	InvalidateMode         string
	InvalidatedServer      string
	GetOrUpdateReadersRet  []string
	GetOrUpdateReadersHook func(bookmarks func(context.Context) ([]string, error), database string) ([]string, error)
	GetOrUpdateWritersRet  []string
	GetOrUpdateWritersHook func(bookmarks func(context.Context) ([]string, error), database string) ([]string, error)
	Err                    error
	CleanUpHook            func()
	GetNameOfDefaultDbHook func(user string) (string, error)
	GetTableHook           func(database string) *db.RoutingTable
}

func (r *RouterFake) InvalidateReader(database string, server string) {
	r.Invalidate(database)
	r.InvalidatedServer = server
	r.InvalidateMode = "reader"
}

func (r *RouterFake) InvalidateWriter(database string, server string) {
	r.Invalidate(database)
	r.InvalidatedServer = server
	r.InvalidateMode = "writer"
}

func (r *RouterFake) InvalidateServer(server string) {
	r.Invalidated = true
	r.InvalidatedServer = server
}

func (r *RouterFake) Invalidate(database string) {
	r.InvalidatedDb = database
	r.Invalidated = true
}

func (r *RouterFake) GetOrUpdateReaders(
	_ context.Context,
	bookmarksFn func(context.Context) ([]string, error),
	dbSelection db.DatabaseSelection,
	_ *db.ReAuthToken,
	_ log.BoltLogger,
	_ func(string),
) ([]string, error) {
	if r.GetOrUpdateReadersHook != nil {
		return r.GetOrUpdateReadersHook(bookmarksFn, dbSelection.Name)
	}
	return r.GetOrUpdateReadersRet, r.Err
}

func (r *RouterFake) Readers(string) []string {
	return nil
}

func (r *RouterFake) GetOrUpdateWriters(
	_ context.Context,
	bookmarksFn func(context.Context) ([]string, error),
	dbSelection db.DatabaseSelection,
	_ *db.ReAuthToken,
	_ log.BoltLogger,
	_ func(string),
) ([]string, error) {
	if r.GetOrUpdateWritersHook != nil {
		return r.GetOrUpdateWritersHook(bookmarksFn, dbSelection.Name)
	}
	return r.GetOrUpdateWritersRet, r.Err
}

func (r *RouterFake) Writers(string) []string {
	return nil
}

func (r *RouterFake) GetNameOfDefaultDatabase(_ context.Context, _ []string, user string, _ *db.ReAuthToken, _ log.BoltLogger) (string, error) {
	if r.GetNameOfDefaultDbHook != nil {
		return r.GetNameOfDefaultDbHook(user)
	}
	return "", nil
}

func (r *RouterFake) CleanUp() {
	if r.CleanUpHook != nil {
		r.CleanUpHook()
	}
}
