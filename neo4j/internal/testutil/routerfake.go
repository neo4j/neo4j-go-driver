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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package testutil

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

type RouterFake struct {
	Invalidated            bool
	InvalidatedDb          string
	ReadersRet             []string
	ReadersHook            func(bookmarks []string, database string) ([]string, error)
	WritersRet             []string
	WritersHook            func(bookmarks []string, database string) ([]string, error)
	Err                    error
	CleanUpHook            func()
	GetNameOfDefaultDbHook func(user string) (string, error)
}

func (r *RouterFake) Invalidate(database string) {
	r.InvalidatedDb = database
	r.Invalidated = true
}

func (r *RouterFake) Readers(ctx context.Context, bookmarks []string, database string, log log.BoltLogger) ([]string, error) {
	if r.ReadersHook != nil {
		return r.ReadersHook(bookmarks, database)
	}
	return r.ReadersRet, r.Err
}

func (r *RouterFake) Writers(ctx context.Context, bookmarks []string, database string, log log.BoltLogger) ([]string, error) {
	if r.WritersHook != nil {
		return r.WritersHook(bookmarks, database)
	}
	return r.WritersRet, r.Err
}

func (r *RouterFake) GetNameOfDefaultDatabase(ctx context.Context, bookmarks []string, user string, boltLogger log.BoltLogger) (string, error) {
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
