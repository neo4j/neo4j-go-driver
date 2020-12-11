/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

type RouterFake struct {
	Invalidated   bool
	InvalidatedDb string
	ReadersRet    []string
	WritersRet    []string
	Err           error
	CleanUpHook   func()
}

func (r *RouterFake) Invalidate(database string) {
	r.InvalidatedDb = database
	r.Invalidated = true
}

func (r *RouterFake) Readers(database string) ([]string, error) {
	return r.ReadersRet, r.Err
}

func (r *RouterFake) Writers(database string) ([]string, error) {
	return r.WritersRet, r.Err
}

func (r *RouterFake) CleanUp() {
	if r.CleanUpHook != nil {
		r.CleanUpHook()
	}
}
