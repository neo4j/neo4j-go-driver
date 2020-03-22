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

package bolt

import (
	"errors"
)

// Success response from server
type successResponse struct {
	m map[string]interface{}

	// RUN with autocommit
	//   map[string] interface{}:
	//     "fields":  []string -- Names of returned fields
	//     "t_first": int64
	//
	// PULL ALL no (more) matching records
	//   map[string] interface{}
	//     "bookmark": string
	//     "t_last":   int64
	//     "type":     string  "r"
	//
	//fields []interface{}
}

// Invoked by packstream
func (r *successResponse) HydrateField(field interface{}) error {
	if r.m != nil {
		return errors.New("Didn't expect more fields")
	}

	m, ok := field.(map[string]interface{})
	if !ok {
		return errors.New("Unexpected type")
	}
	r.m = m
	return nil
}

// Invoked by packstream
func (r *successResponse) HydrationComplete() error {
	if r.m == nil {
		return errors.New("No map")
	}
	return nil
}

func (r *successResponse) Map() map[string]interface{} {
	if r.m == nil {
		return map[string]interface{}{}
	}
	return r.m
}
