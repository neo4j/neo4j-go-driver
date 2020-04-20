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
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
)

func TestSuccessResponseExtraction(ot *testing.T) {
	cases := []struct {
		name     string
		meta     map[string]interface{}
		extract  func(r *successResponse) interface{}
		expected interface{}
	}{
		{
			name: "Hello",
			meta: map[string]interface{}{
				"server":        "Neo4j/3.5.0",
				"connection_id": "x1",
			},
			expected: &helloSuccess{connectionId: "x1", credentialsExpired: false, server: "Neo4j/3.5.0"},
			extract:  func(r *successResponse) interface{} { return r.hello() },
		},
		{
			name: "Hello credentials expired",
			meta: map[string]interface{}{
				"server":              "Neo4j/3.5.0",
				"connection_id":       "x1",
				"credentials_expired": true,
			},
			expected: &helloSuccess{connectionId: "x1", credentialsExpired: true, server: "Neo4j/3.5.0"},
			extract:  func(r *successResponse) interface{} { return r.hello() },
		},
		{
			name: "Run",
			meta: map[string]interface{}{
				"fields":  []interface{}{"f1", "f2"},
				"t_first": int64(3),
			},
			expected: &runSuccess{fields: []string{"f1", "f2"}, t_first: 3},
			extract:  func(r *successResponse) interface{} { return r.run() },
		},
		{
			name: "Summary",
			meta: map[string]interface{}{
				"type":     "w",
				"t_last":   int64(3),
				"bookmark": "bookm",
			},
			expected: &db.Summary{Bookmark: "bookm", TLast: 3, StmntType: db.StatementTypeWrite},
			extract:  func(r *successResponse) interface{} { return r.summary() },
		},
		{
			name: "Commit",
			meta: map[string]interface{}{
				"bookmark": "neo4j:bookmark:v1:tx35",
			},
			expected: &commitSuccess{bookmark: "neo4j:bookmark:v1:tx35"},
			extract:  func(r *successResponse) interface{} { return r.commit() },
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			// Construct success response and perform the extraction from the success response
			succ := successResponse{meta: c.meta}
			extr := c.extract(&succ)
			// Compare the extracted data with the expectation
			if !reflect.DeepEqual(extr, c.expected) {
				t.Errorf("Extracted differs: %+v vs %+v", c.expected, extr)
			}
		})
	}
}
