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

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
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
			expected: &runSuccess{fields: []string{"f1", "f2"}, t_first: 3, qid: -1},
			extract:  func(r *successResponse) interface{} { return r.run() },
		},
		{
			name: "Run with qid",
			meta: map[string]interface{}{
				"fields":  []interface{}{"f1", "f2"},
				"t_first": int64(3),
				"qid":     int64(7),
			},
			expected: &runSuccess{fields: []string{"f1", "f2"}, t_first: 3, qid: 7},
			extract:  func(r *successResponse) interface{} { return r.run() },
		},
		{
			name: "Has more",
			meta: map[string]interface{}{
				"has_more": true,
			},
			expected: true,
			extract:  func(r *successResponse) interface{} { return r.hasMore() },
		},
		{
			name: "Has more on summary",
			meta: map[string]interface{}{
				"type":     "w",
				"t_last":   int64(3),
				"bookmark": "bookm",
			},
			expected: false,
			extract:  func(r *successResponse) interface{} { return r.hasMore() },
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
			name: "Summary with plan",
			meta: map[string]interface{}{
				"type":     "w",
				"t_last":   int64(3),
				"bookmark": "bookm",
				"plan": map[string]interface{}{
					"operatorType": "opType",
					"identifiers":  []interface{}{"id1", "id2"},
					"args": map[string]interface{}{
						"arg1": 1001,
					},
					"children": []interface{}{
						map[string]interface{}{
							"operatorType": "cop",
							"identifiers":  []interface{}{"cid"},
						},
					},
				},
			},
			expected: &db.Plan{
				Operator:    "opType",
				Arguments:   map[string]interface{}{"arg1": 1001},
				Identifiers: []string{"id1", "id2"},
				Children: []db.Plan{
					db.Plan{Operator: "cop", Identifiers: []string{"cid"}, Children: []db.Plan{}},
				},
			},
			extract: func(r *successResponse) interface{} { return r.summary().Plan },
		},
		{
			name: "Summary with profile",
			meta: map[string]interface{}{
				"type":     "w",
				"t_last":   int64(3),
				"bookmark": "bookm",
				"profile": map[string]interface{}{
					"dbHits":       int64(7),
					"rows":         int64(4),
					"operatorType": "opType",
					"identifiers":  []interface{}{"id1", "id2"},
					"args": map[string]interface{}{
						"arg1": 1001,
					},
					"children": []interface{}{
						map[string]interface{}{
							"operatorType": "cop",
							"identifiers":  []interface{}{"cid"},
							"dbHits":       int64(1),
							"rows":         int64(2),
						},
					},
				},
			},
			expected: &db.ProfiledPlan{
				Operator:    "opType",
				Arguments:   map[string]interface{}{"arg1": 1001},
				Identifiers: []string{"id1", "id2"},
				Children: []db.ProfiledPlan{
					db.ProfiledPlan{Operator: "cop", Identifiers: []string{"cid"}, Children: []db.ProfiledPlan{}, DbHits: 1, Records: 2},
				},
				DbHits:  7,
				Records: 4,
			},
			extract: func(r *successResponse) interface{} { return r.summary().ProfiledPlan },
		},
		{
			name: "Summary with notifications",
			meta: map[string]interface{}{
				"type":     "w",
				"t_last":   int64(3),
				"bookmark": "bookm",
				"notifications": []interface{}{
					map[string]interface{}{
						"code":        "c1",
						"title":       "t1",
						"description": "d1",
						"position": map[string]interface{}{
							"offset": int64(1),
							"line":   int64(2),
							"column": int64(3),
						},
						"severity": "s1",
					},
					map[string]interface{}{
						"code":        "c2",
						"title":       "t2",
						"description": "d2",
						"severity":    "s2",
					},
				},
			},
			expected: []db.Notification{
				db.Notification{Code: "c1", Title: "t1", Description: "d1", Severity: "s1", Position: &db.InputPosition{Offset: 1, Line: 2, Column: 3}},
				db.Notification{Code: "c2", Title: "t2", Description: "d2", Severity: "s2"},
			},
			extract: func(r *successResponse) interface{} { return r.summary().Notifications },
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
				t.Errorf("Extracted differs: %#v vs %#v", c.expected, extr)
			}
		})
	}
}
