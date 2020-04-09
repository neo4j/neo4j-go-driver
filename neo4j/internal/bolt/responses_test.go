package bolt

import (
	"reflect"
	"testing"

	conn "github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
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
			expected: &conn.Summary{Bookmark: "bookm", TLast: 3, StmntType: conn.StatementTypeWrite},
			extract:  func(r *successResponse) interface{} { return r.summary() },
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
