package bolt

import (
	"reflect"
	"testing"
)

func TestSuccessResponseExtraction(ot *testing.T) {
	cases := []struct {
		name     string
		meta     map[string]interface{}
		extract  func(r *successResponse) interface{}
		expected interface{}
	}{
		{
			name: "Run",
			meta: map[string]interface{}{
				"fields":  []interface{}{"f1", "f2"},
				"t_first": int64(3),
			},
			expected: &runSuccess{fields: []string{"f1", "f2"}, t_first: 3},
			extract:  func(r *successResponse) interface{} { return r.run() },
		},
		/*
			{ // When no RETURN statement in cypher
				name:     "Run empty",
				meta:     map[string]interface{}{},
				expected: &runSuccess{fields: []string{}, t_first: 0},
				extract:  func(r *successResponse) interface{} { return r.run() },
			},
		*/
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
