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
	"fmt"
	"reflect"
	"testing"
)

func TestNodeHydration(ot *testing.T) {
	cases := []struct {
		name          string
		fields        []interface{}
		fieldErrors   []error
		completeError error
		node          *node
	}{
		{
			name: "happy",
			fields: []interface{}{
				int64(1),
				[]interface{}{"Person"},
				map[string]interface{}{"age": 29},
			},
			node: &node{
				id:    1,
				tags:  []string{"Person"},
				props: map[string]interface{}{"age": 29},
			},
		},
		{
			name:          "no fields",
			completeError: errors.New("something"),
		},
		{
			name:        "id wrong type",
			fields:      []interface{}{"1"},
			fieldErrors: []error{errors.New("1")},
		},
		{
			name:        "labels wrong type",
			fields:      []interface{}{int64(1), "Person"},
			fieldErrors: []error{nil, errors.New("Label")},
		},
		{
			name:        "props wrong type",
			fields:      []interface{}{int64(1), []interface{}{"Person"}, "props"},
			fieldErrors: []error{nil, nil, errors.New("Label")},
		},
	}

	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			n := &node{}
			var err error
			for i, f := range c.fields {
				// Hydrate field
				err = n.HydrateField(f)
				// Check against expected error
				var expectErr error
				if i < len(c.fieldErrors) {
					expectErr = c.fieldErrors[i]
				}
				// Compare existance of error, not actual error (yet)
				if (err == nil) != (expectErr == nil) {
					t.Fatalf("Failed error expectation on field %d, got '%s' but expected '%s'", i, err, expectErr)
				}
			}

			if err != nil {
				return
			}

			// Mark hydration as complete
			err = n.HydrationComplete()
			if (err == nil) != (c.completeError == nil) {
				t.Fatalf("Failed error expectation on completion, got %s but expected %s", err, c.completeError)
			}

			fmt.Printf("The err: %+v", err)
			if err == nil {
				// Compare nodes manually to avoid comparing internal state with reflect.DeepEqual
				ok := c.node.id == n.id && reflect.DeepEqual(c.node.tags, n.tags) && reflect.DeepEqual(c.node.props, n.props)
				if !ok {
					t.Errorf("Nodes differ")
				}
			}
		})
	}
}
