/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package db

import (
	"reflect"
	"testing"
)

func TestErrorReclassification(outer *testing.T) {
	outer.Parallel()

	type testCase struct {
		input  Neo4jError
		output Neo4jError
	}

	testCases := []testCase{
		{
			input: Neo4jError{Code: "Neo.TransientError.Transaction.LockClientStopped"},
			output: Neo4jError{
				Code:           "Neo.ClientError.Transaction.LockClientStopped",
				classification: "ClientError",
				category:       "Transaction",
				title:          "LockClientStopped",
				parsed:         true,
			},
		},
		{
			input: Neo4jError{Code: "Neo.TransientError.Transaction.Terminated"},
			output: Neo4jError{
				Code:           "Neo.ClientError.Transaction.Terminated",
				classification: "ClientError",
				category:       "Transaction",
				title:          "Terminated",
				parsed:         true,
			},
		},
		{
			input: Neo4jError{Code: "Neo.Completely.Made.Up"},
			output: Neo4jError{
				Code:           "Neo.Completely.Made.Up",
				classification: "Completely",
				category:       "Made",
				title:          "Up",
				parsed:         true,
			},
		},
	}

	for _, test := range testCases {
		outer.Run(test.input.Code, func(t *testing.T) {
			test.input.parse()

			if !reflect.DeepEqual(test.input, test.output) {
				t.Errorf("%v does not equal %v", test.input, test.output)
			}
		})
	}
}
