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
package db

import (
	"fmt"
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

func TestGqlStatusFinders(outer *testing.T) {
	outer.Parallel()

	testCases := []struct {
		name      string
		error     *Neo4jError
		gqlStatus string
		contains  bool
	}{
		// Single error cases
		{"single error match", buildSingleError("01N00"), "01N00", true},
		{"single error no match", buildSingleError("01N00"), "99N99", false},
		{"nil error", nil, "01N00", false},
		{"empty gqlStatus", buildSingleError("01N00"), "", false},

		// Error chain cases
		{"chain of 1 error - find first", buildErrorChain(1), "error0", true},
		{"chain of 1 error - not found", buildErrorChain(1), "notfound", false},
		{"chain of 3 errors - find first", buildErrorChain(3), "error0", true},
		{"chain of 3 errors - find middle", buildErrorChain(3), "error1", true},
		{"chain of 3 errors - find last", buildErrorChain(3), "error2", true},
		{"chain of 3 errors - not found", buildErrorChain(3), "notfound", false},
		{"chain of 10 errors - find first", buildErrorChain(10), "error0", true},
		{"chain of 10 errors - find middle", buildErrorChain(10), "error5", true},
		{"chain of 10 errors - find last", buildErrorChain(10), "error9", true},
		{"chain of 10 errors - not found", buildErrorChain(10), "notfound", false},
	}

	for _, tc := range testCases {
		outer.Run(tc.name, func(t *testing.T) {
			// Test ContainsGqlStatus
			contains := tc.error.ContainsGqlStatus(tc.gqlStatus)
			if contains != tc.contains {
				t.Errorf("ContainsGqlStatus(%q) = %v, expected %v", tc.gqlStatus, contains, tc.contains)
			}

			// Test FindByGqlStatus
			found := tc.error.FindByGqlStatus(tc.gqlStatus)
			if tc.contains {
				if found == nil {
					t.Errorf("FindByGqlStatus(%q) = nil, expected non-nil", tc.gqlStatus)
				} else if found.GqlStatus != tc.gqlStatus {
					t.Errorf("FindByGqlStatus(%q) returned error with GqlStatus %q, expected %q", tc.gqlStatus, found.GqlStatus, tc.gqlStatus)
				}
			} else {
				if found != nil {
					t.Errorf("FindByGqlStatus(%q) = %v, expected nil", tc.gqlStatus, found)
				}
			}
		})
	}
}

func buildSingleError(gqlStatus string) *Neo4jError {
	return &Neo4jError{
		GqlStatus: gqlStatus,
	}
}

func buildErrorChain(length int) *Neo4jError {
	var current *Neo4jError

	// Build chain from last to first (error0 -> error1 -> error2)
	for i := length - 1; i >= 0; i-- {
		error := &Neo4jError{
			GqlStatus: fmt.Sprintf("error%d", i),
		}
		if current != nil {
			error.GqlCause = current
		}
		current = error
	}

	return current
}
