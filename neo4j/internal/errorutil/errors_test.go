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

package errorutil_test

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/gql"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func TestCombineErrors(outer *testing.T) {

	type testCase struct {
		description string
		input1      error
		input2      error
		output      error
	}

	err1 := fmt.Errorf("1")
	err2 := fmt.Errorf("2")

	testCases := []testCase{
		{
			description: "first non-nil",
			input1:      err1,
			input2:      nil,
			output:      err1,
		},
		{
			description: "second non-nil",
			input1:      nil,
			input2:      err2,
			output:      err2,
		},
		{
			description: "all nil",
			input1:      nil,
			input2:      nil,
			output:      nil,
		},
		{
			description: "all non-nil",
			input1:      err1,
			input2:      err2,
			output:      fmt.Errorf("error 2 occurred after previous error %w", err1),
		},
	}

	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			output := errorutil.CombineErrors(testCase.input1, testCase.input2)

			AssertDeepEquals(t, testCase.output, output)
		})
	}
}

func TestCombineAllErrors(outer *testing.T) {

	type testCase struct {
		description string
		input       []error
		output      error
	}

	err1 := fmt.Errorf("1")
	err2 := fmt.Errorf("2")
	err3 := fmt.Errorf("3")

	testCases := []testCase{
		{
			description: "nil slice",
			input:       nil,
			output:      nil,
		},
		{
			description: "empty slice - variant 1",
			input:       []error{},
			output:      nil,
		},
		{
			description: "empty slice - variant 2",
			input:       make([]error, 0),
			output:      nil,
		},
		{
			description: "slice with single non-nil element",
			input:       []error{err1},
			output:      err1,
		},
		{
			description: "slice with all three non-nil elements",
			input:       []error{err1, err2, err3},
			output:      errorutil.CombineErrors(errorutil.CombineErrors(err1, err2), err3),
		},
		{
			description: "slice with 1 nil element in the middle",
			input:       []error{err1, nil, err3},
			output:      errorutil.CombineErrors(err1, err3),
		},
	}

	outer.Parallel()
	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			output := errorutil.CombineAllErrors(testCase.input...)

			AssertDeepEquals(t, testCase.output, output)
		})
	}
}

func TestPolyfillGqlError(outer *testing.T) {

	type testCase struct {
		description string
		input       *db.Neo4jError
		output      *db.Neo4jError
	}

	testCases := []testCase{
		{
			description: "fills missing fields",
			input:       &db.Neo4jError{},
			output: &db.Neo4jError{
				Code:                 "Neo.DatabaseError.General.UnknownError",
				Msg:                  "An unknown error occurred",
				GqlStatus:            "50N42",
				GqlStatusDescription: "error: general processing exception - unexpected error. An unknown error occurred",
				GqlClassification:    "UNKNOWN",
				GqlRawClassification: "",
				GqlDiagnosticRecord:  gql.NewDefaultDiagnosticRecord(),
				GqlCause:             nil,
			},
		},
		{
			description: "respects existing fields and diagnostic record classification",
			input: &db.Neo4jError{
				Code:                 "Neo.ClientError.Transaction.LockClientStopped",
				Msg:                  "Transaction lock client stopped",
				GqlStatus:            "01N00",
				GqlStatusDescription: "custom status description",
				GqlDiagnosticRecord: map[string]any{
					"_classification": "CLIENT_ERROR",
					"_custom_key":     "hello",
				},
				GqlCause: &db.Neo4jError{},
			},
			output: &db.Neo4jError{
				Code:                 "Neo.ClientError.Transaction.LockClientStopped",
				Msg:                  "Transaction lock client stopped",
				GqlStatus:            "01N00",
				GqlStatusDescription: "custom status description",
				GqlClassification:    db.ClientError,
				GqlRawClassification: "CLIENT_ERROR",
				GqlDiagnosticRecord: map[string]any{
					"OPERATION":       "",
					"OPERATION_CODE":  "0",
					"CURRENT_SCHEMA":  "/",
					"_classification": "CLIENT_ERROR",
					"_custom_key":     "hello",
				},
				GqlCause: &db.Neo4jError{},
			},
		},
	}

	outer.Parallel()
	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			errorutil.PolyfillGqlError(testCase.input)

			AssertDeepEquals(t, testCase.input, testCase.output)
		})
	}
}
