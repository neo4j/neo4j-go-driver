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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"reflect"
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

			if !reflect.DeepEqual(testCase.output, output) {
				t.Errorf("expected %v, got %v", testCase.output, output)
			}
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

			if !reflect.DeepEqual(testCase.output, output) {
				t.Errorf("expected %v, got %v", testCase.output, output)
			}
		})
	}
}
