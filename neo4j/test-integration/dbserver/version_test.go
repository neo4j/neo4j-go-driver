/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

package dbserver

import (
	"math"
	"reflect"
	"testing"
)

func TestVersionOf(outer *testing.T) {

	outer.Parallel()

	type testCase struct {
		input          string
		expectedOutput Version
	}

	testCases := []testCase{
		{input: "Neo4j/4.3.2", expectedOutput: Version{major: 4, minor: 3, patch: 2}},
		{input: "Neo4j/4.3.2-s.0.m.e-s.u.f.f.1.x", expectedOutput: Version{major: 4, minor: 3, patch: 2}},
		{input: "Neo4j/4.3", expectedOutput: Version{major: 4, minor: 3, patch: 0}},
		{input: "Neo4j/5.dev", expectedOutput: Version{major: 5, minor: math.MaxInt, patch: 0}},
		{input: "Neo4j/5.dev-s.0.m.e-s.u.f.f.1.x", expectedOutput: Version{major: 5, minor: math.MaxInt, patch: 0}},
		{input: "4.3.2", expectedOutput: Version{major: 4, minor: 3, patch: 2}},
		{input: "4.3.2-s.0.m.e-s.u.f.f.1.x", expectedOutput: Version{major: 4, minor: 3, patch: 2}},
		{input: "4.3", expectedOutput: Version{major: 4, minor: 3, patch: 0}},
		{input: "4.3-s.0.m.e-s.u.f.f.1.x", expectedOutput: Version{major: 4, minor: 3, patch: 0}},
		{input: "5.dev", expectedOutput: Version{major: 5, minor: math.MaxInt, patch: 0}},
		{input: "5.dev-s.0.m.e-s.u.f.f.1.x", expectedOutput: Version{major: 5, minor: math.MaxInt, patch: 0}},
	}

	for _, testCase := range testCases {
		input := testCase.input
		outer.Run(input, func(t *testing.T) {
			actualOutput := VersionOf(input)

			expectedOutput := testCase.expectedOutput
			if !reflect.DeepEqual(expectedOutput, actualOutput) {
				t.Errorf("expected %s, but got %s", expectedOutput.String(), actualOutput.String())
			}
		})
	}
}
