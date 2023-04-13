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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"testing"
)

func TestIsRetryable(outer *testing.T) {
	type retryableTestCase struct {
		isRetryable bool
		err         error
	}

	testCases := []retryableTestCase{
		{true, &ConnectivityError{
			Inner: fmt.Errorf("hello, is it me you are looking for"),
		}},
		{true, &db.Neo4jError{
			Code: "Neo.TransientError.No.Stress",
			Msg:  "Relax: Retry it Easyyy",
		}},
		{true, &db.Neo4jError{
			Code: "Neo.ClientError.Cluster.NotALeader",
			Msg:  "Lead is heavy. Leader is heavier?",
		}},
		{true, &db.Neo4jError{
			Code: "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
			Msg:  "One does not simply write to a read-only database.",
		}},
		{false, nil},
		{false, &ConnectivityError{
			Inner: &errorutil.CommitFailedDeadError{},
		}},
		{false, &db.Neo4jError{
			Code: "Neo.TransientError.Transaction.Terminated",
			Msg:  "Don't mess with TX",
		}},
		{false, &db.Neo4jError{
			Code: "Neo.TransientError.Transaction.LockClientStopped",
			Msg:  "Don't tell me what I can't do!",
		}},
		{false, &db.Neo4jError{
			Code: "Neo.Completely.Made.Up",
			Msg:  "There is no spoon!",
		}},
		{false, fmt.Errorf("do not try me... do not retry me either")},
	}

	for _, testCase := range testCases {
		outer.Run(fmt.Sprintf("is error %v retryable?", testCase.err), func(t *testing.T) {
			expected := testCase.isRetryable

			actual := IsRetryable(testCase.err)

			if actual != expected {
				t.Fatalf("Expected %t, got %t", expected, actual)
			}
		})
	}

}
