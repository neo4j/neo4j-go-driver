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

package errorutil

import "fmt"

type CommitFailedDeadError struct {
	Inner error
}

func (e *CommitFailedDeadError) Error() string {
	return fmt.Sprintf("Connection lost during commit: %s", e.Inner)
}

// TransactionExecutionLimit error indicates that a retryable transaction has
// failed due to reaching a limit like a timeout or maximum number of attempts.
type TransactionExecutionLimit struct {
	Cause  string
	Errors []error
}

func (e *TransactionExecutionLimit) Error() string {
	cause := e.Cause
	var err error
	l := len(e.Errors)

	if l > 0 {
		err = e.Errors[l-1]
	}
	return fmt.Sprintf("TransactionExecutionLimit: %s after %d attempts, last error: %s", cause, len(e.Errors), err)
}
