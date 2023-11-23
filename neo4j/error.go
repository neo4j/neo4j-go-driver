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

package neo4j

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/retry"
)

// IsRetryable determines whether an operation can be retried based on the error
// it triggered. This API is meant for use in scenarios where users want to
// implement their own retry mechanism.
// A similar logic is used by the driver for transaction functions.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	return retry.IsRetryable(err)
}

// Neo4jError represents errors originating from Neo4j service.
// Alias for convenience. This error is defined in db package and
// used internally.
type Neo4jError = db.Neo4jError

type UsageError = errorutil.UsageError

type ConnectivityError = errorutil.ConnectivityError

type TransactionExecutionLimit = errorutil.TransactionExecutionLimit

type InvalidAuthenticationError struct {
	inner error
}

func (i *InvalidAuthenticationError) Error() string {
	return fmt.Sprintf("InvalidAuthenticationError: %s", i.inner.Error())
}

func (i *InvalidAuthenticationError) Unwrap() error {
	return i.inner
}

// IsNeo4jError returns true if the provided error is an instance of Neo4jError.
func IsNeo4jError(err error) bool {
	_, is := err.(*Neo4jError)
	return is
}

// IsUsageError returns true if the provided error is an instance of UsageError.
func IsUsageError(err error) bool {
	_, is := err.(*UsageError)
	return is
}

// IsConnectivityError returns true if the provided error is an instance of ConnectivityError.
func IsConnectivityError(err error) bool {
	_, is := err.(*ConnectivityError)
	return is
}

// IsTransactionExecutionLimit returns true if the provided error is an instance of TransactionExecutionLimit.
func IsTransactionExecutionLimit(err error) bool {
	_, is := err.(*TransactionExecutionLimit)
	return is
}

type TokenExpiredError = errorutil.TokenExpiredError

type ctxCloser interface {
	Close(ctx context.Context) error
}

func deferredClose(ctx context.Context, closer ctxCloser, prevErr error) error {
	return errorutil.CombineErrors(prevErr, closer.Close(ctx))
}
