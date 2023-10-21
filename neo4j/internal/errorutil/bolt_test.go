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

package errorutil_test

import (
	"context"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/db"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func TestIsFatalDuringDiscovery(outer *testing.T) {
	type testCase struct {
		description string
		err         error
		isFatal     bool
	}

	testCases := []testCase{
		{
			description: "nil is not fatal, duh!",
			err:         nil,
			isFatal:     false,
		}, {
			description: "DatabaseNotFound is fatal",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Database.DatabaseNotFound"},
			isFatal:     true,
		}, {
			description: "InvalidBookmark is fatal",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Transaction.InvalidBookmark"},
			isFatal:     true,
		}, {
			description: "InvalidBookmarkMixture is fatal",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Transaction.InvalidBookmarkMixture"},
			isFatal:     true,
		}, {
			description: "Any security error but Neo.ClientError.Security.AuthorizationExpired is fatal",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.AuthorizationExpired"},
			isFatal:     false,
		}, {
			description: "Made up security error is fatal",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.MadeUpError"},
			isFatal:     true,
		}, {
			description: "AuthenticationRateLimit is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.AuthenticationRateLimit"},
			isFatal:     true,
		}, {
			description: "CredentialsExpired is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.CredentialsExpired"},
			isFatal:     true,
		}, {
			description: "Forbidden is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.Forbidden"},
			isFatal:     true,
		}, {
			description: "TokenExpired is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.TokenExpired"},
			isFatal:     true,
		}, {
			description: "Unauthorized is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Security.Unauthorized"},
			isFatal:     true,
		}, {
			description: "ArgumentError is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Statement.ArgumentError"},
			isFatal:     true,
		}, {
			description: "Invalid is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Request.Invalid"},
			isFatal:     true,
		}, {
			description: "TypeError is a fatal security error",
			err:         &db.Neo4jError{Code: "Neo.ClientError.Statement.TypeError"},
			isFatal:     true,
		}, {
			description: "DeadlineExceeded is fatal (we don't have time left to do anything else)",
			err:         context.DeadlineExceeded,
			isFatal:     true,
		}, {
			description: "ConnectionReadTimeout is not fatal",
			err:         &errorutil.ConnectionReadTimeout{},
			isFatal:     false,
		}, {
			description: "ConnectionWriteTimeout is not fatal",
			err:         &errorutil.ConnectionWriteTimeout{},
			isFatal:     false,
		}, {
			description: "ConnectionReadCanceled is not fatal",
			err:         &errorutil.ConnectionReadCanceled{},
			isFatal:     false,
		}, {
			description: "ConnectionWriteCanceled is not fatal",
			err:         &errorutil.ConnectionWriteCanceled{},
			isFatal:     false,
		}, {
			description: "PoolTimeout is not fatal",
			err:         &errorutil.PoolTimeout{},
			isFatal:     false,
		}, {
			description: "PoolFull is not fatal",
			err:         &errorutil.PoolFull{},
			isFatal:     false,
		}, {
			description: "PoolClosed is not fatal",
			err:         &errorutil.PoolClosed{},
			isFatal:     false,
		}, {
			description: "ReadRoutingTableError is not fatal",
			err:         &errorutil.ReadRoutingTableError{},
			isFatal:     false,
		}, {
			description: "TlsError is not fatal",
			err:         &errorutil.TlsError{},
			isFatal:     false,
		},
	}

	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			testutil.AssertBoolEqual(t, errorutil.IsFatalDuringDiscovery(testCase.err), testCase.isFatal)
		})
	}
}
