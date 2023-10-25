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

package errorutil

import (
	"context"
	"errors"
	"fmt"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"strings"
	"time"
)

const InvalidTransactionError = "invalid transaction handle"

type ConnectionReadTimeout struct {
	UserContext context.Context
	ReadTimeout time.Duration
	Err         error
}

func (crt *ConnectionReadTimeout) Error() string {
	userDeadline := "N/A"
	if deadline, ok := crt.UserContext.Deadline(); ok {
		userDeadline = deadline.String()
	}
	return fmt.Sprintf(
		"Timeout while reading from connection [server-side timeout hint: %s, user-provided context deadline: %s]: %s",
		crt.ReadTimeout.String(),
		userDeadline,
		crt.Err)
}

type ConnectionWriteTimeout struct {
	UserContext context.Context
	Err         error
}

func (cwt *ConnectionWriteTimeout) Error() string {
	userDeadline := "N/A"
	if deadline, ok := cwt.UserContext.Deadline(); ok {
		userDeadline = deadline.String()
	}
	return fmt.Sprintf("Timeout while writing to connection [user-provided context deadline: %s]: %s", userDeadline, cwt.Err)
}

type ConnectionReadCanceled struct {
	Err error
}

func (crc *ConnectionReadCanceled) Error() string {
	return fmt.Sprintf("Reading from connection has been canceled: %s", crc.Err)
}

type ConnectionWriteCanceled struct {
	Err error
}

func (cwc *ConnectionWriteCanceled) Error() string {
	return fmt.Sprintf("Writing to connection has been canceled: %s", cwc.Err)
}

type timeout interface {
	Timeout() bool
}

func IsTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	timeoutErr, ok := err.(timeout)
	return ok && timeoutErr.Timeout()
}

func IsFatalDuringDiscovery(err error) bool {
	var featureNotSupportedError *idb.FeatureNotSupportedError
	if errors.As(err, &featureNotSupportedError) {
		return true
	}
	var neo4jErr *idb.Neo4jError
	if errors.As(err, &neo4jErr) {
		if neo4jErr.Code == "Neo.ClientError.Database.DatabaseNotFound" ||
			neo4jErr.Code == "Neo.ClientError.Transaction.InvalidBookmark" ||
			neo4jErr.Code == "Neo.ClientError.Transaction.InvalidBookmarkMixture" ||
			neo4jErr.Code == "Neo.ClientError.Statement.TypeError" ||
			neo4jErr.Code == "Neo.ClientError.Statement.ArgumentError" ||
			neo4jErr.Code == "Neo.ClientError.Request.Invalid" {
			return true
		}
		if strings.HasPrefix(neo4jErr.Code, "Neo.ClientError.Security.") &&
			neo4jErr.Code != "Neo.ClientError.Security.AuthorizationExpired" {
			return true
		}
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return true
	}
	return false
}
