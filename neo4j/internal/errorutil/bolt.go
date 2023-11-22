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

import (
	"context"
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
	if err == context.DeadlineExceeded {
		return true
	}
	timeoutErr, ok := err.(timeout)
	return ok && timeoutErr.Timeout()
}

func IsFatalDuringDiscovery(err error) bool {
	if _, ok := err.(*idb.FeatureNotSupportedError); ok {
		return true
	}
	if err, ok := err.(*idb.Neo4jError); ok {
		if err.Code == "Neo.ClientError.Database.DatabaseNotFound" ||
			err.Code == "Neo.ClientError.Transaction.InvalidBookmark" ||
			err.Code == "Neo.ClientError.Transaction.InvalidBookmarkMixture" ||
			err.Code == "Neo.ClientError.Statement.TypeError" ||
			err.Code == "Neo.ClientError.Statement.ArgumentError" ||
			err.Code == "Neo.ClientError.Request.Invalid" {
			return true
		}
		if strings.HasPrefix(err.Code, "Neo.ClientError.Security.") &&
			err.Code != "Neo.ClientError.Security.AuthorizationExpired" {
			return true
		}
	}
	if err == context.DeadlineExceeded ||
		err == context.Canceled {
		return true
	}
	return false
}
