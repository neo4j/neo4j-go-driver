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
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"io"
	"net"
)

func CombineAllErrors(errs ...error) error {
	if len(errs) == 0 {
		return nil
	}
	result := errs[0]
	for _, err := range errs[1:] {
		result = CombineErrors(result, err)
	}
	return result
}

func CombineErrors(err1, err2 error) error {
	if err2 == nil {
		return err1
	}
	if err1 == nil {
		return err2
	}
	return fmt.Errorf("error %v occurred after previous error %w", err2, err1)
}

func WrapError(err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		return &ConnectivityError{Inner: err}
	}
	switch e := err.(type) {
	case *db.UnsupportedTypeError:
		// Usage of a type not supported by database network protocol or feature
		// not supported by current version or edition.
		return &UsageError{Message: err.Error()}
	case *db.FeatureNotSupportedError:
		return &UsageError{Message: fmt.Sprintf("feature not supported: %s", err.Error())}
	case *PoolClosed:
		return &UsageError{Message: err.Error()}
	case *TlsError, net.Error:
		return &ConnectivityError{Inner: err}
	case *PoolTimeout, *PoolFull:
		return &ConnectivityError{Inner: err}
	case *ReadRoutingTableError:
		return &ConnectivityError{Inner: err}
	case *CommitFailedDeadError:
		return &ConnectivityError{Inner: err}
	case *ConnectionReadTimeout:
		return &ConnectivityError{Inner: err}
	case *ConnectionWriteTimeout:
		return &ConnectivityError{Inner: err}
	case *db.Neo4jError:
		if e.Code == "Neo.ClientError.Security.TokenExpired" {
			return &TokenExpiredError{Code: e.Code, Message: e.Msg, cause: e}
		}
	}
	if err != nil && err.Error() == InvalidTransactionError {
		return &UsageError{Message: InvalidTransactionError}
	}
	return err
}

// UsageError represents errors caused by incorrect usage of the driver API.
// This does not include Cypher syntax (those errors will be Neo4jError).
type UsageError struct {
	Message string
}

func (e *UsageError) Error() string {
	return e.Message
}

// ConnectivityError represent errors caused by the driver not being able to connect to Neo4j services,
// or lost connections.
type ConnectivityError struct {
	Inner error
}

func (e *ConnectivityError) Error() string {
	return fmt.Sprintf("ConnectivityError: %s", e.Inner.Error())
}

// TokenExpiredError represent errors caused by the driver not being able to connect to Neo4j services,
// or lost connections.
type TokenExpiredError struct {
	Code    string
	Message string
	cause   *db.Neo4jError
}

func (e *TokenExpiredError) Unwrap() error {
	return e.cause
}

func (e *TokenExpiredError) Error() string {
	return fmt.Sprintf("TokenExpiredError: %s (%s)", e.Code, e.Message)
}
