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

package propertyencryption

import (
	"errors"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/errorutil"
	ipe "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/propertyencryption"
)

// Error reports a failure to resolve an encryption key, to encrypt, to decrypt, or to
// interpret an encrypted value.
//
// A KeyEncapsulationService or EncapsulatedKeyRepository failure that is already a driver
// error is returned unchanged, so that a retryable failure stays retryable. Anything else is
// wrapped, with the original reachable through errors.Unwrap.
//
// Error is part of the property encryption preview feature (see README on what it means in
// terms of support and compatibility guarantees).
type Error struct {
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause == nil {
		return e.Message
	}
	return e.Message + ": " + e.Cause.Error()
}

func (e *Error) Unwrap() error {
	return e.Cause
}

// wrap turns err into an Error unless it is already a driver error, which the caller needs
// to classify as raised.
func wrap(message string, err error) error {
	if err == nil {
		return nil
	}
	if isDriverError(err) {
		return err
	}
	return &Error{Message: message, Cause: err}
}

func isDriverError(err error) bool {
	var neo4jErr *db.Neo4jError
	var usageErr *errorutil.UsageError
	var connectivityErr *errorutil.ConnectivityError
	var tokenErr *errorutil.TokenExpiredError
	return errors.As(err, &neo4jErr) ||
		errors.As(err, &usageErr) ||
		errors.As(err, &connectivityErr) ||
		errors.As(err, &tokenErr)
}

// asError converts a codec failure into an Error, keeping message as the description.
func asError(message string, err error) error {
	if err == nil {
		return nil
	}
	var valueErr *ipe.ValueError
	var malformedErr *ipe.MalformedError
	if errors.As(err, &valueErr) || errors.As(err, &malformedErr) {
		return &Error{Message: message, Cause: err}
	}
	return wrap(message, err)
}
