/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
)

type driverError struct {
	message string
}

func (failure *driverError) Error() string {
	return failure.message
}

func newDriverError(format string, args ...interface{}) *driverError {
	return &driverError{message: fmt.Sprintf(format, args...)}
}

/*
func (failure *driverError) BoltError() bool {
	return true
}

func (failure *driverError) Message() string {
	return failure.message
}
*/

/*
import "C"

type databaseError struct {
	classification string
	code           string
	message        string
}

type connectorError struct {
	state       int
	code        int
	codeText    string
	context     string
	description string
}

type sessionExpiredError struct {
	message string
}

func (failure *databaseError) BoltError() bool {
	return true
}

func (failure *databaseError) Classification() string {
	return failure.classification
}

func (failure *databaseError) Code() string {
	return failure.code
}

func (failure *databaseError) Message() string {
	return failure.message
}

func (failure *databaseError) Error() string {
	return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

func (failure *connectorError) BoltError() bool {
	return true
}

func (failure *connectorError) State() int {
	return failure.state
}

func (failure *connectorError) Code() int {
	return failure.code
}

func (failure *connectorError) Context() string {
	return failure.context
}

func (failure *connectorError) Description() string {
	return failure.description
}

func (failure *connectorError) Error() string {
	if failure.description != "" {
		return fmt.Sprintf("%s: error: [%d] %s, state: %d, context: %s", failure.description, failure.code, failure.codeText, failure.state, failure.context)
	} else {
		return fmt.Sprintf("error: [%d] %s, state: %d, context: %s", failure.code, failure.codeText, failure.state, failure.context)
	}
}

func (failure *sessionExpiredError) BoltError() bool {
	return true
}

func (failure *sessionExpiredError) Error() string {
	return failure.message
}

func newSessionExpiredError(format string, args ...interface{}) error {
	return &sessionExpiredError{message: fmt.Sprintf(format, args...)}
}

func newDatabaseError(classification, code, message string) *databaseError {
	return &databaseError{code: code, message: message, classification: classification}
}

func newConnectorError(state int, code int, codeText, context, description string) *connectorError {
	return &connectorError{state: state, code: code, codeText: codeText, context: context, description: description}
}
*/

// IsSecurityError is a utility method to check if the provided error is related with any
// TLS failure or authentication issues.
func IsSecurityError(err error) bool {
	_, is := err.(*tlsError)
	return is
}

/*
func isRetriableError(err error) bool {
	return gobolt.IsServiceUnavailable(err) || gobolt.IsTransientError(err) || gobolt.IsWriteError(err)
}
*/

// IsAuthenticationError is a utility method to check if the provided error is related with any
// authentication issues.
func IsAuthenticationError(err error) bool {
	dbErr, is := err.(*db.DatabaseError)
	if !is {
		return false
	}
	return dbErr.IsAuthentication()
}

// IsClientError is a utility method to check if the provided error is related with the client
// carrying out an invalid operation.
func IsClientError(err error) bool {
	dbErr, is := err.(*db.DatabaseError)
	if !is {
		return false
	}
	return dbErr.IsClient()
}

// IsTransientError is a utility method to check if the provided error is related with a temporary
// failure that may be worked around by retrying.
func IsTransientError(err error) bool {
	dbErr, is := err.(*db.DatabaseError)
	if !is {
		return false
	}
	return dbErr.IsRetriableTransient()
}

/*
// IsSessionExpired is a utility method to check if the session no longer satisfy the criteria
// under which it was acquired, e.g. a server no longer accepts write requests.
func IsSessionExpired(err error) bool {
	if _, ok := err.(*sessionExpiredError); ok {
		return true
	}

	return gobolt.IsSessionExpired(err)
}
*/

// IsServiceUnavailable is a utility method to check if the provided error can be classified
// to be in service unavailable category.
func IsServiceUnavailable(err error) bool {
	_, is := err.(*connectError)
	// TODO: More errors in this category!
	return is
}
