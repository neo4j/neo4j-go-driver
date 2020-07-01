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
	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/pool"
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

// IsSecurityError is a utility method to check if the provided error is related with any
// TLS failure or authentication issues.
func IsSecurityError(err error) bool {
	_, is := err.(*tlsError)
	return is
}

// IsAuthenticationError is a utility method to check if the provided error is related with any
// authentication issues.
func IsAuthenticationError(err error) bool {
	dbErr, is := err.(*connection.DatabaseError)
	if !is {
		return false
	}
	return dbErr.IsAuthentication()
}

// IsClientError is a utility method to check if the provided error is related with the client
// carrying out an invalid operation.
func IsClientError(err error) bool {
	dbErr, is := err.(*connection.DatabaseError)
	if !is {
		return false
	}
	return dbErr.IsClient()
}

// IsTransientError is a utility method to check if the provided error is related with a temporary
// failure that may be worked around by retrying.
func IsTransientError(err error) bool {
	dbErr, is := err.(*connection.DatabaseError)
	if !is {
		return false
	}
	return dbErr.IsRetriableTransient()
}

// IsSessionExpired is a utility method to check if the session no longer satisfy the criteria
// under which it was acquired, e.g. a server no longer accepts write requests.
func IsSessionExpired(err error) bool {
	return false
}

// IsServiceUnavailable is a utility method to check if the provided error can be classified
// to be in service unavailable category.
func IsServiceUnavailable(err error) bool {
	_, is := err.(*connectError)
	if is {
		return true
	}
	_, is = err.(*pool.PoolTimeout)
	if is {
		return true
	}
	_, is = err.(*pool.PoolFull)
	return is
}
