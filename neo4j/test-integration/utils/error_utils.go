/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package utils

import (
	"fmt"

	"github.com/neo4j-drivers/gobolt"
)

type testDatabaseError struct {
	classification string
	code           string
	message        string
}

type testConnectorError struct {
	state       int
	code        int
	codeText    string
	context     string
	description string
}

type testDriverError struct {
	message string
}

func (failure *testDatabaseError) BoltError() bool {
	return true
}

func (failure *testDatabaseError) Classification() string {
	return failure.classification
}

func (failure *testDatabaseError) Code() string {
	return failure.code
}

func (failure *testDatabaseError) Message() string {
	return failure.message
}

func (failure *testDatabaseError) Error() string {
	return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

func (failure *testConnectorError) BoltError() bool {
	return true
}

func (failure *testConnectorError) State() int {
	return failure.state
}

func (failure *testConnectorError) Code() int {
	return failure.code
}

func (failure *testConnectorError) Context() string {
	return failure.context
}

func (failure *testConnectorError) Description() string {
	return failure.description
}

func (failure *testConnectorError) Error() string {
	return fmt.Sprintf("expected connection to be in READY state, where it is %d [error is %d]", failure.state, failure.code)
}

func (failure *testDriverError) BoltError() bool {
	return true
}

func (failure *testDriverError) Message() string {
	return failure.message
}

func (failure *testDriverError) Error() string {
	return failure.message
}

func NewDriverErrorForTest(format string, args ...interface{}) gobolt.GenericError {
	return &testDriverError{message: fmt.Sprintf(format, args...)}
}

func NewDatabaseErrorForTest(classification, code, message string) gobolt.DatabaseError {
	return &testDatabaseError{code: code, message: message, classification: classification}
}

func NewConnectorErrorForTest(state int, code int, codeText, context, description string) gobolt.ConnectorError {
	return &testConnectorError{state: state, code: code, codeText: codeText, context: context, description: description}
}
