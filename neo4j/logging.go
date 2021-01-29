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
	"path/filepath"
	"runtime"
)

// Logging is the interface that any provided logging target must satisfy for the neo4j
// driver to send its logging messages
type Logging interface {
	ErrorEnabled() bool
	WarningEnabled() bool
	InfoEnabled() bool
	DebugEnabled() bool

	Errorf(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Debugf(message string, args ...interface{})
}

func fileAndLineNumberOfCall() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "?"
		line = 0
	}
	_, file = filepath.Split(file)

	return fmt.Sprintf("%s:%d", file, line)
}

func errorf(logging Logging, message string, args ...interface{}) {
	if logging != nil && logging.ErrorEnabled() {
		logging.Errorf(fmt.Sprintf("%s: %s", fileAndLineNumberOfCall(), message), args...)
	}
}

func warningf(logging Logging, message string, args ...interface{}) {
	if logging != nil && logging.WarningEnabled() {
		logging.Warningf(fmt.Sprintf("%s: %s", fileAndLineNumberOfCall(), message), args...)
	}
}

func infof(logging Logging, message string, args ...interface{}) {
	if logging != nil && logging.InfoEnabled() {
		logging.Infof(fmt.Sprintf("%s: %s", fileAndLineNumberOfCall(), message), args...)
	}
}

func debugf(logging Logging, message string, args ...interface{}) {
	if logging != nil && logging.DebugEnabled() {
		logging.Debugf(fmt.Sprintf("%s: %s", fileAndLineNumberOfCall(), message), args...)
	}
}
