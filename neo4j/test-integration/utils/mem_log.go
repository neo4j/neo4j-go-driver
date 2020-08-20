/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

// This is an internal package and is not part of the public API.
package utils

import "fmt"

// MemoryLog collects all log messages into its own data structures, mostly to
// be used in tests
type MemoryLog struct {
	// Errors contains all log messages written at Error level
	Errors []string
	// Warnings contains all log messages written at Warning level
	Warnings []string
	// Infos contains all log messages written at Info level
	Infos []string
	// Debugs contains all log messages written at Debug level
	Debugs []string
}

func (l *MemoryLog) Error(name, id string, err error) {
}

func (l *MemoryLog) Errorf(name, id string, msg string, args ...interface{}) {
	l.Errors = append(l.Errors, fmt.Sprintf(msg, args...))
}

func (l *MemoryLog) Infof(name, id string, msg string, args ...interface{}) {
	l.Infos = append(l.Infos, fmt.Sprintf(msg, args...))
}

func (l *MemoryLog) Warnf(name, id string, msg string, args ...interface{}) {
	l.Warnings = append(l.Warnings, fmt.Sprintf(msg, args...))
}

func (l *MemoryLog) Debugf(name, id string, msg string, args ...interface{}) {
	l.Debugs = append(l.Debugs, fmt.Sprintf(msg, args...))
}
