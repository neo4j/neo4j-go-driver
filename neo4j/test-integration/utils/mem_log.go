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

import "fmt"

// MemoryLogging collects all log messages into its own data structures, mostly to
// be used in tests
type MemoryLogging struct {
	// Errors contains all log messages written at Error level
	Errors []string
	// Warnings contains all log messages written at Warning level
	Warnings []string
	// Infos contains all log messages written at Info level
	Infos []string
	// Debugs contains all log messages written at Debug level
	Debugs []string
}

// ErrorEnabled returns whether Error level is enabled
func (log *MemoryLogging) ErrorEnabled() bool {
	return true
}

// WarningEnabled returns whether Warning level is enabled
func (log *MemoryLogging) WarningEnabled() bool {
	return true
}

// InfoEnabled returns whether Info level is enabled
func (log *MemoryLogging) InfoEnabled() bool {
	return true
}

// DebugEnabled returns whether Debug level is enabled
func (log *MemoryLogging) DebugEnabled() bool {
	return true
}

// Errorf writes a log message at Error level
func (log *MemoryLogging) Errorf(message string, args ...interface{}) {
	log.Errors = append(log.Errors, fmt.Sprintf(message, args...))
}

// Warningf writes a log message at Warning level
func (log *MemoryLogging) Warningf(message string, args ...interface{}) {
	log.Warnings = append(log.Warnings, fmt.Sprintf(message, args...))
}

// Infof writes a log message at Info level
func (log *MemoryLogging) Infof(message string, args ...interface{}) {
	log.Infos = append(log.Infos, fmt.Sprintf(message, args...))
}

// Debugf writes a log message at Debug level
func (log *MemoryLogging) Debugf(message string, args ...interface{}) {
	log.Debugs = append(log.Debugs, fmt.Sprintf(message, args...))
}
