/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

// LogLevel is the type that default logging implementations use for available
// log levels
//
// Deprecated: use log.Level instead.
type LogLevel = log.Level

const (
	// ERROR is the level that error messages are written
	//
	// Deprecated: use log.ERROR instead.
	ERROR = log.ERROR
	// WARNING is the level that warning messages are written
	//
	// Deprecated: use log.WARNING instead.
	WARNING = log.WARNING
	// INFO is the level that info messages are written
	//
	// Deprecated: use log.INFO instead.
	INFO = log.INFO
	// DEBUG is the level that debug messages are written
	//
	// Deprecated: use log.DEBUG instead.
	DEBUG = log.DEBUG
)

// Deprecated: use log.ToConsole() instead.
func ConsoleLogger(level log.Level) *log.Console {
	return &log.Console{
		Errors: level >= log.ERROR,
		Warns:  level >= log.WARNING,
		Infos:  level >= log.INFO,
		Debugs: level >= log.DEBUG,
	}
}

// Deprecated: use log.BoltToConsole() instead.
func ConsoleBoltLogger() *log.ConsoleBoltLogger {
	return &log.ConsoleBoltLogger{}
}
