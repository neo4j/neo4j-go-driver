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
