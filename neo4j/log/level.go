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

package log

// Level is the type that default logging implementations use for available
// log levels
type Level int

const (
	// ERROR is the level that error messages are written
	ERROR Level = 1
	// WARNING is the level that warning messages are written
	WARNING = 2
	// INFO is the level that info messages are written
	INFO = 3
	// DEBUG is the level that debug messages are written
	DEBUG = 4
)
