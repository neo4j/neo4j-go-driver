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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package log

// Logger is used throughout the driver for logging purposes.
// Driver client can implement this interface and provide an implementation
// upon driver creation.
//
// All logging functions takes a name and id that corresponds to the name of
// the logging component and it's identity, for example "router" and "1" to
// indicate who is logging and what instance.
//
// Database connections takes to form of "bolt3" and "bolt-123@192.168.0.1:7687"
// where "bolt3" is the name of the protocol handler in use, "bolt-123" is the
// databatases identity of the connection on server "192.168.0.1:7687".
type Logger interface {
	Error(name string, id string, err error)
	Errorf(name string, id string, msg string, args ...interface{})
	Warnf(name string, id string, msg string, args ...interface{})
	Infof(name string, id string, msg string, args ...interface{})
	Debugf(name string, id string, msg string, args ...interface{})
}
