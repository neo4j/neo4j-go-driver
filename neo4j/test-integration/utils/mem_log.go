/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

type MemoryLogging struct {
	Errors   []string
	Warnings []string
	Infos    []string
	Debugs   []string
}

func (log *MemoryLogging) ErrorEnabled() bool {
	return true
}

func (log *MemoryLogging) WarningEnabled() bool {
	return true
}

func (log *MemoryLogging) InfoEnabled() bool {
	return true
}

func (log *MemoryLogging) DebugEnabled() bool {
	return true
}

func (log *MemoryLogging) Errorf(message string, args ...interface{}) {
	log.Errors = append(log.Errors, fmt.Sprintf(message, args...))
}

func (log *MemoryLogging) Warningf(message string, args ...interface{}) {
	log.Warnings = append(log.Warnings, fmt.Sprintf(message, args...))
}

func (log *MemoryLogging) Infof(message string, args ...interface{}) {
	log.Infos = append(log.Infos, fmt.Sprintf(message, args...))
}

func (log *MemoryLogging) Debugf(message string, args ...interface{}) {
	log.Debugs = append(log.Debugs, fmt.Sprintf(message, args...))
}
