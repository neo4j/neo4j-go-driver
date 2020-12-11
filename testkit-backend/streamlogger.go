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

package main

import (
	"fmt"
)

type streamLog struct {
	writeLine func(string) error
}

func (l *streamLog) Error(name string, id string, err error) {
	l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, err))
}
func (l *streamLog) Warnf(name string, id string, msg string, args ...interface{}) {
	l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Infof(name string, id string, msg string, args ...interface{}) {
	l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Debugf(name string, id string, msg string, args ...interface{}) {
	l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
}
