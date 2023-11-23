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

package main

import (
	"fmt"
	"time"
)

const timeFormat = "2006-01-02 15:04:05.000"

type streamLog struct {
	writeLine func(string) error
}

func (l *streamLog) Error(name string, id string, err error) {
	_ = l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, err))
}
func (l *streamLog) Warnf(name string, id string, msg string, args ...any) {
	_ = l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Infof(name string, id string, msg string, args ...any) {
	_ = l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Debugf(name string, id string, msg string, args ...any) {
	_ = l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
}

func (l *streamLog) LogClientMessage(id, msg string, args ...any) {
	l.logBoltMessage("C", id, msg, args)
}

func (l *streamLog) LogServerMessage(id, msg string, args ...any) {
	l.logBoltMessage("S", id, msg, args)
}

func (l *streamLog) logBoltMessage(src, id string, msg string, args []any) {
	_ = l.writeLine(fmt.Sprintf("%s   BOLT  %s%s: %s", time.Now().Format(timeFormat), formatId(id), src, fmt.Sprintf(msg, args...)))
}

func formatId(id string) string {
	if id == "" {
		return ""
	}
	return fmt.Sprintf("[%s] ", id)
}
