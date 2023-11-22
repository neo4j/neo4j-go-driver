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

import (
	"fmt"
	"os"
	"time"
)

type BoltLogger interface {
	LogClientMessage(context string, msg string, args ...any)
	LogServerMessage(context string, msg string, args ...any)
}

type ConsoleBoltLogger struct {
}

func (cbl *ConsoleBoltLogger) LogClientMessage(id, msg string, args ...any) {
	cbl.logBoltMessage("C", id, msg, args)
}

func (cbl *ConsoleBoltLogger) LogServerMessage(id, msg string, args ...any) {
	cbl.logBoltMessage("S", id, msg, args)
}

func (cbl *ConsoleBoltLogger) logBoltMessage(src, id string, msg string, args []any) {
	_, _ = fmt.Fprintf(os.Stdout, "%s   BOLT  %s%s: %s\n", time.Now().Format(timeFormat), formatId(id), src, fmt.Sprintf(msg, args...))
}

func formatId(id string) string {
	if id == "" {
		return ""
	}
	return fmt.Sprintf("[%s] ", id)
}
