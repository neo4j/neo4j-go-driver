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

import (
	"fmt"
	"os"
	"time"
)

// 2020-05-03 12:39:45.001  ERROR  [Server@192.168.0.1:666] Failed to connect
// 2020-05-03 12:39:45.001   INFO  [Server@192.168.0.1:666] Custom message
// 2020-05-03 12:39:45.001   WARN  [Server@192.168.0.1:666] Custom message
type ConsoleLogger struct {
	Errors bool
	Infos  bool
	Warns  bool
}

const timeFormat = "2006-01-02 15:04:05.000"

func (l *ConsoleLogger) Error(id string, err error) {
	if !l.Errors {
		return
	}

	now := time.Now()
	fmt.Fprintf(os.Stderr, "%s  ERROR  [%s] %s\n", now.Format(timeFormat), id, err.Error())
}

func (l *ConsoleLogger) Errorf(id string, msg string, args ...interface{}) {
	if !l.Infos {
		return
	}

	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s  ERROR  [%s] %s\n", now.Format(timeFormat), id, fmt.Sprintf(msg, args...))
}

func (l *ConsoleLogger) Infof(id string, msg string, args ...interface{}) {
	if !l.Infos {
		return
	}

	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s   INFO  [%s] %s\n", now.Format(timeFormat), id, fmt.Sprintf(msg, args...))
}

func (l *ConsoleLogger) Warnf(id string, msg string, args ...interface{}) {
	if !l.Warns {
		return
	}

	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s   WARN  [%s] %s\n", now.Format(timeFormat), id, fmt.Sprintf(msg, args...))
}

func (l *ConsoleLogger) Debugf(id string, msg string, args ...interface{}) {
	if !l.Warns {
		return
	}

	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s  DEBUG  [%s] %s\n", now.Format(timeFormat), id, fmt.Sprintf(msg, args...))
}
