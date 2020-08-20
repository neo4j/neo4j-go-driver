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

package neo4j

import (
	"fmt"
	"os"
	"time"
)

// LogLevel is the type that default logging implementations use for available
// log levels
type LogLevel int

const (
	// ERROR is the level that error messages are written
	ERROR LogLevel = 1
	// WARNING is the level that warning messages are written
	WARNING = 2
	// INFO is the level that info messages are written
	INFO = 3
	// DEBUG is the level that debug messages are written
	DEBUG = 4
)

func ConsoleLogger(level LogLevel) *ConsoleLog {
	return &ConsoleLog{
		Errors: level >= ERROR,
		Warns:  level >= WARNING,
		Infos:  level >= INFO,
		Debugs: level >= DEBUG,
	}
}

// 2020-05-03 12:39:45.001  ERROR  [router 1] Failed to connect
// 2020-05-03 12:39:45.001   INFO  [Server@192.168.0.1:666] Custom message
// 2020-05-03 12:39:45.001   WARN  [bolt3 bolt-361@127.0.0.1:7687] Custom message
type ConsoleLog struct {
	Errors bool
	Infos  bool
	Warns  bool
	Debugs bool
}

const timeFormat = "2006-01-02 15:04:05.000"

func (l *ConsoleLog) Error(name, id string, err error) {
	if !l.Errors {
		return
	}
	now := time.Now()
	fmt.Fprintf(os.Stderr, "%s  ERROR  [%s %s] %s\n", now.Format(timeFormat), name, id, err.Error())
}

func (l *ConsoleLog) Errorf(name, id string, msg string, args ...interface{}) {
	if !l.Infos {
		return
	}
	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s  ERROR  [%s %s] %s\n", now.Format(timeFormat), name, id, fmt.Sprintf(msg, args...))
}

func (l *ConsoleLog) Infof(name, id string, msg string, args ...interface{}) {
	if !l.Infos {
		return
	}
	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s   INFO  [%s %s] %s\n", now.Format(timeFormat), name, id, fmt.Sprintf(msg, args...))
}

func (l *ConsoleLog) Warnf(name, id string, msg string, args ...interface{}) {
	if !l.Warns {
		return
	}
	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s   WARN  [%s %s] %s\n", now.Format(timeFormat), name, id, fmt.Sprintf(msg, args...))
}

func (l *ConsoleLog) Debugf(name, id string, msg string, args ...interface{}) {
	if !l.Debugs {
		return
	}
	now := time.Now()
	fmt.Fprintf(os.Stdout, "%s  DEBUG  [%s %s] %s\n", now.Format(timeFormat), name, id, fmt.Sprintf(msg, args...))
}
