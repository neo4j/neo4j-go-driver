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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"log"
	"os"
)

// Logging is the interface that any provided logging target must satisfy for the neo4j
// driver to send its logging messages
type Logging interface {
	ErrorEnabled() bool
	WarningEnabled() bool
	InfoEnabled() bool
	DebugEnabled() bool

	Errorf(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Debugf(message string, args ...interface{})
}

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

type internalLogger struct {
	level         LogLevel
	errorLogger   *log.Logger
	warningLogger *log.Logger
	infoLogger    *log.Logger
	debugLogger   *log.Logger
}

// NoOpLogger returns a logger that doesn't generate any output at all
func NoOpLogger() Logging {
	return &internalLogger{level: 0}
}

// ConsoleLogger returns a simple logger that writes its messages to the console
func ConsoleLogger(level LogLevel) Logging {
	return &internalLogger{
		level:         level,
		errorLogger:   log.New(os.Stderr, "ERROR  : ", log.Ldate|log.Ltime|log.Lmicroseconds),
		warningLogger: log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lmicroseconds),
		infoLogger:    log.New(os.Stdout, "INFO   : ", log.Ldate|log.Ltime|log.Lmicroseconds),
		debugLogger:   log.New(os.Stdout, "DEBUG  : ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}

func (logger *internalLogger) ErrorEnabled() bool {
	return ERROR <= logger.level
}

func (logger *internalLogger) WarningEnabled() bool {
	return WARNING <= logger.level
}

func (logger *internalLogger) InfoEnabled() bool {
	return INFO <= logger.level
}

func (logger *internalLogger) DebugEnabled() bool {
	return DEBUG <= logger.level
}

func (logger *internalLogger) Errorf(message string, args ...interface{}) {
	logger.errorLogger.Printf(message, args...)
}

func (logger *internalLogger) Warningf(message string, args ...interface{}) {
	logger.warningLogger.Printf(message, args...)
}

func (logger *internalLogger) Infof(message string, args ...interface{}) {
	logger.infoLogger.Printf(message, args...)
}

func (logger *internalLogger) Debugf(message string, args ...interface{}) {
	logger.debugLogger.Printf(message, args...)
}

// Implement internal logger interface on top of the exported.
// Temporary solution for backwards compatibility, next major we should export the internal
// logger interface instead.
type adaptorLogger struct {
	logging Logging
}

func (l adaptorLogger) Error(componentId string, err error) {
	if l.logging.ErrorEnabled() {
		l.logging.Errorf("%s:%s", componentId, err)
	}
}

func (l *adaptorLogger) Errorf(componentId string, msg string, args ...interface{}) {
	if l.logging.ErrorEnabled() {
		l.logging.Errorf(componentId+":"+msg, args...)
	}
}

func (l *adaptorLogger) Warnf(componentId string, msg string, args ...interface{}) {
	if l.logging.WarningEnabled() {
		l.logging.Warningf(componentId+":"+msg, args...)
	}
}

func (l *adaptorLogger) Infof(componentId string, msg string, args ...interface{}) {
	if l.logging.InfoEnabled() {
		l.logging.Infof(componentId+":"+msg, args...)
	}
}

func (l *adaptorLogger) Debugf(componentId string, msg string, args ...interface{}) {
	if l.logging.DebugEnabled() {
		l.logging.Debugf(componentId+":"+msg, args...)
	}
}
