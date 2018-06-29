package neo4j_go_driver

import (
	"log"
	"os"
	"io/ioutil"
)

type LogLevel int

const (
	ERROR LogLevel = 0
	WARN           = 1
	INFO           = 2
	DEBUG          = 3
)

type Logging interface {
	Level() LogLevel
	Log(level LogLevel, message string, messageArgs ...string)
}

type internalLogging struct {
	level       LogLevel
	errorLogger *log.Logger
	warnLogger  *log.Logger
	infoLogger  *log.Logger
	debugLogger *log.Logger
}

func isLevelEnabled(logging Logging, level LogLevel) bool {
	if logging == nil {
		return false
	}

	return level <= logging.Level()
}

func (logging *internalLogging) Level() LogLevel {
	return logging.level
}

func (logging *internalLogging) Log(level LogLevel, message string, messageArgs ...string) {
	if level <= logging.level {
		switch level {
		case ERROR:
			logging.errorLogger.Printf(message, messageArgs)
		case WARN:
			logging.warnLogger.Printf(message, messageArgs)
		case INFO:
			logging.infoLogger.Printf(message, messageArgs)
		case DEBUG:
			logging.debugLogger.Printf(message, messageArgs)
		}
	}
}

func NoOpLogger() Logging {
	discardLogger := log.New(ioutil.Discard, "", log.LstdFlags)

	return &internalLogging{
		level:       ERROR,
		errorLogger: discardLogger,
		warnLogger:  discardLogger,
		infoLogger:  discardLogger,
		debugLogger: discardLogger,
	}
}

func ConsoleLogger(level LogLevel) Logging {
	return &internalLogging{
		level:       level,
		errorLogger: log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile),
		warnLogger:  log.New(os.Stdout, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile),
		infoLogger:  log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
		debugLogger: log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}
