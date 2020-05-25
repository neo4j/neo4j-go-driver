package main

import (
	"bufio"
	"fmt"
)

type streamLogger struct {
	wr *bufio.Writer
}

func (l *streamLogger) ErrorEnabled() bool {
	return true
}

func (l *streamLogger) WarningEnabled() bool {
	return true
}

func (l *streamLogger) InfoEnabled() bool {
	return true
}

func (l *streamLogger) DebugEnabled() bool {
	return true
}

func (l *streamLogger) Errorf(message string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf(message, args...))
	l.wr.WriteString("\n")
}

func (l *streamLogger) Warningf(message string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf(message, args...))
	l.wr.WriteString("\n")
}

func (l *streamLogger) Infof(message string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf(message, args...))
	l.wr.WriteString("\n")
}

func (l *streamLogger) Debugf(message string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf(message, args...))
	l.wr.WriteString("\n")
}
