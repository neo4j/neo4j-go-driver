package main

import (
	"bufio"
	"fmt"
)

type streamLog struct {
	wr *bufio.Writer
}

func (l *streamLog) Error(name string, id string, err error) {
	l.wr.WriteString(fmt.Sprintf("[%s %s] %s\n", name, id, err))
}
func (l *streamLog) Errorf(name string, id string, msg string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf("[%s %s] %s\n", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Warnf(name string, id string, msg string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf("[%s %s] %s\n", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Infof(name string, id string, msg string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf("[%s %s] %s\n", name, id, fmt.Sprintf(msg, args...)))
}
func (l *streamLog) Debugf(name string, id string, msg string, args ...interface{}) {
	l.wr.WriteString(fmt.Sprintf("[%s %s] %s\n", name, id, fmt.Sprintf(msg, args...)))
}

/*
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
*/
