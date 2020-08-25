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
func (l *streamLog) Errorf(name string, id string, msg string, args ...interface{}) {
	l.writeLine(fmt.Sprintf("[%s %s] %s", name, id, fmt.Sprintf(msg, args...)))
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
