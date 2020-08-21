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
