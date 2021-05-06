package log

import (
	"fmt"
	"os"
	"time"
)

type MessageSource int

const (
	BoltClient MessageSource = iota
	BoltServer
)

func (src MessageSource) String() string {
	switch src {
	case BoltClient:
		return "C"
	case BoltServer:
		return "S"
	default:
		return ""
	}
}

type BoltLogger interface {
	LogMessage(src MessageSource, context string, msg string, args ...interface{})
}

type ConsoleBoltLogger struct {
}

func (cbl *ConsoleBoltLogger) LogMessage(src MessageSource, id, msg string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stdout, "%s   BOLT  %s%s: %s\n", time.Now().Format(timeFormat), formatId(id), src, fmt.Sprintf(msg, args...))
}

func formatId(id string) string {
	if id == "" {
		return ""
	}
	return fmt.Sprintf("[%s] ", id)
}
