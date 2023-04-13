package errorutil

import (
	"context"
	"fmt"
	"time"
)

const InvalidTransactionError = "invalid transaction handle"

type ConnectionReadTimeout struct {
	UserContext context.Context
	ReadTimeout time.Duration
	Err         error
}

func (crt *ConnectionReadTimeout) Error() string {
	userDeadline := "N/A"
	if deadline, ok := crt.UserContext.Deadline(); ok {
		userDeadline = deadline.String()
	}
	return fmt.Sprintf(
		"Timeout while reading from connection [server-side timeout hint: %s, user-provided context deadline: %s]: %s",
		crt.ReadTimeout.String(),
		userDeadline,
		crt.Err)
}

type ConnectionWriteTimeout struct {
	UserContext context.Context
	Err         error
}

func (cwt *ConnectionWriteTimeout) Error() string {
	userDeadline := "N/A"
	if deadline, ok := cwt.UserContext.Deadline(); ok {
		userDeadline = deadline.String()
	}
	return fmt.Sprintf("Timeout while writing to connection [user-provided context deadline: %s]: %s", userDeadline, cwt.Err)
}

type ConnectionReadCanceled struct {
	Err error
}

func (crc *ConnectionReadCanceled) Error() string {
	return fmt.Sprintf("Reading from connection has been canceled: %s", crc.Err)
}

type ConnectionWriteCanceled struct {
	Err error
}

func (cwc *ConnectionWriteCanceled) Error() string {
	return fmt.Sprintf("Writing to connection has been canceled: %s", cwc.Err)
}

type timeout interface {
	Timeout() bool
}

func IsTimeoutError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	timeoutErr, ok := err.(timeout)
	return ok && timeoutErr.Timeout()
}
