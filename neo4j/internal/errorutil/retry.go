package errorutil

import "fmt"

type CommitFailedDeadError struct {
	Inner error
}

func (e *CommitFailedDeadError) Error() string {
	return fmt.Sprintf("Connection lost during commit: %s", e.Inner)
}

// TransactionExecutionLimit error indicates that a retryable transaction has
// failed due to reaching a limit like a timeout or maximum number of attempts.
type TransactionExecutionLimit struct {
	Cause  string
	Errors []error
}

func (e *TransactionExecutionLimit) Error() string {
	cause := e.Cause
	var err error
	l := len(e.Errors)

	if l > 0 {
		err = e.Errors[l-1]
	}
	return fmt.Sprintf("TransactionExecutionLimit: %s after %d attempts, last error: %s", cause, len(e.Errors), err)
}
