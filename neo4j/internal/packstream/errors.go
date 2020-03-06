package packstream

import (
	"fmt"
)

type OverflowError struct {
	msg string
}

func (e *OverflowError) Error() string {
	return e.msg
}

type UnsupportedTypeError struct {
	msg string
}

func (e *UnsupportedTypeError) Error() string {
	return e.msg
}

type IoError struct {
	inner error
}

func (e *IoError) Error() string {
	return fmt.Sprintf("IO error: %s", e.inner)
}

type IllegalFormatError struct {
	msg string
}

func (e *IllegalFormatError) Error() string {
	return e.msg
}

