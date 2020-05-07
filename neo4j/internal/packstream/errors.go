package packstream

import (
	"fmt"
	"reflect"
)

type OverflowError struct {
	msg string
}

func (e *OverflowError) Error() string {
	return e.msg
}

type UnsupportedTypeError struct {
	t reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Packing of type '%s' is not supported", e.t.String())
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
