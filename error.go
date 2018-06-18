package neo4j_go_driver

import "fmt"

type Failure struct {
	code    string
	message string
}

func (failure *Failure) Code() string {
	return failure.code
}

func (failure *Failure) Message() string {
	return failure.message
}

func (failure *Failure) Error() string {
	return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

func IsDatabaseFailure(err error) bool {
	_, ok := err.(*Failure)
	return ok
}