package neo4j_go_driver

type Error struct {
	code    string
	message string
}

func (error *Error) Code() string {
	return error.code
}

func (error *Error) Message() string {
	return error.message
}

func (error *Error) Error() string {
	return error.message
}
