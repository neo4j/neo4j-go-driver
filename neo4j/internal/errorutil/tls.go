package errorutil

// TlsError encapsulates all errors related to TLS connection creation
// This is needed since the tls package does not provide a common error type
// à la net.Error, and a common type is needed to properly classify the error
// for Testkit
type TlsError struct {
	Inner error
}

func (e *TlsError) Error() string {
	return e.Inner.Error()
}
