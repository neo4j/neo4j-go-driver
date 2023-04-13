package errorutil

import "fmt"

type PoolTimeout struct {
	Err     error
	Servers []string
}

func (e *PoolTimeout) Error() string {
	return fmt.Sprintf("Timeout while waiting for connection to any of [%s]: %s", e.Servers, e.Err)
}

type PoolFull struct {
	Servers []string
}

func (e *PoolFull) Error() string {
	return fmt.Sprintf("No idle connections on any of [%s]", e.Servers)
}

type PoolClosed struct {
}

func (e *PoolClosed) Error() string {
	return "Pool closed"
}
