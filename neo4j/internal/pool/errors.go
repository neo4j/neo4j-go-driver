package pool

import (
	"fmt"
)

type PoolTimeout struct {
	err     error
	servers []string
}

func (e *PoolTimeout) Error() string {
	return fmt.Sprintf("Timeout while waiting for connection to any of [%s]: %s", e.servers, e.err)
}

type PoolFull struct {
	servers []string
}

func (e *PoolFull) Error() string {
	return fmt.Sprintf("No idle connections on any of [%s]", e.servers)
}

type PoolClosed struct {
}

func (e *PoolClosed) Error() string {
	return "Pool closed"
}
