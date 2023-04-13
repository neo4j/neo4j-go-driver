package errorutil

import (
	"fmt"
)

type ReadRoutingTableError struct {
	Err    error
	Server string
}

func (e *ReadRoutingTableError) Error() string {
	if e.Err != nil || len(e.Server) > 0 {
		return fmt.Sprintf("Unable to retrieve routing table from %s: %s", e.Server, e.Err)
	}
	return "Unable to retrieve routing table, no router provided"
}
