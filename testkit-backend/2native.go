package main

import (
	"fmt"
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c interface{}) interface{} {
	m := c.(map[string]interface{})
	d := m["data"].(map[string]interface{})
	switch m["name"] {
	case "CypherString":
		return d["value"].(string)
	case "CypherInt":
		return int64(d["value"].(float64))
	}
	panic(fmt.Sprintf("Don't know how to convert cypher %s to native", m))
}
