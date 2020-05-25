package main

import (
	"fmt"
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c interface{}) interface{} {
	m := c.(map[string]interface{})
	switch m["name"] {
	case "CypherString":
		return m["data"].(map[string]interface{})["value"].(string)
	}
	panic(fmt.Sprintf("Don't know how to convert cypher %s to native", m))
}
