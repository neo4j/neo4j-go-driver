package main

import (
	"fmt"
)

// Converts received proxied "cypher" types to Go native types.
func cypherToNative(c interface{}) interface{} {
	m := c.(map[string]interface{})
	d := m["data"].(map[string]interface{})
	n := m["name"]
	switch n {
	case "CypherString":
		return d["value"].(string)
	case "CypherInt":
		return int64(d["value"].(float64))
	case "CypherBool":
		return d["value"].(bool)
	case "CypherFloat":
		return d["value"].(float64)
	case "CypherNull":
		return nil
	case "CypherList":
		lc := d["value"].([]interface{})
		ln := make([]interface{}, len(lc))
		for i, x := range lc {
			ln[i] = cypherToNative(x)
		}
		return ln
	}
	panic(fmt.Sprintf("Don't know how to convert %s to native", n))
}
