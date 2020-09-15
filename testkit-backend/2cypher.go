package main

import (
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// Converts native type to proxied "cypher" to be sent to frontend.
func nativeToCypher(v interface{}) map[string]interface{} {
	if v == nil {
		return map[string]interface{}{"name": "CypherNull", "data": nil}
	}
	switch x := v.(type) {
	case int64:
		return valueResponse("CypherInt", x)
	case string:
		return valueResponse("CypherString", x)
	case bool:
		return valueResponse("CypherBool", x)
	case float64:
		return valueResponse("CypherFloat", x)
	case []interface{}:
		values := make([]interface{}, len(x))
		for i, y := range x {
			values[i] = nativeToCypher(y)
		}
		return valueResponse("CypherList", values)
	case []string:
		values := make([]interface{}, len(x))
		for i, y := range x {
			values[i] = nativeToCypher(y)
		}
		return valueResponse("CypherList", values)
	case map[string]interface{}:
		values := make(map[string]interface{})
		for k, v := range x {
			values[k] = nativeToCypher(v)
		}
		return valueResponse("CypherMap", values)
	case neo4j.Node:
		return map[string]interface{}{
			"name": "Node", "data": map[string]interface{}{
				"id":     nativeToCypher(x.Id),
				"labels": nativeToCypher(x.Labels),
				"props":  nativeToCypher(x.Props),
			}}
	}
	panic(fmt.Sprintf("Don't know how to patch %T", v))
}

// Helper to wrap proxied "cypher" value into a response
func valueResponse(name string, v interface{}) map[string]interface{} {
	return map[string]interface{}{"name": name, "data": map[string]interface{}{"value": v}}
}
