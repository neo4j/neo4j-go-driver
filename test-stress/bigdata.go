/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func nCopiesInt(n int, x int) []int {
	copies := make([]int, n)
	for i := range copies {
		copies[i] = x
	}
	return copies
}

func nCopiesFloat(n int, x float32) []float32 {
	copies := make([]float32, n)
	for i := range copies {
		copies[i] = x
	}
	return copies
}

func nCopiesBool(n int, x bool) []bool {
	copies := make([]bool, n)
	for i := range copies {
		copies[i] = x
	}
	return copies
}

func assertSliceContains(slice []string, s string) {
	for _, x := range slice {
		if x == s {
			return
		}
	}

	panic("Slice doesn't contain: " + s)
}

func assertStringsEqual(s1, s2 string) {
	if s1 != s2 {
		panic("Strings differ")
	}
}

func assertInt64Slice(ints1 []int, ints2 []interface{}) {
	if len(ints1) != len(ints2) {
		panic("Int slices length differ")
	}
	for i, x1 := range ints1 {
		x2 := ints2[i].(int64)
		if x1 != int(x2) {
			panic("Int slices values differ")
		}
	}
}

func assertFloatSlice(floats1 []float32, floats2 []interface{}) {
	if len(floats1) != len(floats2) {
		panic("Float slices length differ")
	}
	for i, x1 := range floats1 {
		x2 := floats2[i].(float64)
		if x1 != float32(x2) {
			panic("Float slices values differ")
		}
	}
}

func assertBooleansSlice(bools1 []bool, bools2 []interface{}) {
	if len(bools1) != len(bools2) {
		panic("Bool slices length differ")
	}
	for i, x1 := range bools1 {
		x2 := bools2[i].(bool)
		if x1 != x2 {
			panic("Bool slices values differ")
		}
	}
}

func runBigDataThing(driver neo4j.Driver) {
	const batchSize = 10000
	const nodeCount = 30000

	// Write nodes
	session := driver.NewSession(neo4j.SessionConfig{})
	for index := 0; index < nodeCount; {
		_, err := session.WriteTransaction(context.TODO(), func(tx neo4j.Transaction) (interface{}, error) {
			batch := 0
			for index < nodeCount && batch < batchSize {
				result, err := tx.Run("CREATE (n:Test:Node) SET n = $props", map[string]interface{}{
					"props": map[string]interface{}{
						"index":          index,
						"name":           fmt.Sprintf("name-%d", index),
						"surname":        fmt.Sprintf("surname-%d", index),
						"long-indices":   nCopiesInt(10, index),
						"double-indices": nCopiesFloat(10, float32(index)),
						"booleans":       nCopiesBool(10, (index%2) == 0),
					},
				})
				if err != nil {
					return nil, err
				}
				_, err = result.Consume()
				if err != nil {
					return nil, err
				}
				index++
				batch++
			}
			return nil, nil
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("Wrote big data %d of %d\n", index, nodeCount)
	}

	// Read nodes
	seen := 0
	_, err := session.ReadTransaction(context.TODO(), func(tx neo4j.Transaction) (interface{}, error) {
		seen = 0
		result, err := tx.Run("MATCH (n:Node) RETURN n", nil)
		if err != nil {
			return nil, err
		}
		var rec *neo4j.Record
		for result.NextRecord(&rec) {
			node := rec.Values[0].(neo4j.Node)
			// Validate the node
			assertSliceContains(node.Labels, "Node")
			assertSliceContains(node.Labels, "Test")
			if len(node.Labels) != 2 {
				panic("Node doesn't have correct size")
			}
			index := node.Props["index"].(int64)
			assertStringsEqual(fmt.Sprintf("name-%d", index), node.Props["name"].(string))
			assertStringsEqual(fmt.Sprintf("surname-%d", index), node.Props["surname"].(string))
			assertInt64Slice(nCopiesInt(10, int(index)), node.Props["long-indices"].([]interface{}))
			assertFloatSlice(nCopiesFloat(10, float32(index)), node.Props["double-indices"].([]interface{}))
			assertBooleansSlice(nCopiesBool(10, (index%2) == 0), node.Props["booleans"].([]interface{}))
			seen++
		}
		return nil, result.Err()
	})
	if err != nil {
		panic(err)
	}
	if seen < nodeCount {
		panic("Too few nodes")
	}
	fmt.Println("Verified big data nodes")
}
