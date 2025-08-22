/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Benchmark tool that uses driver 1.8 as baseline.
// The tool requires a running Neo4j instance to connect to.
// Run with: go run main.go neo4j://localhost user pass
package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	neo4j18 "github.com/neo4j/neo4j-go-driver/neo4j"
	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func getSetup(ctx context.Context, driver neo4j.Driver) *neo4j.Node {
	// Check if setup already built
	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer sess.Close(ctx)

	result, err := sess.Run(ctx, "MATCH (s:Setup) RETURN s", nil)
	if err != nil {
		panic(err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		panic(err)
	}
	switch len(records) {
	case 0:
		return nil
	case 1:
		node := records[0].Values[0].(neo4j.Node)
		return &node
	default:
		panic("More than one setup")
	}
}

const (
	iterMxLNUMRECS  = 1000
	iterMxLNUMPROPS = 500
)

func getBoolProp(node *neo4j.Node, name string, dflt bool) bool {
	if node == nil {
		return dflt
	}
	b, ok := node.Props[name].(bool)
	if !ok {
		return dflt
	}
	return b
}

func buildSetup(ctx context.Context, driver neo4j.Driver, setup *neo4j.Node) {
	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer sess.Close(ctx)

	if !getBoolProp(setup, "iterMxL", false) {
		fmt.Println("Building iterMxL")

		// Create n nodes with a bunch of int properties
		for n := 0; n < iterMxLNUMRECS; n++ {
			nums := make(map[string]int)
			x := n
			for i := 0; i < iterMxLNUMPROPS; i++ {
				x = x + i*i
				nums[strconv.Itoa(i)] = x
			}
			_, err := sess.Run(ctx, "CREATE (n:IterMxL) SET n = $nums RETURN n", map[string]any{"nums": nums})
			if err != nil {
				panic(err)
			}
		}
		sess.Run(ctx, "MERGE (s:Setup) SET s.iterMxL = true", nil)
	}
}

func iterMxL(ctx context.Context, driver neo4j.Driver) {
	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer sess.Close(ctx)

	result, err := sess.Run(ctx, "MATCH (n:IterMxL) RETURN n", nil)
	if err != nil {
		panic(err)
	}

	num := 0
	var record *neo4j.Record
	for result.NextRecord(ctx, &record) {
		num++
		node := record.Values[0].(neo4j.Node)
		if len(node.Props) != iterMxLNUMPROPS {
			panic("Num props differ")
		}
	}
	if num != iterMxLNUMRECS {
		panic(fmt.Sprintf("Num records differ: %d vs %d", num, iterMxLNUMRECS))
	}
}

func iterMxL18(driver neo4j18.Driver) {
	sess, err := driver.NewSession(neo4j18.SessionConfig{})
	if err != nil {
		panic(err)
	}
	defer sess.Close()

	result, err := sess.Run("MATCH (n:IterMxL) RETURN n", nil)
	if err != nil {
		panic(err)
	}

	num := 0
	for result.Next() {
		num++
		node := result.Record().Values()[0].(neo4j18.Node)
		if len(node.Props()) != iterMxLNUMPROPS {
			panic("Too few props")
		}
	}
	if num != iterMxLNUMRECS {
		panic("Too few records")
	}
}

func buildParamsLMap() map[string]any {
	m := map[string]any{}
	// Bunch of ints
	for i := 0; i < 500; i++ {
		m[fmt.Sprintf("i%d", i)] = i * i
	}
	return m
}

func params(ctx context.Context, driver neo4j.Driver, m map[string]any, n int) {
	// Use same session for all of n, not part of measurement
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	for i := 0; i < n; i++ {
		_, err := session.Run(ctx, "RETURN 0", m)
		if err != nil {
			panic(err)
		}
	}
	session.Close(ctx)
}

func params18(driver neo4j18.Driver, m map[string]any, n int) {
	// Use same session for all of n, not part of measurement
	session, _ := driver.NewSession(neo4j18.SessionConfig{})
	for i := 0; i < n; i++ {
		_, err := session.Run("RETURN 0", m)
		if err != nil {
			panic(err)
		}
	}
	session.Close()
}

// Measures time to get a single result using tx function
// Include session creation in measurement
func getS(ctx context.Context, driver neo4j.Driver, n int) {
	for i := 0; i < n; i++ {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		x, _ := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			res, err := tx.Run(ctx, "RETURN $i", map[string]any{"i": i})
			if err != nil {
				panic(err)
			}
			var rec *neo4j.Record
			if !res.NextRecord(ctx, &rec) {
				panic("no record")
			}
			return int(rec.Values[0].(int64)), nil
		})
		if x.(int) != i {
			panic("!= i")
		}
		session.Close(ctx)
	}
}

func getS18(driver neo4j18.Driver, n int) {
	for i := 0; i < n; i++ {
		session, err := driver.NewSession(neo4j18.SessionConfig{})
		if err != nil {
			panic(err)
		}
		x, _ := session.ReadTransaction(func(tx neo4j18.Transaction) (any, error) {
			res, err := tx.Run("RETURN $i", map[string]any{"i": i})
			if err != nil {
				panic(err)
			}
			if !res.Next() {
				panic("no record")
			}
			rec := res.Record()
			return int(rec.Values()[0].(int64)), nil
		})
		if x.(int) != i {
			panic("!= i")
		}
		session.Close()
	}
}

type memDiff struct {
	Mallocs    uint64
	TotalAlloc uint64
}

func perf(warmup, measure func()) (time.Duration, memDiff) {
	// Run a warm up first, typically reduced
	warmup()

	// Collect all garbage before and retrieve baseline mem stats
	runtime.GC()
	memBefore := runtime.MemStats{}
	runtime.ReadMemStats(&memBefore)
	start := time.Now()

	// Perform the measurement
	// Include garbage collection in the measurement
	measure()
	runtime.GC()

	dur := time.Since(start)
	memAfter := runtime.MemStats{}
	runtime.ReadMemStats(&memAfter)

	mem := memDiff{
		Mallocs:    memAfter.Mallocs - memBefore.Mallocs,
		TotalAlloc: memAfter.TotalAlloc - memBefore.TotalAlloc,
	}
	return dur, mem
}

// Run with bolt://localhost:7687 user pass
func main() {
	ctx := context.Background()

	driver, err := neo4j.NewDriver(os.Args[1], neo4j.BasicAuth(os.Args[2], os.Args[3], ""))
	if err != nil {
		panic(err)
	}
	driver18, err := neo4j18.NewDriver(os.Args[1], neo4j18.BasicAuth(os.Args[2], os.Args[3], ""), func(conf *neo4j18.Config) {
		conf.Encrypted = false
	})
	if err != nil {
		panic(err)
	}

	// Build the setup if needed
	buildSetup(ctx, driver, getSetup(ctx, driver))

	fmt.Printf("%-15v %-6v %-5v %-5v\n", "Benchmark", "Dur", "Mal", "Tal")
	printRes := func(name string, dur, dur18 time.Duration, mem, mem18 memDiff) {
		fmt.Printf("%-15v %.2f   %.2f  %.2f\n", name,
			float64(dur)/float64(dur18),
			float64(mem.Mallocs)/float64(mem18.Mallocs),
			float64(mem.TotalAlloc)/float64(mem18.TotalAlloc))
	}

	dur, mem := perf(func() { iterMxL(ctx, driver) }, func() { iterMxL(ctx, driver) })
	dur18, mem18 := perf(func() { iterMxL18(driver18) }, func() { iterMxL18(driver18) })
	printRes("iterMxL", dur, dur18, mem, mem18)

	dur, mem = perf(func() { getS(ctx, driver, 10) }, func() { getS(ctx, driver, 1000) })
	dur18, mem18 = perf(func() { getS18(driver18, 10) }, func() { getS18(driver18, 1000) })
	printRes("getS", dur, dur18, mem, mem18)

	m := buildParamsLMap()
	dur, mem = perf(func() { params(ctx, driver, m, 10) }, func() { params(ctx, driver, m, 1000) })
	dur18, mem18 = perf(func() { params18(driver18, m, 10) }, func() { params18(driver18, m, 1000) })
	printRes("paramsL", dur, dur18, mem, mem18)
}
