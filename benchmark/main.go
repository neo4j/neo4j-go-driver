package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	neo4j18 "github.com/neo4j/neo4j-go-driver/neo4j"
	neo4j "github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func getSetup(driver neo4j.Driver) *neo4j.Node {
	// Check if setup already built
	sess := driver.NewSession(neo4j.SessionConfig{})
	defer sess.Close()

	result, err := sess.Run("MATCH (s:Setup) RETURN s", nil)
	if err != nil {
		panic(err)
	}
	records, err := result.Collect()
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

func buildSetup(driver neo4j.Driver, setup *neo4j.Node) {

	sess := driver.NewSession(neo4j.SessionConfig{})
	defer sess.Close()

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
			_, err := sess.Run("CREATE (n:IterMxL) SET n = $nums RETURN n", map[string]interface{}{"nums": nums})
			if err != nil {
				panic(err)
			}
		}
		sess.Run("MERGE (s:Setup) SET s.iterMxL = true", nil)
	}
}

func iterMxL(driver neo4j.Driver) {
	sess := driver.NewSession(neo4j.SessionConfig{})
	defer sess.Close()

	result, err := sess.Run("MATCH (n:IterMxL) RETURN n", nil)
	if err != nil {
		panic(err)
	}

	num := 0
	var record *neo4j.Record
	for result.NextRecord(&record) {
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

func buildParamsLMap() map[string]interface{} {
	m := map[string]interface{}{}
	// Bunch of ints
	for i := 0; i < 500; i++ {
		m[fmt.Sprintf("i%d", i)] = i * i
	}
	return m
}

func params(driver neo4j.Driver, m map[string]interface{}, n int) {
	// Use same session for all of n, not part of measurement
	session := driver.NewSession(neo4j.SessionConfig{})
	for i := 0; i < n; i++ {
		_, err := session.Run("RETURN 0", m)
		if err != nil {
			panic(err)
		}
	}
	session.Close()
}

func params18(driver neo4j18.Driver, m map[string]interface{}, n int) {
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
func getS(driver neo4j.Driver, n int) {
	for i := 0; i < n; i++ {
		session := driver.NewSession(neo4j.SessionConfig{})
		x, _ := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			res, err := tx.Run("RETURN $i", map[string]interface{}{"i": i})
			if err != nil {
				panic(err)
			}
			var rec *neo4j.Record
			if !res.NextRecord(&rec) {
				panic("no record")
			}
			return int(rec.Values[0].(int64)), nil
		})
		if x.(int) != i {
			panic("!= i")
		}
		session.Close()
	}
}

func getS18(driver neo4j18.Driver, n int) {
	for i := 0; i < n; i++ {
		session, err := driver.NewSession(neo4j18.SessionConfig{})
		if err != nil {
			panic(err)
		}
		x, _ := session.ReadTransaction(func(tx neo4j18.Transaction) (interface{}, error) {
			res, err := tx.Run("RETURN $i", map[string]interface{}{"i": i})
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

// Run with bolt://localhost:7687 user pass
func main() {
	driver, err := neo4j.NewDriver(os.Args[1], neo4j.BasicAuth(os.Args[2], os.Args[3], ""),
	func(c *neo4j.Config) {
		//c.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
	})
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
	buildSetup(driver, getSetup(driver))

	iterMxL(driver) // Warm up
	start := time.Now()
	iterMxL(driver)
	duration := time.Since(start)
	iterMxL18(driver18) // Warm up
	start18 := time.Now()
	iterMxL18(driver18)
	duration18 := time.Since(start18)
	fmt.Printf("iterMxL: %.2f\n", float64(duration)/float64(duration18))

	getS(driver, 10) // Warm up
	start = time.Now()
	getS(driver, 1000)
	duration = time.Since(start)
	getS18(driver18, 10) // Warm up
	start18 = time.Now()
	getS18(driver18, 1000)
	duration18 = time.Since(start18)
	fmt.Printf("getS: %.2f\n", float64(duration)/float64(duration18))

	m := buildParamsLMap()
	params(driver, m, 10) // Warm up
	start = time.Now()
	params(driver, m, 1000)
	duration = time.Since(start)
	params18(driver18, m, 10) // Warm up
	start18 = time.Now()
	params(driver, m, 1000)
	duration18 = time.Since(start18)
	fmt.Printf("paramsL: %.2f\n", float64(duration)/float64(duration18))

}
