package neo4j

import (
	"fmt"
	"math/rand"
	"testing"
)

func dumpResult(res Result) error {
	for res.Next() {
		rec := res.Record()
		fmt.Println("RECORD")
		keys := rec.Keys()
		for i, x := range rec.Values() {
			fmt.Printf("    %s: %+v\n", keys[i], x)
		}
	}
	err := res.Err()
	if err != nil {
		return err
	}

	sum, err := res.Summary()
	if err != nil {
		return err
	}
	server := sum.Server()
	fmt.Printf("Server version: %s\n", server.Version())

	stmnt := sum.Statement()
	fmt.Printf("Cypher: %s\nParams: %+v\n", stmnt.Text(), stmnt.Params())

	return nil
}

func TestOnRealServer(t *testing.T) {
	driver, err := NewDriver("bolt://localhost:7687", BasicAuth("neo4j", "pass", ""))
	if err != nil {
		t.Fatalf("%s", err)
	}

	sess, err := driver.Session()
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Run query
	fmt.Println("Match")
	res, err := sess.Run("MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	// Run create statement
	fmt.Println("Create")
	params := map[string]interface{}{"rnd": rand.Int()}
	res, err = sess.Run("CREATE (n:Random {val: $rnd})", params)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	err = sess.Close()
	if err != nil {
		t.Errorf("Failed to close: %s", err)
	}

	t.Fail()
}
