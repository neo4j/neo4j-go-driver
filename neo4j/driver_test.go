package neo4j

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"
)

type runInTxOrNot interface {
	Run(cypher string, params map[string]interface{}) (Result, error)
}

func TestDriverIntegration(ot *testing.T) {
	makeDriver := func(t *testing.T) Driver {
		driver, err := NewDriver("bolt://localhost:7687", BasicAuth("neo4j", "pass", ""), func(c *Config) {
			c.MaxConnectionPoolSize = 2
			c.ConnectionAcquisitionTimeout = time.Second * 1
		})
		if err != nil {
			t.Fatalf("%s", err)
		}
		return driver
	}

	makeRandomNode := func(t *testing.T, r runInTxOrNot) map[string]interface{} {
		bid, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
		id := bid.Int64()
		params := map[string]interface{}{"now": id}
		res, err := r.Run("CREATE (n:Rand {val: $now})", params)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next() {
			t.Fatal("Should be no records for create")
		}
		return params
	}

	assertRandomNode := func(t *testing.T, r runInTxOrNot, params map[string]interface{}) {
		res, err := r.Run("MATCH (n:Rand) WHERE n.val =  $now RETURN n", params)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Next() {
			t.Fatal("No result")
		}
		rec := res.Record()
		if rec == nil {
			t.Fatal("No record")
		}
		n := rec.Values()[0].(Node)
		p := n.Props()
		if p["val"] != params["now"] {
			t.Fatal("Wrong value")
		}
		if res.Next() {
			t.Fatal("Should be no more records")
		}
		_, err = res.Summary()
		if err != nil {
			t.Fatal(err)
		}
	}

	assertNoRandomNode := func(t *testing.T, r runInTxOrNot, params map[string]interface{}) {
		res, err := r.Run("MATCH (n:Rand) WHERE n.val =  $now RETURN n", params)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next() {
			t.Fatalf("Should be no result: %+v", res.Record())
		}
	}

	ot.Run("Run auto-commit", func(t *testing.T) {
		driver := makeDriver(t)
		//defer driver.Close()

		// Create a "random" node in auto commit
		sess1, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		defer sess1.Close()

		params := makeRandomNode(t, sess1)

		// Query for above node in another session without closing the first session
		sess2, err := driver.Session(AccessModeRead)
		if err != nil {
			t.Fatal(err)
		}
		assertRandomNode(t, sess2, params)
	})

	ot.Run("Run explicit commit", func(t *testing.T) {
		driver := makeDriver(t)
		// Create a "random" node in a tx in session 1
		sess1, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		defer sess1.Close()
		tx1, err := sess1.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}
		params := makeRandomNode(t, tx1)

		// Query for above node in another session before commit
		sess2, err := driver.Session(AccessModeRead)
		if err != nil {
			t.Fatal(err)
		}
		assertNoRandomNode(t, sess2, params)

		// Commit in first session
		err = tx1.Commit()
		if err != nil {
			t.Fatal(err)
		}

		// Now the node should be visible in second session
		assertRandomNode(t, sess2, params)
	})

	ot.Run("Run rollback", func(t *testing.T) {
		driver := makeDriver(t)
		// Create a "random" node in a tx in session 1
		sess1, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		defer sess1.Close()
		tx1, err := sess1.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}
		params := makeRandomNode(t, tx1)
		err = tx1.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		// Query for above node in another session
		sess2, err := driver.Session(AccessModeRead)
		if err != nil {
			t.Fatal(err)
		}
		assertNoRandomNode(t, sess2, params)
	})
}

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

/*
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
	params := map[string]interface{}{"now": time.Now().Unix()}
	res, err = sess.Run("CREATE (n:Now {val: $now})", params)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	// Test transaction rollback
	fmt.Println("In tx rollback")
	tx, err := sess.BeginTransaction()
	if err != nil {
		t.Fatalf("Begin transaction failed: %s", err)
	}
	params = map[string]interface{}{"now": time.Now().Unix()}
	res, err = tx.Run("CREATE (n:Trans {val: $now})", params)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	// Try to fetch the rollbacked node
	res, err = sess.Run("MATCH (n:Trans) WHERE n.val =  $now RETURN n", params)
	dumpResult(res)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}

	fmt.Println("In tx commit")
	tx, err = sess.BeginTransaction()
	if err != nil {
		t.Fatalf("Begin transaction failed: %s", err)
	}
	params = map[string]interface{}{"now": time.Now().Unix()}
	res, err = tx.Run("CREATE (n:Trans {val: $now})", params)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	err = dumpResult(res)
	if err != nil {
		t.Errorf("Got err: %s", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Errorf("Got err: %s", err)
	}

	// Try to fetch the rollbacked node
	res, err = sess.Run("MATCH (n:Trans) WHERE n.val =  $now RETURN n", params)
	dumpResult(res)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}

	err = sess.Close()
	if err != nil {
		t.Errorf("Failed to close: %s", err)
	}

	t.Fail()
}
*/
