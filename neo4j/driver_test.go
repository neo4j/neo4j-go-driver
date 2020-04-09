package neo4j

/*
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
		t.Helper()
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

	ot.Run("Read node", func(t *testing.T) {
		driver := makeDriver(t)
		sess, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		x, err := sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
			res, err := tx.Run("CREATE (p:Person:Swedish {name: 'Nisse', age: 19, city: 'Lund' }) RETURN p", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = res.Summary()
			if err != nil {
				t.Fatal(err)
			}
			res.Next()
			return res.Record().Values()[0], nil
		})
		if err != nil {
			t.Fatal(err)
		}
		n := x.(Node)
		if n.Id() == 0 {
			t.Error("Node id can not be 0")
		}
		labels := n.Labels()
		if len(labels) != 2 {
			t.Error("Should be two labels")
		}
		props := n.Props()
		if len(props) != 3 {
			t.Error("Should be three props")
		}
	})

	ot.Run("Read relationship", func(t *testing.T) {
		driver := makeDriver(t)
		sess, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
			res, err := tx.Run("CREATE (a:Person{name: 'p1'})-[r:Knows {val: 0.5}]->(b:Person{name:'p2'}) RETURN a, b, r", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = res.Summary()
			if err != nil {
				t.Fatal(err)
			}
			res.Next()
			vals := res.Record().Values()
			n1 := vals[0].(Node)
			n2 := vals[1].(Node)
			r := vals[2].(Relationship)
			if r.StartId() != n1.Id() {
				t.Error("Wrong start")
			}
			if r.EndId() != n2.Id() {
				t.Error("Wrong end")
			}
			return nil, nil
		})
	})

	ot.Run("Read path", func(t *testing.T) {
		driver := makeDriver(t)
		sess, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}

		sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
			res, err := tx.Run(
				"CREATE p =(andy { name:'Andy' })-[:WORKS_AT]->(neo)<-[:WORKS_AT]-(michael { name: 'Michael' }) RETURN p", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = res.Summary()
			if err != nil {
				t.Fatal(err)
			}
			res.Next()
			vals := res.Record().Values()
			p := vals[0].(Path)

			nodes := p.Nodes()
			andy := nodes[0]
			neo := nodes[1]
			michael := nodes[2]

			rels := p.Relationships()
			andy2Neo := rels[0]
			michael2Neo := rels[1]

			if andy2Neo.StartId() != andy.Id() {
				t.Error("Wrong start of andy to neo")
			}
			if andy2Neo.EndId() != neo.Id() {
				t.Error("Wrong end of andy to neo")
			}
			if michael2Neo.StartId() != michael.Id() {
				t.Error("Wrong start of michael to neo")
			}

			return nil, nil
		})
	})

	ot.Run("Read/write spatial", func(t *testing.T) {
		driver := makeDriver(t)
		sess, err := driver.Session(AccessModeWrite)
		if err != nil {
			t.Fatal(err)
		}

		sess.ReadTransaction(func(tx Transaction) (interface{}, error) {
			res, err := tx.Run(
				"WITH point({ latitude:toFloat('13.43'), longitude:toFloat('56.21')}) AS p2, point({ x:toFloat('13.10'), y:toFloat('56.41'), z:toFloat(7)}) AS p3 RETURN p2, p3", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = res.Summary()
			if err != nil {
				t.Fatal(err)
			}
			res.Next()
			vals := res.Record().Values()
			p2 := vals[0].(*Point)
			p3 := vals[1].(*Point)

			if p2.SrId() != 4326 {
				t.Error("2D point has wrong spatial ref id")
			}
			if p3.SrId() != 9157 {
				t.Error("3D point has wrong spatial ref id")
			}
			if p3.Z() != float64(7) {
				t.Error("3D point has wrong Z")
			}

			return nil, nil
		})
		sess.WriteTransaction(func(tx Transaction) (interface{}, error) {
			res, err := tx.Run(
				"CREATE (c:City {coord: $coord, name: $name, top: $top}) RETURN c",
				map[string]interface{}{"name": "Lund", "coord": NewPoint2D(4326, 12, 199), "top": NewPoint3D(4979, 1, 2, 3)})
			if err != nil {
				t.Fatal(err)
			}
			_, err = res.Summary()
			if err != nil {
				t.Fatal(err)
			}
			res.Next()
			vals := res.Record().Values()
			n := vals[0].(Node)
			p2 := n.Props()["coord"].(*Point)
			if p2.Y() != float64(199) {
				t.Error("Point 2 has wrong Y")
			}
			p3 := n.Props()["top"].(*Point)
			if p3.Z() != float64(3) {
				t.Error("Point 3 has wrong Z")
			}
			return nil, nil
		})
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
*/

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
