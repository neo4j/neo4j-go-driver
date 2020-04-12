/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package test_integration

import (
	"crypto/rand"
	"math"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"
)

// Tests the specification of the internal/connection API
func TestConnectionConformance(ot *testing.T) {
	server, err := control.EnsureSingleInstance()
	if err != nil {
		ot.Fatal(err)
	}

	uri := server.BoltURI()
	parsedUri, err := url.Parse(uri)
	if err != nil {
		ot.Fatal(err)
	}

	conn, err := net.Dial("tcp", parsedUri.Host)
	if err != nil {
		ot.Fatal(err)
	}

	authMap := map[string]interface{}{
		"scheme":      "basic",
		"principal":   server.Username(),
		"credentials": server.Password(),
	}

	boltConn, err := bolt.Connect(parsedUri.Host, conn, authMap)
	if err != nil {
		ot.Fatal(err)
	}

	randInt := func() int64 {
		bid, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
		return bid.Int64()
	}

	// All of these tests should leave the connection in a good state without the need
	// for a reset. All tests share the same connection.
	cases := []struct {
		name string
		fun  func(*testing.T, connection.Connection)
	}{
		{
			// Leaves the connection in perfect state after creating and cosuming all records
			name: "Run autocommit, full consume",
			fun: func(t *testing.T, c connection.Connection) {
				s, err := c.Run("CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": randInt()})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err := c.Next(s.Handle)
				if rec == nil || err != nil || sum != nil {
					t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec != nil || err != nil || sum == nil {
					t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
				}
			},
		},
		{
			// Let the connection consume the result before next autocommit (also leaves the
			// connection in a streaming state)
			name: "Run autocommit twice, no consume",
			fun: func(t *testing.T, c connection.Connection) {
				_, err := c.Run("CREATE (n:Rand {val: $r})", map[string]interface{}{"r": randInt()})
				if err != nil {
					t.Fatal(err)
				}
				_, err = c.Run("CREATE (n:Rand {val: $r})", map[string]interface{}{"r": randInt()})
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			// Consume everything returned from create before committing
			name: "Run explicit commit, full consume",
			fun: func(t *testing.T, c connection.Connection) {
				txHandle, err := c.TxBegin(connection.WriteMode, []string{}, 10*time.Minute, nil)
				if err != nil {
					t.Fatal(err)
				}
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err := c.Next(s.Handle)
				if rec == nil || err != nil || sum != nil {
					t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec != nil || err != nil || sum == nil {
					t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
				}
				err = c.TxCommit(txHandle)
				if err != nil {
					t.Fatal(err)
				}
				// Make sure it's commited
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec == nil || err != nil || sum != nil {
					t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
				}
				// Not everything consumed from the read check, but that is also fine
			},
		},
		{
			// Do not consume anything before commiting
			name: "Run explicit commit, no consume",
			fun: func(t *testing.T, c connection.Connection) {
				txHandle, err := c.TxBegin(connection.WriteMode, []string{}, 10*time.Minute, nil)
				if err != nil {
					t.Fatal(err)
				}
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				err = c.TxCommit(txHandle)
				if err != nil {
					t.Fatal(err)
				}
				// Make sure it's commited
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err := c.Next(s.Handle)
				if rec == nil || err != nil || sum != nil {
					t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
				}
				// Not everything consumed from the read check, but that is also fine
			},
		},
		{
			// Consume everything returned from create before rolling back
			name: "Run explicit rollback, full consume",
			fun: func(t *testing.T, c connection.Connection) {
				txHandle, err := c.TxBegin(connection.WriteMode, []string{}, 10*time.Minute, nil)
				if err != nil {
					t.Fatal(err)
				}
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err := c.Next(s.Handle)
				if rec == nil || err != nil || sum != nil {
					t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec != nil || err != nil || sum == nil {
					t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
				}
				err = c.TxRollback(txHandle)
				if err != nil {
					t.Fatal(err)
				}
				// Make sure it's rolled back
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec != nil || err != nil || sum == nil {
					t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
				}
			},
		},
		{
			// Do not consume anything before rolling back
			name: "Run explicit rollback, no consume",
			fun: func(t *testing.T, c connection.Connection) {
				txHandle, err := c.TxBegin(connection.WriteMode, []string{}, 10*time.Minute, nil)
				if err != nil {
					t.Fatal(err)
				}
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				err = c.TxRollback(txHandle)
				if err != nil {
					t.Fatal(err)
				}
				// Make sure it's commited
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err := c.Next(s.Handle)
				if rec != nil || err != nil || sum == nil {
					t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
				}
			},
		},
		{
			name: "Next without streaming",
			fun: func(t *testing.T, c connection.Connection) {
				rec, sum, err := c.Next(3)
				if rec != nil || err == nil || sum != nil {
					t.Fatalf("Should only be an error, %+v, %+v, %+v", rec, sum, err)
				}
			},
		},
		{
			name: "Next passed the summary",
			fun: func(t *testing.T, c connection.Connection) {
				s, err := boltConn.Run("RETURN datetime()", nil)
				if err != nil {
					t.Fatal(err)
				}
				rec, sum, err := c.Next(s.Handle)
				if rec == nil || err != nil || sum != nil {
					t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec != nil || err != nil || sum == nil {
					t.Fatalf("Should only be a summary, %+v, %+v, %+v", rec, sum, err)
				}
				rec, sum, err = c.Next(s.Handle)
				if rec != nil || err == nil || sum != nil {
					t.Fatalf("Should only be an error, %+v, %+v, %+v", rec, sum, err)
				}
			},
		},
		{
			name: "Run autocommit while in tx",
			fun: func(t *testing.T, c connection.Connection) {
				txHandle, err := c.TxBegin(connection.WriteMode, []string{}, 10*time.Minute, nil)
				if txHandle == nil || err != nil {
					t.Fatalf("Failed to begin tx: %s", err)
				}
				defer c.TxRollback(txHandle)
				s, err := c.Run("CREATE (n:Rand {val: $r})", map[string]interface{}{"r": randInt()})
				if s != nil || err == nil {
					t.Fatal("Should fail to run auto commit when in transaction")
				}
				// TODO: Assert type of error!
			},
		},
		{
			name: "Commit while not in tx",
			fun: func(t *testing.T, c connection.Connection) {
				err := c.TxCommit(1)
				if err == nil {
					t.Fatal("Should have failed")
				}
				// TODO: Assert type of error!
			},
		},
		{
			name: "Rollback while not in tx",
			fun: func(t *testing.T, c connection.Connection) {
				err := c.TxRollback(3)
				if err == nil {
					t.Fatal("Should have failed")
				}
				// TODO: Assert type of error!
			},
		},
	}
	// Run all above in sequence
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			c.fun(t, boltConn)
			if !boltConn.IsAlive() {
				t.Error("Connection died")
			}
		})
	}
	// Run some of the above at random
	randoms := make([]int, 25)
	for i := range randoms {
		randoms[i] = int(randInt() % int64(len(cases)))
	}
	for _, i := range randoms {
		c := cases[i]
		ot.Run("Random "+c.name, func(t *testing.T) {
			c.fun(t, boltConn)
			if !boltConn.IsAlive() {
				t.Error("Connection died")
			}
		})
	}

	// All of these tests should leave the connection in a good state after a reset but not
	// necessarily without it. All tests share the same connection.
	cases = []struct {
		name string
		fun  func(*testing.T, connection.Connection)
	}{
		// Connection is in failed state upon syntax error
		{
			name: "Run autocommit with syntax error",
			fun: func(t *testing.T, c connection.Connection) {
				s, err := c.Run("MATCH (n:Rand {val: $r} ", map[string]interface{}{"r": randInt()})
				if err == nil || s != nil {
					t.Fatal("Should have received error")
				}
				// TODO: Assert type of error!
			},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			c.fun(t, boltConn)
			if !boltConn.IsAlive() {
				t.Error("Connection died")
			}
			boltConn.Reset()
			// Should be working now
			s, err := boltConn.Run("RETURN datetime()", nil)
			if err != nil {
				t.Error("Reset didn't help")
			}
			if s == nil {
				t.Error("Didn't get a stream")
			}
			boltConn.Next(s.Handle)
			boltConn.Next(s.Handle)
		})
	}

	// Write really big query
	ot.Run("Really big query", func(t *testing.T) {
		query := "RETURN $x"
		bigBuilder := strings.Builder{}
		s := "0123456789"
		n := 10000
		size := len(s) * n // Should exceed 64k
		bigBuilder.Grow(size)
		for i := 0; i < n; i++ {
			bigBuilder.WriteString("0123456789")
		}

		stream, err := boltConn.Run(query, map[string]interface{}{"x": bigBuilder.String()})
		if err != nil {
			t.Fatal(err)
		}
		rec, sum, err := boltConn.Next(stream.Handle)
		if rec == nil || err != nil || sum != nil {
			t.Fatalf("Should only be a record, %+v, %+v, %+v", rec, sum, err)
		}
		recS := rec.Values[0].(string)
		if recS != bigBuilder.String() {
			t.Errorf("Strings differ")
		}
	})
}
