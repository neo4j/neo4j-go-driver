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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/log"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"
)

// Tests the specification of the internal db connection API
func TestConnectionConformance(ot *testing.T) {
	server, err := control.EnsureSingleInstance()
	assertNoError(ot, err)

	uri := server.BoltURI()
	parsedUri, err := url.Parse(uri)
	assertNoError(ot, err)

	tcpConn, err := net.Dial("tcp", parsedUri.Host)
	assertNoError(ot, err)

	authMap := map[string]interface{}{
		"scheme":      "basic",
		"principal":   server.Username(),
		"credentials": server.Password(),
	}

	logger := &log.ConsoleLogger{Errors: true, Infos: true, Warns: true}
	boltConn, err := bolt.Connect(parsedUri.Host, tcpConn, authMap, logger)
	assertNoError(ot, err)
	defer boltConn.Close()

	randInt := func() int64 {
		bid, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
		return bid.Int64()
	}

	// All of these tests should leave the connection in a good state without the need
	// for a reset. All tests share the same connection.
	cases := []struct {
		name string
		fun  func(*testing.T, db.Connection)
	}{
		{
			// Leaves the connection in perfect state after creating and cosuming all records
			name: "Run autocommit, full consume",
			fun: func(t *testing.T, c db.Connection) {
				s, err := c.Run("CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": randInt()}, db.WriteMode, nil, 0, nil)
				assertNoError(t, err)
				rec, sum, err := c.Next(s.Handle)
				assertDbRecord(t, rec, sum, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbSummary(t, rec, sum, err)
			},
		},
		{
			// Let the connection consume the result before next autocommit (also leaves the
			// connection in a streaming state)
			name: "Run autocommit twice, no consume",
			fun: func(t *testing.T, c db.Connection) {
				_, err := c.Run("CREATE (n:Rand {val: $r})", map[string]interface{}{"r": randInt()}, db.WriteMode, nil, 0, nil)
				assertNoError(t, err)
				_, err = c.Run("CREATE (n:Rand {val: $r})", map[string]interface{}{"r": randInt()}, db.WriteMode, nil, 0, nil)
				assertNoError(t, err)
			},
		},
		{
			// Consume everything returned from create before committing
			name: "Run explicit commit, full consume",
			fun: func(t *testing.T, c db.Connection) {
				txHandle, err := c.TxBegin(db.WriteMode, []string{}, 10*time.Minute, nil)
				assertNoError(t, err)
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				assertNoError(t, err)
				rec, sum, err := c.Next(s.Handle)
				assertDbRecord(t, rec, sum, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbSummary(t, rec, sum, err)
				err = c.TxCommit(txHandle)
				assertNoError(t, err)
				// Make sure it's commited
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r}, db.ReadMode, nil, 0, nil)
				assertNoError(t, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbRecord(t, rec, sum, err)
				// Not everything consumed from the read check, but that is also fine
			},
		},
		{
			// Do not consume anything before commiting
			name: "Run explicit commit, no consume",
			fun: func(t *testing.T, c db.Connection) {
				txHandle, err := c.TxBegin(db.WriteMode, []string{}, 10*time.Minute, nil)
				assertNoError(t, err)
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				assertNoError(t, err)
				err = c.TxCommit(txHandle)
				assertNoError(t, err)
				// Make sure it's commited
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r}, db.ReadMode, nil, 0, nil)
				assertNoError(t, err)
				rec, sum, err := c.Next(s.Handle)
				assertDbRecord(t, rec, sum, err)
				// Not everything consumed from the read check, but that is also fine
			},
		},
		{
			// Consume everything returned from create before rolling back
			name: "Run explicit rollback, full consume",
			fun: func(t *testing.T, c db.Connection) {
				txHandle, err := c.TxBegin(db.WriteMode, []string{}, 10*time.Minute, nil)
				assertNoError(t, err)
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				assertNoError(t, err)
				rec, sum, err := c.Next(s.Handle)
				assertDbRecord(t, rec, sum, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbSummary(t, rec, sum, err)
				err = c.TxRollback(txHandle)
				assertNoError(t, err)
				// Make sure it's rolled back
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r}, db.ReadMode, nil, 0, nil)
				assertNoError(t, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbSummary(t, rec, sum, err)
			},
		},
		{
			// Do not consume anything before rolling back
			name: "Run explicit rollback, no consume",
			fun: func(t *testing.T, c db.Connection) {
				txHandle, err := c.TxBegin(db.WriteMode, []string{}, 10*time.Minute, nil)
				assertNoError(t, err)
				r := randInt()
				s, err := c.RunTx(txHandle, "CREATE (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r})
				assertNoError(t, err)
				err = c.TxRollback(txHandle)
				assertNoError(t, err)
				// Make sure it's commited
				s, err = c.Run("MATCH (n:Rand {val: $r}) RETURN n", map[string]interface{}{"r": r}, db.ReadMode, nil, 0, nil)
				assertNoError(t, err)
				rec, sum, err := c.Next(s.Handle)
				assertDbSummary(t, rec, sum, err)
			},
		},
		{
			name: "Next without streaming",
			fun: func(t *testing.T, c db.Connection) {
				rec, sum, err := c.Next(3)
				assertDbError(t, rec, sum, err)
			},
		},
		{
			name: "Next passed the summary",
			fun: func(t *testing.T, c db.Connection) {
				s, err := boltConn.Run("RETURN datetime()", nil, db.ReadMode, nil, 0, nil)
				assertNoError(t, err)
				rec, sum, err := c.Next(s.Handle)
				assertDbRecord(t, rec, sum, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbSummary(t, rec, sum, err)
				rec, sum, err = c.Next(s.Handle)
				assertDbError(t, rec, sum, err)
			},
		},
		{
			name: "Run autocommit while in tx",
			fun: func(t *testing.T, c db.Connection) {
				txHandle, err := c.TxBegin(db.WriteMode, []string{}, 10*time.Minute, nil)
				assertNoError(t, err)
				defer c.TxRollback(txHandle)
				s, err := c.Run("CREATE (n:Rand {val: $r})", map[string]interface{}{"r": randInt()}, db.WriteMode, nil, 0, nil)
				if s != nil || err == nil {
					t.Fatal("Should fail to run auto commit when in transaction")
				}
				// TODO: Assert type of error!
			},
		},
		{
			name: "Commit while not in tx",
			fun: func(t *testing.T, c db.Connection) {
				err := c.TxCommit(1)
				if err == nil {
					t.Fatal("Should have failed")
				}
				// TODO: Assert type of error!
			},
		},
		{
			name: "Rollback while not in tx",
			fun: func(t *testing.T, c db.Connection) {
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
		fun  func(*testing.T, db.Connection)
	}{
		// Connection is in failed state due to syntax error
		{
			name: "Run autocommit with syntax error",
			fun: func(t *testing.T, c db.Connection) {
				s, err := c.Run("MATCH (n:Rand {val: $r} ", map[string]interface{}{"r": randInt()}, db.ReadMode, nil, 0, nil)
				if err == nil || s != nil {
					t.Fatal("Should have received error")
				}
				_, isDbError := err.(*db.DatabaseError)
				if !isDbError {
					t.Error("Should be db error")
				}
			},
		},
		{
			name: "Run autocommit with division by zero in result",
			fun: func(t *testing.T, c db.Connection) {
				s, err := c.Run("UNWIND [0] AS x RETURN 10 / x", map[string]interface{}{"r": randInt()}, db.ReadMode, nil, 0, nil)
				assertNoError(t, err)
				// Should get error while iterating
				_, _, err = c.Next(s.Handle)
				if err == nil {
					t.Error("Should have error")
				}
				_, isDbError := err.(*db.DatabaseError)
				if !isDbError {
					t.Error("Should be db error")
				}
			},
		},
		// Connection is in transaction
		{
			name: "Set connection in transaction mode",
			fun: func(t *testing.T, c db.Connection) {
				_, err := c.TxBegin(db.WriteMode, []string{}, 0, nil)
				assertNoError(t, err)
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
			s, err := boltConn.Run("RETURN datetime()", nil, db.ReadMode, nil, 0, nil)
			assertNoError(t, err)
			if s == nil {
				t.Fatal("Didn't get a stream")
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

		stream, err := boltConn.Run(query, map[string]interface{}{"x": bigBuilder.String()}, db.ReadMode, nil, 0, nil)
		assertNoError(t, err)
		rec, sum, err := boltConn.Next(stream.Handle)
		assertDbRecord(t, rec, sum, err)
		recS := rec.Values[0].(string)
		if recS != bigBuilder.String() {
			t.Errorf("Strings differ")
		}
	})

	assertTime := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		if t1.Hour() != t2.Hour() || t1.Minute() != t2.Minute() ||
			t1.Second() != t2.Second() || t1.Nanosecond() != t2.Nanosecond() {
			t.Errorf("Time %+v vs %+v", t1, t1)
		}
	}

	assertTimeOffset := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()

		_, off1 := t1.Zone()
		_, off2 := t2.Zone()

		if off1 != off2 {
			t.Errorf("Offset %d vs %d", off1, off2)
		}
	}

	assertTimeZone := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()

		z1, _ := t1.Zone()
		z2, _ := t2.Zone()

		if z1 != z2 {
			t.Errorf("Zone %s vs %s", z1, z2)
		}
	}

	assertDate := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		if t1.Year() != t2.Year() || t1.Month() != t2.Month() || t1.Day() != t2.Day() {
			t.Errorf("Date %s vs %s", t1, t2)
		}
	}

	assertDuration := func(t *testing.T, dur1, dur2 types.Duration) {
		t.Helper()
		if !reflect.DeepEqual(dur1, dur2) {
			t.Errorf("Duration %+v vs %+v", dur1, dur2)
		}
	}

	// Temporal types
	ot.Run("Temporal types", func(tt *testing.T) {
		london, _ := time.LoadLocation("Europe/London")

		// In Cypher
		cTime := "time({ hour: 23, minute: 49, second: 59, nanosecond: 999999999, timezone:'+03:00' })"
		cDate := "date({ year: 1994, month: 11, day: 15 })"
		cDateTimeO := "datetime({ year: 1859, month: 5, day: 31, hour: 23, minute: 49, second: 59, nanosecond: 999999999, timezone:'+02:30' })"
		cDateTimeZ := "datetime({ year: 1959, month: 5, day: 31, hour: 23, minute: 49, second: 59, nanosecond: 999999999, timezone:'Europe/London' })"
		cLocalTime := "localtime({ hour: 23, minute: 49, second: 59, nanosecond: 999999999 })"
		cLocalDateTime := "localdatetime({ year: 1859, month: 5, day: 31, hour: 23, minute: 49, second: 59, nanosecond: 999999999 })"
		cDuration := "duration({ months: 16, days: 45, seconds: 120, nanoseconds: 187309812 })"
		// Same as above in time.Time
		tTime := time.Date(0, 0, 0, 23, 49, 59, 999999999, time.FixedZone("Offset", 3*60*60))
		tDate := time.Date(1994, 11, 15, 0, 0, 0, 0, time.Local)
		tDateTimeO := time.Date(1859, 5, 31, 23, 49, 59, 999999999, time.FixedZone("Offset", 150*60))
		tDateTimeZ := time.Date(1959, 5, 31, 23, 49, 59, 999999999, london)
		tLocalTime := time.Date(0, 0, 0, 23, 49, 59, 999999999, time.Local)
		tLocalDateTime := time.Date(1859, 5, 31, 23, 49, 59, 999999999, time.Local)
		tDuration := types.Duration{Months: 16, Days: 45, Seconds: 120, Nanos: 187309812}

		tt.Run("Reading", func(t *testing.T) {
			query := "RETURN " +
				cTime + ", " + cDate + ", " + cDateTimeO + ", " + cDateTimeZ + ", " + cLocalTime + ", " + cLocalDateTime + ", " + cDuration
			stream, err := boltConn.Run(query, nil, db.ReadMode, nil, 0, nil)
			assertNoError(t, err)
			rec, sum, err := boltConn.Next(stream.Handle)
			if rec == nil || err != nil || sum != nil {
				t.Fatalf("Should be a record, %+v, %+v, %+v", rec, sum, err)
			}

			// Verify each temporal type
			// Time
			gotTime := time.Time(rec.Values[0].(types.Time))
			assertTime(t, gotTime, tTime)
			assertTimeOffset(t, gotTime, tTime)
			// Date
			gotDate := time.Time(rec.Values[1].(types.Date))
			assertDate(t, gotDate, tDate)
			// DateTime, offset
			gotDateTime := time.Time(rec.Values[2].(time.Time))
			assertDate(t, time.Time(gotDateTime), tDateTimeO)
			assertTime(t, gotDateTime, tDateTimeO)
			assertTimeOffset(t, gotDateTime, tDateTimeO)
			// DateTime, zone
			gotDateTime = time.Time(rec.Values[3].(time.Time))
			assertDate(t, time.Time(gotDateTime), tDateTimeZ)
			assertTime(t, gotDateTime, tDateTimeZ)
			assertTimeZone(t, gotDateTime, tDateTimeZ)
			// Local time
			gotTime = time.Time(rec.Values[4].(types.LocalTime))
			assertTime(t, gotTime, tLocalTime)
			// Local DateTime
			gotDateTime = time.Time(rec.Values[5].(types.LocalDateTime))
			assertDate(t, time.Time(gotDateTime), tLocalDateTime)
			assertTime(t, gotDateTime, tLocalDateTime)
			// Duration
			gotDuration := rec.Values[6].(types.Duration)
			assertDuration(t, gotDuration, tDuration)
		})

		tt.Run("Writing", func(t *testing.T) {
			// Make a node with all temporal types as parameters and make sure that we can interpret
			// it the same way again.
			r := randInt()
			stream, _ := boltConn.Run(
				"CREATE (n:Rand {"+
					"val: $r, time: $time, date: $date, dateTimeO: $dateTimeO, "+
					"dateTimeZ: $dateTimeZ, localTime: $localTime, localDateTime: $localDateTime}) "+
					"RETURN n",
				map[string]interface{}{
					"r":             r,
					"time":          types.Time(tTime),
					"date":          types.Date(tDate),
					"dateTimeO":     tDateTimeO,
					"dateTimeZ":     tDateTimeZ,
					"localTime":     types.LocalTime(tLocalTime),
					"localDateTime": types.LocalDateTime(tLocalDateTime),
				}, db.WriteMode, nil, 0, nil)
			rec, sum, err := boltConn.Next(stream.Handle)
			if rec == nil || err != nil || sum != nil {
				t.Fatalf("Should be a record, %+v, %+v, %+v", rec, sum, err)
			}

			// Verify all temporal instances as when reading (as long as that test passes, the
			// errors here should be due to writing).
			node := rec.Values[0].(*types.Node)
			// Time
			gotTime := time.Time(node.Props["time"].(types.Time))
			assertTime(t, gotTime, tTime)
			assertTimeOffset(t, gotTime, tTime)
			// Date
			gotDate := time.Time(node.Props["date"].(types.Date))
			assertDate(t, gotDate, tDate)
			// DateTime, offset
			gotDateTime := time.Time(node.Props["dateTimeO"].(time.Time))
			assertDate(t, time.Time(gotDateTime), tDateTimeO)
			assertTime(t, gotDateTime, tDateTimeO)
			assertTimeOffset(t, gotDateTime, tDateTimeO)
			// DateTime, zone
			gotDateTime = time.Time(node.Props["dateTimeZ"].(time.Time))
			assertDate(t, time.Time(gotDateTime), tDateTimeZ)
			assertTime(t, gotDateTime, tDateTimeZ)
			assertTimeZone(t, gotDateTime, tDateTimeZ)
			// Local time
			gotTime = time.Time(node.Props["localTime"].(types.LocalTime))
			assertTime(t, gotTime, tLocalTime)
			// Local DateTime
			gotDateTime = time.Time(node.Props["localDateTime"].(types.LocalDateTime))
			assertDate(t, time.Time(gotDateTime), tLocalDateTime)
			assertTime(t, gotDateTime, tLocalDateTime)
		})
	})

	// Bookmark tests
	ot.Run("Bookmarks", func(tt *testing.T) {
		boltConn.Reset()
		lastBookmark := boltConn.Bookmark()

		assertNewBookmark := func(t *testing.T) {
			t.Helper()
			bookmark := boltConn.Bookmark()
			if len(bookmark) == 0 {
				t.Fatal("No bookmark")
			}
			if bookmark == lastBookmark {
				t.Fatal("No new bookmark")
			}
			lastBookmark = bookmark
		}

		assertNoNewBookmark := func(t *testing.T) {
			t.Helper()
			bookmark := boltConn.Bookmark()
			if bookmark != lastBookmark {
				t.Fatal("New bookmark")
			}
		}

		tt.Run("Auto-commit, bookmark by iteration", func(t *testing.T) {
			s, _ := boltConn.Run("CREATE (n:BmRand {x: $rand}) RETURN n", map[string]interface{}{"rand": randInt()}, db.WriteMode, nil, 0, nil)
			boltConn.Next(s.Handle)
			boltConn.Next(s.Handle)
			assertNewBookmark(t)
		})
		tt.Run("Auto-commit, bookmark by new auto-commit", func(t *testing.T) {
			boltConn.Run("CREATE (n:BmRand {x: $rand}) RETURN n", map[string]interface{}{"rand": randInt()}, db.WriteMode, nil, 0, nil)
			s, _ := boltConn.Run("CREATE (n:BmRand {x: $rand}) RETURN n", map[string]interface{}{"rand": randInt()}, db.WriteMode, nil, 0, nil)
			assertNewBookmark(t)
			boltConn.Next(s.Handle)
			boltConn.Next(s.Handle)
			assertNewBookmark(t)
		})
		tt.Run("Commit", func(t *testing.T) {
			tx, _ := boltConn.TxBegin(db.WriteMode, nil, 0, nil)
			s, _ := boltConn.RunTx(tx, "CREATE (n:BmRand {x: $rand}) RETURN n", map[string]interface{}{"rand": randInt()})
			boltConn.Next(s.Handle)
			boltConn.Next(s.Handle)
			assertNoNewBookmark(t)
			boltConn.TxCommit(tx)
			assertNewBookmark(t)
		})
		tt.Run("Rollback", func(t *testing.T) {
			tx, _ := boltConn.TxBegin(db.WriteMode, nil, 0, nil)
			s, _ := boltConn.RunTx(tx, "CREATE (n:BmRand {x: $rand}) RETURN n", map[string]interface{}{"rand": randInt()})
			boltConn.Next(s.Handle)
			boltConn.Next(s.Handle)
			assertNoNewBookmark(t)
			boltConn.TxRollback(tx)
			assertNoNewBookmark(t)
		})
	})

	// Enterprise feature
	ot.Run("Multidatabase", func(tt *testing.T) {
		selector, supportsMultidatabase := boltConn.(db.DatabaseSelector)
		if !supportsMultidatabase {
			tt.Skipf("Database %s:%s does not support multidatabase functionality", boltConn.ServerName(), boltConn.ServerVersion())
		}

		// Should always reset before selecting a database
		boltConn.Reset()
		// Connect to system database and create a test databases
		selector.SelectDatabase("system")
		boltConn.Run("DROP DATABASE test1 IF EXISTS", nil, db.WriteMode, nil, 0, nil)
		_, err := boltConn.Run("CREATE DATABASE test1", nil, db.WriteMode, nil, 0, nil)
		if err != nil {
			dbErr, _ := err.(*db.DatabaseError)
			if dbErr == nil || dbErr.Code != "Neo.ClientError.Database.ExistingDatabaseFound" {
				tt.Fatal(err)
			}
		}
		boltConn.Reset()
		// Use test database to create a random node
		selector.SelectDatabase("test1")
		r := randInt()
		_, err = boltConn.Run("CREATE (n:MdbRand {x: $x}) RETURN n", map[string]interface{}{"x": r}, db.WriteMode, nil, 0, nil)
		assertNoError(tt, err)
		boltConn.Reset()
		// Connect to standard database and make sure we can't see the node
		s, err := boltConn.Run("MATCH (n:MdbRand {x: $x}) RETURN n", map[string]interface{}{"x": r}, db.ReadMode, nil, 0, nil)
		assertNoError(tt, err)
		rec, sum, err := boltConn.Next(s.Handle)
		assertDbSummary(tt, rec, sum, err)
		boltConn.Reset()
		// Connect to test database and make sure we can see the node
		selector.SelectDatabase("test1")
		s, err = boltConn.Run("MATCH (n:MdbRand {x: $x}) RETURN n", map[string]interface{}{"x": r}, db.ReadMode, nil, 0, nil)
		assertNoError(tt, err)
		rec, sum, err = boltConn.Next(s.Handle)
		assertDbRecord(tt, rec, sum, err)
		boltConn.Reset()
	})
}
