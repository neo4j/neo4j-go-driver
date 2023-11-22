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

package bolt

import (
	"context"
	"fmt"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
)

// Utility to dehydrate/unpack
func unpack(u *packstream.Unpacker) any {
	u.Next()
	switch u.Curr {
	case packstream.PackedInt:
		return u.Int()
	case packstream.PackedFloat:
		return u.Float()
	case packstream.PackedStr:
		return u.String()
	case packstream.PackedByteArray:
		return u.ByteArray()
	case packstream.PackedNil:
		return nil
	case packstream.PackedTrue:
		return true
	case packstream.PackedFalse:
		return false
	case packstream.PackedArray:
		l := u.Len()
		a := make([]any, l)
		for i := range a {
			a[i] = unpack(u)
		}
		return a
	case packstream.PackedMap:
		l := u.Len()
		m := make(map[string]any, l)
		for i := uint32(0); i < l; i++ {
			u.Next()
			k := u.String()
			m[k] = unpack(u)
		}
		return m
	case packstream.PackedStruct:
		t := u.StructTag()
		l := u.Len()
		s := testStruct{tag: t}
		if l == 0 {
			return &s
		}
		s.fields = make([]any, l)
		for i := range s.fields {
			s.fields[i] = unpack(u)
		}
		return &s
	default:
		panic(".")
	}
}

func TestOutgoing(ot *testing.T) {
	var err error
	// Utility to unpack through dechunking and a custom build func
	dechunkAndUnpack := func(t *testing.T, build func(*testing.T, *outgoing)) any {
		out := &outgoing{
			chunker:   newChunker(),
			packer:    packstream.Packer{},
			onPackErr: func(e error) { err = e },
			onIoErr: func(_ context.Context, err error) {
				ot.Fatalf("Should be no io errors in this test: %s", err)
			},
		}
		serv, cli := net.Pipe()
		defer func() {
			if err := cli.Close(); err != nil {
				ot.Errorf("failed to close client connection %v", err)
			}
			if err := serv.Close(); err != nil {
				ot.Errorf("failed to close server connection %v", err)
			}
		}()
		err = nil
		build(t, out)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			out.send(context.Background(), cli)
		}()

		// Dechunk it
		_, byts, err := dechunkMessage(context.Background(), serv, []byte{}, -1)
		if err != nil {
			t.Fatal(err)
		}
		// Hydrate it
		unpacker := &packstream.Unpacker{}
		unpacker.Reset(byts)
		x := unpack(unpacker)
		if unpacker.Err != nil {
			t.Fatal(err)
		}
		return x
	}

	// tests for top level appending and sending outgoing messages
	cases := []struct {
		name   string
		build  func(t *testing.T, outgoing *outgoing)
		expect any
	}{
		{
			name: "hello",
			build: func(t *testing.T, out *outgoing) {
				out.appendHello(nil)
			},
			expect: &testStruct{
				tag:    byte(msgHello),
				fields: []any{map[string]any{}},
			},
		},
		{
			name: "begin",
			build: func(t *testing.T, out *outgoing) {
				out.appendBegin(map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgBegin),
				fields: []any{map[string]any{"mode": "r"}},
			},
		},
		{
			name: "commit",
			build: func(t *testing.T, out *outgoing) {
				out.appendCommit()
			},
			expect: &testStruct{
				tag: byte(msgCommit),
			},
		},
		{
			name: "rollback",
			build: func(t *testing.T, out *outgoing) {
				out.appendRollback()
			},
			expect: &testStruct{
				tag: byte(msgRollback),
			},
		},
		{
			name: "goodbye",
			build: func(t *testing.T, out *outgoing) {
				out.appendGoodbye()
			},
			expect: &testStruct{
				tag: byte(msgGoodbye),
			},
		},
		{
			name: "reset",
			build: func(t *testing.T, out *outgoing) {
				out.appendReset()
			},
			expect: &testStruct{
				tag: byte(msgReset),
			},
		},
		{
			name: "pull all",
			build: func(t *testing.T, out *outgoing) {
				out.appendPullAll()
			},
			expect: &testStruct{
				tag: byte(msgPullAll),
			},
		},
		{
			name: "pull n",
			build: func(t *testing.T, out *outgoing) {
				out.appendPullN(7)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []any{map[string]any{"n": int64(7)}},
			},
		},
		{
			name: "pull n+qid",
			build: func(t *testing.T, out *outgoing) {
				out.appendPullNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []any{map[string]any{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "discard n+qid",
			build: func(t *testing.T, out *outgoing) {
				out.appendDiscardNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgDiscardN),
				fields: []any{map[string]any{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "run, no params, no meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", nil, nil)
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{}, map[string]any{}},
			},
		},
		{
			name: "run, no params, meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", nil, map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{}, map[string]any{"mode": "r"}},
			},
		},
		{
			name: "run, params, meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", map[string]any{"x": 1, "y": "2"}, map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{"x": int64(1), "y": "2"}, map[string]any{"mode": "r"}},
			},
		},
		{
			name: "run, params, meta",
			build: func(t *testing.T, out *outgoing) {
				out.appendRun("cypher", map[string]any{"x": 1, "y": "2"}, map[string]any{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []any{"cypher", map[string]any{"x": int64(1), "y": "2"}, map[string]any{"mode": "r"}},
			},
		},
		{
			name: "route v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1", "key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, "adb")
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{"deutsch-mark", "mark-twain"}, "adb"},
			},
		},
		{
			name: "route, default database v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1",
					"key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, idb.DefaultDatabase)
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{"deutsch-mark", "mark-twain"}, nil},
			},
		},
		{
			name: "route, default bookmarks v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1", "key2": "val2"}, nil, "adb")
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{}, "adb"},
			},
		},
		{
			name: "route, default bookmarks and database v43",
			build: func(t *testing.T, out *outgoing) {
				out.appendRouteToV43(map[string]string{"key1": "val1",
					"key2": "val2"}, nil, idb.DefaultDatabase)
			},
			expect: &testStruct{
				tag:    byte(msgRoute),
				fields: []any{map[string]any{"key1": "val1", "key2": "val2"}, []any{}, nil},
			},
		},
		{
			name: "route",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, map[string]any{"db": "adb"})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{"deutsch-mark", "mark-twain"},
					map[string]any{"db": "adb"}},
			},
		},
		{
			name: "route, default database",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, []string{"deutsch-mark", "mark-twain"}, map[string]any{})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{"deutsch-mark", "mark-twain"},
					map[string]any{}},
			},
		},
		{
			name: "route, default bookmarks",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, nil, map[string]any{"db": "adb"})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{},
					map[string]any{"db": "adb"}},
			},
		},
		{
			name: "route, default bookmarks and database",
			build: func(t *testing.T, out *outgoing) {
				out.appendRoute(map[string]string{"key1": "val1", "key2": "val2"}, nil, map[string]any{})
			},
			expect: &testStruct{
				tag: byte(msgRoute),
				fields: []any{
					map[string]any{"key1": "val1", "key2": "val2"},
					[]any{},
					map[string]any{}},
			},
		},
		{
			name: "UTC datetime struct, with timezone offset",
			build: func(t *testing.T, out *outgoing) {
				defer func() {
					out.useUtc = false
				}()
				out.useUtc = true
				minusTwoHours := -2 * 60 * 60
				tz := time.FixedZone("Offset", minusTwoHours)
				out.begin()
				// June 15, 2020 12:30:00 in "Offset"
				// aka June 15, 2020 14:30:00 UTC
				// aka 1592231400 seconds since Unix epoch
				out.packStruct(time.Date(2020, 6, 15, 12, 30, 0, 42, tz))
				out.end()
			},
			expect: &testStruct{
				tag: 'I',
				fields: []any{
					int64(1592231400),
					int64(42),
					int64(-2 * 60 * 60),
				},
			},
		},
		{
			name: "UTC datetime struct, with timezone name",
			build: func(t *testing.T, out *outgoing) {
				defer func() {
					out.useUtc = false
				}()
				out.useUtc = true
				tz, err := time.LoadLocation("Pacific/Honolulu")
				if err != nil {
					t.Fatal(err)
				}
				out.begin()
				// June 15, 2020 04:30:00 in "Pacific/Honolulu"
				// aka June 15, 2020 14:30:00 UTC
				// aka 1592231400 seconds since Unix epoch
				out.packStruct(time.Date(2020, 6, 15, 4, 30, 0, 42, tz))
				out.end()
			},
			expect: &testStruct{
				tag: 'i',
				fields: []any{
					int64(1592231400),
					int64(42),
					"Pacific/Honolulu",
				},
			},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			x := dechunkAndUnpack(t, c.build)
			if !reflect.DeepEqual(x, c.expect) {
				t.Errorf("Unpacked differs, expected %+v (%T) but was %+v (%T)", c.expect, c.expect, x, x)
			}
		})
	}

	offsetZone := time.FixedZone("Offset", 100)

	type (
		customBool        bool
		customFloat       float64
		customInt         int64
		customString      string
		customByteSlice   []byte
		customStringSlice []string
		customMapOfInts   map[string]int
	)
	// Test packing of maps in more detail, essentially tests allowed parameters to Run command
	// tests for top level appending and sending outgoing messages
	paramCases := []struct {
		name   string
		inp    map[string]any
		expect map[string]any
	}{
		{
			name: "map of maps",
			inp: map[string]any{
				"map":    map[string]any{"k1": "v1"},
				"int":    map[string]int{"k2": 1},
				"[]int":  map[string][]int{"k3": {1, 2, 3}},
				"[]bool": map[string]bool{"t": true, "f": false},
			},
			expect: map[string]any{
				"map":    map[string]any{"k1": "v1"},
				"int":    map[string]any{"k2": int64(1)},
				"[]int":  map[string]any{"k3": []any{int64(1), int64(2), int64(3)}},
				"[]bool": map[string]any{"t": true, "f": false},
			},
		},
		{
			name: "map of spatial",
			inp: map[string]any{
				"p1": dbtype.Point2D{SpatialRefId: 1, X: 2, Y: 3},
				"p2": dbtype.Point3D{SpatialRefId: 4, X: 5, Y: 6, Z: 7},
				"ps": []dbtype.Point3D{{SpatialRefId: 4, X: 5, Y: 6, Z: 7}},
			},
			expect: map[string]any{
				"p1": &testStruct{tag: 'X', fields: []any{int64(1), float64(2), float64(3)}},
				"p2": &testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}},
				"ps": []any{&testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}}},
			},
		},
		{
			name: "map of spatial pointers",
			inp: map[string]any{
				"p1": &dbtype.Point2D{SpatialRefId: 1, X: 2, Y: 3},
				"p2": &dbtype.Point3D{SpatialRefId: 4, X: 5, Y: 6, Z: 7},
			},
			expect: map[string]any{
				"p1": &testStruct{tag: 'X', fields: []any{int64(1), float64(2), float64(3)}},
				"p2": &testStruct{tag: 'Y', fields: []any{int64(4), float64(5), float64(6), float64(7)}},
			},
		},
		{
			name: "map of temporals",
			inp: map[string]any{
				"time.Time UTC":    time.Unix(1, 2).UTC(),
				"time.Time offset": time.Unix(1, 2).In(offsetZone),
				"LocalDateTime":    dbtype.LocalDateTime(time.Unix(1, 2).UTC()),
				"Date":             dbtype.Date(time.Date(1993, 11, 31, 7, 59, 1, 100, time.UTC)),
				"Time":             dbtype.Time(time.Unix(1, 2).In(offsetZone)),
				"LocalTime":        dbtype.LocalTime(time.Unix(1, 2).UTC()),
				"Duration":         dbtype.Duration{Months: 1, Days: 2, Seconds: 3, Nanos: 4},
			},
			expect: map[string]any{
				"time.Time UTC":    &testStruct{tag: 'f', fields: []any{int64(1), int64(2), "UTC"}},
				"time.Time offset": &testStruct{tag: 'F', fields: []any{int64(101), int64(2), int64(100)}},
				"LocalDateTime":    &testStruct{tag: 'd', fields: []any{int64(1), int64(2)}},
				"Date":             &testStruct{tag: 'D', fields: []any{int64(8735)}},
				"Time":             &testStruct{tag: 'T', fields: []any{int64(101*time.Second + 2), int64(100)}},
				"LocalTime":        &testStruct{tag: 't', fields: []any{int64(1*time.Second + 2)}},
				"Duration":         &testStruct{tag: 'E', fields: []any{int64(1), int64(2), int64(3), int64(4)}},
			},
		},
		{
			name: "map of custom native types",
			inp: map[string]any{
				"custom bool":         customBool(true),
				"custom float":        customFloat(3.14),
				"custom int":          customInt(12345),
				"custom string":       customString("Hello"),
				"custom byte slice":   customByteSlice([]byte{1, 2, 3}),
				"custom string slice": customStringSlice([]string{"hello", "again"}),
				"custom map of ints":  customMapOfInts(map[string]int{"l": 1}),
			},
			expect: map[string]any{
				"custom bool":   true,
				"custom float":  3.14,
				"custom int":    int64(12345),
				"custom string": "Hello",
				// Custom will cause []byte to come back as []any, could be handled but maybe not worth it
				"custom byte slice":   []any{int64(1), int64(2), int64(3)},
				"custom string slice": []any{"hello", "again"},
				"custom map of ints":  map[string]any{"l": int64(1)},
			},
		},
		{
			name: "map of pointer types",
			inp: map[string]any{
				"*[]int":             &([]int{3}),
				"*map[string]string": &(map[string]string{"x": "y"}),
			},
			expect: map[string]any{
				"*[]int":             []any{int64(3)},
				"*map[string]string": map[string]any{"x": "y"},
			},
		},
	}

	for _, c := range paramCases {
		ot.Run(c.name, func(t *testing.T) {
			x := dechunkAndUnpack(t, func(t *testing.T, out *outgoing) {
				out.begin()
				out.packMap(c.inp)
				out.end()
			})
			if !reflect.DeepEqual(x, c.expect) {
				t.Errorf("Unpacked differs, expected\n %#v but was\n %#v", c.expect, x)
			}
		})
	}

	type aStruct struct{}

	// Test packing of stuff that is expected to give an error
	paramErrorCases := []struct {
		name string
		inp  map[string]any
		err  error
	}{
		{
			name: "map with non string key",
			inp: map[string]any{
				"m": map[int]string{1: "y"},
			},
			err: &db.UnsupportedTypeError{},
		},
		{
			name: "a random struct",
			inp: map[string]any{
				"m": aStruct{},
			},
			err: &db.UnsupportedTypeError{},
		},
		{
			name: "a random *struct",
			inp: map[string]any{
				"m": &aStruct{},
			},
			err: &db.UnsupportedTypeError{},
		},
	}
	for _, c := range paramErrorCases {
		var err error
		out := &outgoing{
			chunker:   newChunker(),
			packer:    packstream.Packer{},
			onPackErr: func(e error) { err = e },
			onIoErr: func(_ context.Context, err error) {
				ot.Fatalf("Should be no io errors in this test: %s", err)
			},
		}
		ot.Run(c.name, func(t *testing.T) {
			out.begin()
			out.packMap(c.inp)
			out.end()
			if reflect.TypeOf(err) != reflect.TypeOf(c.err) {
				t.Error(err)
			}
		})
	}
}

func TestCredentialsRedaction(outer *testing.T) {
	outer.Parallel()

	type testCase struct {
		description        string
		produceBoltMessage func(*outgoing)
		credentials        string
	}

	testCases := []testCase{
		{
			description: "HELLO msg",
			produceBoltMessage: func(o *outgoing) {
				o.appendHello(map[string]any{
					"credentials": "sup3rs3cr3t",
				})
			},
			credentials: "sup3rs3cr3t",
		},
		{
			description: "LOGON msg",
			produceBoltMessage: func(o *outgoing) {
				o.appendLogon(map[string]any{
					"credentials": "letmein!",
				})
			},
			credentials: "letmein!",
		},
	}

	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			logger := &inMemoryBoltLogger{}
			outWriter := &outgoing{
				chunker:    newChunker(),
				packer:     packstream.Packer{},
				boltLogger: logger,
			}

			testCase.produceBoltMessage(outWriter)

			AssertFalse(t, logger.AnyClientMessageContains(testCase.credentials))
		})
	}
}

type inMemoryBoltLogger struct {
	clientMessages []string
	serverMessages []string
}

func (log *inMemoryBoltLogger) LogClientMessage(context string, msg string, args ...any) {
	log.clientMessages = append(log.clientMessages, log.format(context, msg, args))
}

func (log *inMemoryBoltLogger) LogServerMessage(context string, msg string, args ...any) {
	log.serverMessages = append(log.serverMessages, log.format(context, msg, args))
}

func (log *inMemoryBoltLogger) AnyClientMessageContains(substring string) bool {
	for _, clientMessage := range log.clientMessages {
		if strings.Contains(clientMessage, substring) {
			return true
		}
	}
	return false
}

func (log *inMemoryBoltLogger) format(context string, msg string, args []any) string {
	return fmt.Sprintf("[%s] %s", context, fmt.Sprintf(msg, args...))
}
