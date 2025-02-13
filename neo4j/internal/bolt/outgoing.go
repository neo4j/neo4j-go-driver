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
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"io"
	"reflect"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
)

type outgoing struct {
	chunker    chunker
	packer     packstream.Packer
	onPackErr  func(error)
	onIoErr    func(context.Context, error)
	boltLogger log.BoltLogger
	logId      string
	useUtc     bool
}

func (o *outgoing) begin() {
	o.chunker.beginMessage()
	o.packer.Begin(o.chunker.buf)
}

func (o *outgoing) end() {
	buf, err := o.packer.End()
	o.chunker.buf = buf
	o.chunker.endMessage()
	if err != nil {
		o.onPackErr(err)
	}
}

func (o *outgoing) appendHello(hello map[string]any) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "HELLO %s", loggableDictionary(hello))
	}
	o.begin()
	o.packer.StructHeader(msgHello, 1)
	o.packMap(hello)
	o.end()
}

func (o *outgoing) appendLogoff() {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "LOGOFF")
	}
	o.begin()
	o.packer.StructHeader(msgLogoff, 0)
	o.end()
}

func (o *outgoing) appendLogon(logon map[string]any) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "LOGON %s", loggableDictionary(logon))
	}
	o.begin()
	o.packer.StructHeader(msgLogon, 1)
	o.packMap(logon)
	o.end()
}

func (o *outgoing) appendBegin(meta map[string]any) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "BEGIN %s", loggableDictionary(meta))
	}
	o.begin()
	o.packer.StructHeader(msgBegin, 1)
	o.packMap(meta)
	o.end()
}

func (o *outgoing) appendCommit() {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "COMMIT")
	}
	o.begin()
	o.packer.StructHeader(msgCommit, 0)
	o.end()
}

func (o *outgoing) appendRollback() {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "ROLLBACK")
	}
	o.begin()
	o.packer.StructHeader(msgRollback, 0)
	o.end()
}

func (o *outgoing) appendRun(cypher string, params, meta map[string]any) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "RUN %q %s %s", cypher, loggableDictionary(params), loggableDictionary(meta))
	}
	o.begin()
	o.packer.StructHeader(msgRun, 3)
	o.packer.String(cypher)
	o.packMap(params)
	o.packMap(meta)
	o.end()
}

func (o *outgoing) appendPullN(n int) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "PULL %s", loggableDictionary{"n": n})
	}
	o.begin()
	o.packer.StructHeader(msgPullN, 1)
	o.packer.MapHeader(1)
	o.packer.String("n")
	o.packer.Int(n)
	o.end()
}

func (o *outgoing) appendPullNQid(n int, qid int64) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "PULL %s", loggableDictionary{"n": n, "qid": qid})
	}
	o.begin()
	o.packer.StructHeader(msgPullN, 1)
	o.packer.MapHeader(2)
	o.packer.String("n")
	o.packer.Int(n)
	o.packer.String("qid")
	o.packer.Int64(qid)
	o.end()
}

func (o *outgoing) appendDiscardN(n int) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "DISCARD %s", loggableDictionary{"n": n})
	}
	o.begin()
	o.packer.StructHeader(msgDiscardN, 1)
	o.packer.MapHeader(1)
	o.packer.String("n")
	o.packer.Int(n)
	o.end()
}

func (o *outgoing) appendDiscardNQid(n int, qid int64) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "DISCARD %s", loggableDictionary{"n": n, "qid": qid})
	}
	o.begin()
	o.packer.StructHeader(msgDiscardN, 1)
	o.packer.MapHeader(2)
	o.packer.String("n")
	o.packer.Int(n)
	o.packer.String("qid")
	o.packer.Int64(qid)
	o.end()
}

func (o *outgoing) appendPullAll() {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "PULL ALL")
	}
	o.begin()
	o.packer.StructHeader(msgPullAll, 0)
	o.end()
}

// Only valid for V4.3
func (o *outgoing) appendRouteToV43(context map[string]string, bookmarks []string, database string) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "ROUTE %s %s %q", loggableStringDictionary(context), loggableStringList(bookmarks), database)
	}
	o.begin()
	o.packer.StructHeader(msgRoute, 3)
	o.packer.MapHeader(len(context))
	for k, v := range context {
		o.packer.String(k)
		o.packer.String(v)
	}
	o.packer.ArrayHeader(len(bookmarks))
	for _, bookmark := range bookmarks {
		o.packer.String(bookmark)
	}
	if database == idb.DefaultDatabase {
		o.packer.Nil()
	} else {
		o.packer.String(database)
	}
	o.end()
}

func (o *outgoing) appendRoute(context map[string]string, bookmarks []string, what map[string]any) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "ROUTE %s %s %s", loggableStringDictionary(context), loggableStringList(bookmarks), loggableDictionary(what))
	}
	o.begin()
	o.packer.StructHeader(msgRoute, 3)
	o.packer.MapHeader(len(context))
	for k, v := range context {
		o.packer.String(k)
		o.packer.String(v)
	}
	o.packer.ArrayHeader(len(bookmarks))
	for _, bookmark := range bookmarks {
		o.packer.String(bookmark)
	}
	o.packMap(what)
	o.end()
}

func (o *outgoing) appendReset() {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "RESET")
	}
	o.begin()
	o.packer.StructHeader(msgReset, 0)
	o.end()
}

func (o *outgoing) appendGoodbye() {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "GOODBYE")
	}
	o.begin()
	o.packer.StructHeader(msgGoodbye, 0)
	o.end()
}

func (o *outgoing) appendTelemetry(api int) {
	if o.boltLogger != nil {
		o.boltLogger.LogClientMessage(o.logId, "TELEMETRY %d", api)
	}
	o.begin()
	o.packer.StructHeader(msgTelemetry, 1)
	o.packer.Int(api)
	o.end()
}

// For tests
func (o *outgoing) appendX(tag byte, fields ...any) {
	o.begin()
	o.packer.StructHeader(tag, len(fields))
	for _, f := range fields {
		o.packX(f)
	}
	o.end()
}

func (o *outgoing) send(ctx context.Context, wr io.Writer) {
	err := o.chunker.send(ctx, wr)
	if err != nil {
		o.onIoErr(ctx, err)
	}
}

func (o *outgoing) packMap(m map[string]any) {
	o.packer.MapHeader(len(m))
	for k, v := range m {
		o.packer.String(k)
		o.packX(v)
	}
}

func (o *outgoing) packStruct(x any) {
	switch v := x.(type) {
	case *dbtype.Point2D:
		o.packer.StructHeader('X', 3)
		o.packer.Uint32(v.SpatialRefId)
		o.packer.Float64(v.X)
		o.packer.Float64(v.Y)
	case dbtype.Point2D:
		o.packer.StructHeader('X', 3)
		o.packer.Uint32(v.SpatialRefId)
		o.packer.Float64(v.X)
		o.packer.Float64(v.Y)
	case *dbtype.Point3D:
		o.packer.StructHeader('Y', 4)
		o.packer.Uint32(v.SpatialRefId)
		o.packer.Float64(v.X)
		o.packer.Float64(v.Y)
		o.packer.Float64(v.Z)
	case dbtype.Point3D:
		o.packer.StructHeader('Y', 4)
		o.packer.Uint32(v.SpatialRefId)
		o.packer.Float64(v.X)
		o.packer.Float64(v.Y)
		o.packer.Float64(v.Z)
	case time.Time:
		if o.useUtc {
			if zone, _ := v.Zone(); zone == "Offset" {
				o.packUtcDateTimeWithTzOffset(v)
			} else {
				o.packUtcDateTimeWithTzName(v)
			}
			break
		}
		if zone, _ := v.Zone(); zone == "Offset" {
			o.packLegacyDateTimeWithTzOffset(v)
		} else {
			o.packLegacyDateTimeWithTzName(v)
		}
	case dbtype.LocalDateTime:
		t := time.Time(v)
		_, offset := t.Zone()
		secs := t.Unix() + int64(offset)
		o.packer.StructHeader('d', 2)
		o.packer.Int64(secs)
		o.packer.Int(t.Nanosecond())
	case dbtype.Date:
		t := time.Time(v)
		secs := t.Unix()
		_, offset := t.Zone()
		secs += int64(offset)
		days := secs / (60 * 60 * 24)
		o.packer.StructHeader('D', 1)
		o.packer.Int64(days)
	case dbtype.Time:
		t := time.Time(v)
		_, tzOffsetSecs := t.Zone()
		d := t.Sub(
			time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
		o.packer.StructHeader('T', 2)
		o.packer.Int64(d.Nanoseconds())
		o.packer.Int(tzOffsetSecs)
	case dbtype.LocalTime:
		t := time.Time(v)
		nanos := int64(time.Hour)*int64(t.Hour()) +
			int64(time.Minute)*int64(t.Minute()) +
			int64(time.Second)*int64(t.Second()) +
			int64(t.Nanosecond())
		o.packer.StructHeader('t', 1)
		o.packer.Int64(nanos)
	case dbtype.Duration:
		o.packer.StructHeader('E', 4)
		o.packer.Int64(v.Months)
		o.packer.Int64(v.Days)
		o.packer.Int64(v.Seconds)
		o.packer.Int(v.Nanos)
	default:
		o.onPackErr(&db.UnsupportedTypeError{Type: reflect.TypeOf(x)})
	}
}

func (o *outgoing) packX(x any) {
	if x == nil {
		o.packer.Nil()
		return
	}

	v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Bool:
		o.packer.Bool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		o.packer.Int64(v.Int())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		o.packer.Uint32(uint32(v.Uint()))
	case reflect.Uint64, reflect.Uint:
		o.packer.Uint64(v.Uint())
	case reflect.Float32, reflect.Float64:
		o.packer.Float64(v.Float())
	case reflect.String:
		o.packer.String(v.String())
	case reflect.Ptr:
		if v.IsNil() {
			o.packer.Nil()
			return
		}
		// Inspect what the pointer points to
		i := reflect.Indirect(v)
		switch i.Kind() {
		case reflect.Struct:
			o.packStruct(x)
		default:
			o.packV(i)
		}
	case reflect.Struct:
		o.packStruct(x)
	case reflect.Slice:
		// Optimizations
		switch s := x.(type) {
		case []byte:
			o.packer.Bytes(s) // Not just optimization
		case []int:
			o.packer.Ints(s)
		case []int64:
			o.packer.Int64s(s)
		case []string:
			o.packer.Strings(s)
		case []float64:
			o.packer.Float64s(s)
		case []any:
			o.packer.ArrayHeader(len(s))
			for _, e := range s {
				o.packX(e)
			}
		default:
			num := v.Len()
			o.packer.ArrayHeader(num)
			for i := 0; i < num; i++ {
				o.packV(v.Index(i))
			}
		}
	case reflect.Map:
		// Optimizations
		switch m := x.(type) {
		case map[string][]byte:
			o.packer.BytesMap(m)
		case map[string]int:
			o.packer.IntMap(m)
		case map[string]int64:
			o.packer.Int64Map(m)
		case map[string]string:
			o.packer.StringMap(m)
		case map[string]float64:
			o.packer.Float64Map(m)
		case map[string]any:
			o.packMap(m)
		default:
			t := reflect.TypeOf(x)
			if t.Key().Kind() != reflect.String {
				o.onPackErr(&db.UnsupportedTypeError{Type: reflect.TypeOf(x)})
				return
			}
			l := v.Len()
			o.packer.MapHeader(l)
			if l == 0 {
				return
			}
			key := reflect.New(t.Key()).Elem()
			value := reflect.New(t.Elem()).Elem()
			iter := v.MapRange()
			for iter.Next() {
				key.SetIterKey(iter)
				value.SetIterValue(iter)
				o.packer.String(key.String())
				o.packV(value)
			}
		}
	default:
		o.onPackErr(&db.UnsupportedTypeError{Type: reflect.TypeOf(x)})
	}
}

func typeForPrimitive[T any]() reflect.Type {
	var v T
	return reflect.TypeOf(v)
}

var intT = typeForPrimitive[int]()
var int64T = typeForPrimitive[int64]()
var stringT = typeForPrimitive[string]()
var float64T = typeForPrimitive[float64]()
var anyT = reflect.TypeOf((*any)(nil)).Elem()

func (o *outgoing) packV(v reflect.Value) {
	switch v.Kind() {
	case reflect.Bool:
		o.packer.Bool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		o.packer.Int64(v.Int())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		o.packer.Uint32(uint32(v.Uint()))
	case reflect.Uint64, reflect.Uint:
		o.packer.Uint64(v.Uint())
	case reflect.Float32, reflect.Float64:
		o.packer.Float64(v.Float())
	case reflect.String:
		o.packer.String(v.String())
	case reflect.Ptr:
		if v.IsNil() {
			o.packer.Nil()
			return
		}
		// Inspect what the pointer points to
		i := reflect.Indirect(v)
		switch i.Kind() {
		case reflect.Struct:
			o.packStruct(v.Interface())
		default:
			o.packV(i)
		}
	case reflect.Struct:
		o.packStruct(v.Interface())
	case reflect.Slice:
		elemType := v.Type().Elem()
		if elemType.Kind() == reflect.Uint8 {
			o.packer.Bytes(v.Bytes())
			return
		}
		num := v.Len()
		if num > 5 {
			switch elemType {
			case intT, int64T, stringT, float64T, anyT:
				// Accept the cost of an allocation (v.Interface())
				// because this slice type is optimized in packX.
				// The exact length from which the optimization amortizes
				// the allocation cost depends on many factors.
				// 5 seemed a close to the cross-over point on my system
				// running Go 1.23.0 linux/amd64.
				// cf. https://github.com/neo4j/neo4j-go-driver/pull/617
				o.packX(v.Interface())
				return
			}
		}
		o.packer.ArrayHeader(num)
		for i := 0; i < num; i++ {
			o.packV(v.Index(i))
		}
	default:
		o.packX(v.Interface())
	}
}

// deprecated: remove once 4.x Neo4j all reach EOL
func (o *outgoing) packLegacyDateTimeWithTzOffset(dateTime time.Time) {
	_, offset := dateTime.Zone()
	o.packer.StructHeader('F', 3)
	o.packer.Int64(dateTime.Unix() + int64(offset))
	o.packer.Int(dateTime.Nanosecond())
	o.packer.Int(offset)
}

// deprecated: remove once 4.x Neo4j all reach EOL
func (o *outgoing) packLegacyDateTimeWithTzName(dateTime time.Time) {
	_, offset := dateTime.Zone()
	o.packer.StructHeader('f', 3)
	o.packer.Int64(dateTime.Unix() + int64(offset))
	o.packer.Int(dateTime.Nanosecond())
	o.packer.String(dateTime.Location().String())
}

func (o *outgoing) packUtcDateTimeWithTzOffset(dateTime time.Time) {
	_, offset := dateTime.Zone()
	o.packer.StructHeader('I', 3)
	o.packer.Int64(dateTime.Unix())
	o.packer.Int(dateTime.Nanosecond())
	o.packer.Int(offset)
}

func (o *outgoing) packUtcDateTimeWithTzName(dateTime time.Time) {
	o.packer.StructHeader('i', 3)
	o.packer.Int64(dateTime.Unix())
	o.packer.Int(dateTime.Nanosecond())
	o.packer.String(dateTime.Location().String())
}
