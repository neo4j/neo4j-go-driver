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

package propertyencryption

import (
	"fmt"
	"reflect"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/packstream"
)

const secondsPerDay = 60 * 60 * 24

// Encoded is a value encoded to plaintext bytes, with the metadata recorded alongside it.
type Encoded struct {
	Bytes    []byte
	TypeName string
	// Baseline is the lowest scheme version able to decode Bytes, the maximum baseline of
	// every property type the value contains.
	Baseline Version
}

// EncodeValue encodes a Neo4j property value into plaintext bytes.
func EncodeValue(value any) (Encoded, error) {
	return encode(value, typeBaselines)
}

// EncodeAAD encodes a value for use as additional authenticated data, accepting only the
// property types a caller can reliably reproduce.
func EncodeAAD(value any) (Encoded, error) {
	return encode(value, aadTypeBaselines)
}

var aadTypeBaselines = func() map[string]Version {
	m := make(map[string]Version, len(aadTypes))
	for name := range aadTypes {
		m[name] = typeBaselines[name]
	}
	return m
}()

func encode(value any, permitted map[string]Version) (Encoded, error) {
	e := encoder{permitted: permitted}
	e.packer.Begin(make([]byte, 0, 64))
	typeName := e.value(value, false)
	buf, packErr := e.packer.End()
	if e.err != nil {
		return Encoded{}, e.err
	}
	if packErr != nil {
		return Encoded{}, packErr
	}
	return Encoded{Bytes: buf, TypeName: typeName, Baseline: e.baseline}, nil
}

type encoder struct {
	packer    packstream.Packer
	permitted map[string]Version
	baseline  Version
	err       error
}

func (e *encoder) setErr(format string, args ...any) {
	if e.err == nil {
		e.err = &ValueError{Message: fmt.Sprintf(format, args...)}
	}
}

// accept raises the running baseline for an encountered type and reports whether it may be
// encoded here.
func (e *encoder) accept(typeName string) bool {
	baseline, ok := e.permitted[typeName]
	if !ok {
		if _, isProperty := typeBaselines[typeName]; isProperty {
			e.setErr("%s is not supported as additional authenticated data", typeName)
		} else {
			e.setErr("%s is not a Neo4j property type", typeName)
		}
		return false
	}
	e.baseline = e.baseline.max(baseline)
	return true
}

// value encodes a single value and returns its property type name. inList restricts it to
// the types a Neo4j list may hold.
func (e *encoder) value(x any, inList bool) string {
	if e.err != nil {
		return ""
	}
	if x == nil {
		if inList {
			e.setErr("a list stored as a property cannot contain null")
		} else {
			e.setErr("null is not a Neo4j property type")
		}
		return ""
	}

	switch v := x.(type) {
	case bool:
		return e.pack(TypeBoolean, func() { e.packer.Bool(v) })
	case string:
		return e.pack(TypeString, func() { e.packer.String(v) })
	case []byte:
		return e.pack(TypeBytes, func() { e.packer.Bytes(v) })
	case int:
		return e.packInt(int64(v))
	case int8:
		return e.packInt(int64(v))
	case int16:
		return e.packInt(int64(v))
	case int32:
		return e.packInt(int64(v))
	case int64:
		return e.packInt(v)
	case uint:
		return e.packUint(uint64(v))
	case uint8:
		return e.packInt(int64(v))
	case uint16:
		return e.packInt(int64(v))
	case uint32:
		return e.packInt(int64(v))
	case uint64:
		return e.packUint(v)
	case float32:
		return e.pack(TypeFloat, func() { e.packer.Float64(float64(v)) })
	case float64:
		return e.pack(TypeFloat, func() { e.packer.Float64(v) })
	case dbtype.UUID:
		return e.pack(TypeUUID, func() { e.packer.UUID(v) })
	case dbtype.Date:
		return e.pack(TypeDate, func() { e.packDate(v) })
	case dbtype.Time:
		return e.pack(TypeZonedTime, func() { e.packZonedTime(v) })
	case dbtype.LocalTime:
		return e.pack(TypeLocalTime, func() { e.packLocalTime(v) })
	case dbtype.LocalDateTime:
		return e.pack(TypeLocalDateTime, func() { e.packLocalDateTime(v) })
	case time.Time:
		return e.pack(TypeZonedDateTime, func() { e.packZonedDateTime(v) })
	case dbtype.Duration:
		return e.pack(TypeDuration, func() { e.packDuration(v) })
	case dbtype.Point2D:
		return e.pack(TypePoint, func() { e.packPoint2D(v) })
	case dbtype.Point3D:
		return e.pack(TypePoint, func() { e.packPoint3D(v) })
	case dbtype.Vector[int8]:
		return e.packVector(inList, func() { e.packer.VectorInt8(v.Elems) })
	case dbtype.Vector[int16]:
		return e.packVector(inList, func() { e.packer.VectorInt16(v.Elems) })
	case dbtype.Vector[int32]:
		return e.packVector(inList, func() { e.packer.VectorInt32(v.Elems) })
	case dbtype.Vector[int64]:
		return e.packVector(inList, func() { e.packer.VectorInt64(v.Elems) })
	case dbtype.Vector[float32]:
		return e.packVector(inList, func() { e.packer.VectorFloat32(v.Elems) })
	case dbtype.Vector[float64]:
		return e.packVector(inList, func() { e.packer.VectorFloat64(v.Elems) })
	}

	return e.reflected(x, inList)
}

// reflected handles pointers and slices.
func (e *encoder) reflected(x any, inList bool) string {
	rv := reflect.ValueOf(x)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return e.value(nil, inList)
		}
		return e.value(rv.Elem().Interface(), inList)
	case reflect.Slice:
		if inList {
			e.setErr("a list stored as a property cannot contain another list")
			return ""
		}
		if !e.accept(TypeList) {
			return ""
		}
		e.packer.ArrayHeader(rv.Len())
		for i := 0; i < rv.Len(); i++ {
			e.value(rv.Index(i).Interface(), true)
			if e.err != nil {
				return ""
			}
		}
		return TypeList
	default:
		e.setErr("%s is not a Neo4j property type", reflect.TypeOf(x))
		return ""
	}
}

func (e *encoder) pack(typeName string, write func()) string {
	if !e.accept(typeName) {
		return ""
	}
	write()
	return typeName
}

func (e *encoder) packInt(i int64) string {
	return e.pack(TypeInteger, func() { e.packer.Int64(i) })
}

func (e *encoder) packUint(u uint64) string {
	return e.pack(TypeInteger, func() { e.packer.Uint64(u) })
}

func (e *encoder) packVector(inList bool, write func()) string {
	if inList {
		e.setErr("a list stored as a property cannot contain a vector")
		return ""
	}
	return e.pack(TypeVector, write)
}

func (e *encoder) packDate(d dbtype.Date) {
	t := time.Time(d)
	_, offset := t.Zone()
	// Floored, not truncated, so a date before 1970 lands on the day it names.
	seconds := t.Unix() + int64(offset)
	days := seconds / secondsPerDay
	if seconds%secondsPerDay < 0 {
		days--
	}
	e.packer.StructHeader('D', 1)
	e.packer.Int64(days)
}

func (e *encoder) packZonedTime(zt dbtype.Time) {
	t := time.Time(zt)
	_, offset := t.Zone()
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	e.packer.StructHeader('T', 2)
	e.packer.Int64(t.Sub(midnight).Nanoseconds())
	e.packer.Int(offset)
}

func (e *encoder) packLocalTime(lt dbtype.LocalTime) {
	t := time.Time(lt)
	nanos := int64(time.Hour)*int64(t.Hour()) +
		int64(time.Minute)*int64(t.Minute()) +
		int64(time.Second)*int64(t.Second()) +
		int64(t.Nanosecond())
	e.packer.StructHeader('t', 1)
	e.packer.Int64(nanos)
}

func (e *encoder) packLocalDateTime(ldt dbtype.LocalDateTime) {
	t := time.Time(ldt)
	_, offset := t.Zone()
	e.packer.StructHeader('d', 2)
	e.packer.Int64(t.Unix() + int64(offset))
	e.packer.Int(t.Nanosecond())
}

// packZonedDateTime always writes the UTC-based structures, whatever Bolt version the
// connection negotiated.
func (e *encoder) packZonedDateTime(t time.Time) {
	if zone, _ := t.Zone(); zone == "Offset" {
		_, offset := t.Zone()
		e.packer.StructHeader('I', 3)
		e.packer.Int64(t.Unix())
		e.packer.Int(t.Nanosecond())
		e.packer.Int(offset)
		return
	}
	e.packer.StructHeader('i', 3)
	e.packer.Int64(t.Unix())
	e.packer.Int(t.Nanosecond())
	e.packer.String(t.Location().String())
}

func (e *encoder) packPoint2D(p dbtype.Point2D) {
	e.packer.StructHeader('X', 3)
	e.packer.Uint32(p.SpatialRefId)
	e.packer.Float64(p.X)
	e.packer.Float64(p.Y)
}

func (e *encoder) packPoint3D(p dbtype.Point3D) {
	e.packer.StructHeader('Y', 4)
	e.packer.Uint32(p.SpatialRefId)
	e.packer.Float64(p.X)
	e.packer.Float64(p.Y)
	e.packer.Float64(p.Z)
}

func (e *encoder) packDuration(d dbtype.Duration) {
	e.packer.StructHeader('E', 4)
	e.packer.Int64(d.Months)
	e.packer.Int64(d.Days)
	e.packer.Int64(d.Seconds)
	e.packer.Int(d.Nanos)
}
