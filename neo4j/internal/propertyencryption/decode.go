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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/packstream"
)

// MalformedError reports bytes that are not a well-formed encoding of a value.
type MalformedError struct {
	Message string
}

func (e *MalformedError) Error() string {
	return e.Message
}

// errUnsupported unwinds decoding when an encoding definition is missing. DecodeValue turns
// it into an UnsupportedType and it never reaches the caller.
var errUnsupported = errors.New("unsupported encoding definition")

// DecodeValue decodes plaintext bytes back into a Neo4j property value, using the type name
// and baseline version recorded with the encrypted value.
//
// An encoding this driver cannot interpret yields a *dbtype.UnsupportedType rather than an
// error.
func DecodeValue(plaintext []byte, typeName string, recorded Version) (any, error) {
	if unsupported := unsupportedBaseline(typeName, recorded); unsupported != nil {
		return unsupported, nil
	}

	d := decoder{recorded: recorded}
	d.unpacker.Reset(plaintext)
	value := d.value()
	switch {
	case errors.Is(d.err, errUnsupported):
		return newUnsupportedType(typeName, recorded,
			fmt.Sprintf("the encrypted value contains %s, which this driver cannot decode "+
				"under Bolt Value Encoding Scheme %s", d.unsupportedDetail, Implemented())), nil
	case d.err != nil:
		return nil, d.err
	case d.unpacker.Err != nil:
		return nil, &MalformedError{Message: d.unpacker.Err.Error()}
	case d.unpacker.Remaining() > 0:
		// The plaintext holds exactly one value.
		return nil, &MalformedError{Message: fmt.Sprintf(
			"%d bytes remain after the encrypted value", d.unpacker.Remaining())}
	}
	return value, nil
}

// unsupportedBaseline reports a *dbtype.UnsupportedType when the recorded baseline rules out
// decoding, and nil when decoding may proceed.
func unsupportedBaseline(typeName string, recorded Version) any {
	implemented := Implemented()
	if recorded.Major == implemented.Major {
		if implemented.Minor >= recorded.Minor {
			return nil
		}
		return newUnsupportedType(typeName, recorded,
			fmt.Sprintf("the encrypted value requires Bolt Value Encoding Scheme %s but this "+
				"driver implements %s, so the driver needs updating", recorded, implemented))
	}
	if recorded.Major > implemented.Major {
		return newUnsupportedType(typeName, recorded,
			fmt.Sprintf("the encrypted value requires Bolt Value Encoding Scheme %s, which is "+
				"newer than the %s implemented by this driver, so the driver needs updating",
				recorded, implemented))
	}
	return newUnsupportedType(typeName, recorded,
		fmt.Sprintf("the encrypted value requires Bolt Value Encoding Scheme %s, which this "+
			"driver no longer supports", recorded))
}

func newUnsupportedType(typeName string, recorded Version, message string) *dbtype.UnsupportedType {
	return &dbtype.UnsupportedType{
		Name: typeName,
		MinimumProtocolVersion: db.ProtocolVersion{
			Major: recorded.Major,
			Minor: recorded.Minor,
		},
		Message: &message,
	}
}

type decoder struct {
	unpacker          packstream.Unpacker
	recorded          Version
	err               error
	unsupportedDetail string
}

func (d *decoder) setErr(err error) {
	if d.err == nil {
		d.err = err
	}
}

func (d *decoder) malformed(format string, args ...any) {
	d.setErr(&MalformedError{Message: fmt.Sprintf(format, args...)})
}

// unsupported abandons decoding because detail has no applicable encoding definition.
func (d *decoder) unsupported(format string, args ...any) {
	if d.err == nil {
		d.unsupportedDetail = fmt.Sprintf(format, args...)
		d.err = errUnsupported
	}
}

// require checks the encoding definition for typeName is no newer than the recorded
// baseline, since a newer one would not match the bytes as written.
func (d *decoder) require(typeName string) bool {
	baseline, ok := typeBaselines[typeName]
	if !ok {
		d.unsupported("the %s type", typeName)
		return false
	}
	if !d.recorded.atLeast(baseline) {
		d.unsupported("a %s encoded under scheme %s", typeName, baseline)
		return false
	}
	return true
}

func (d *decoder) value() any {
	if d.err != nil {
		return nil
	}
	d.unpacker.Next()
	switch d.unpacker.Curr {
	case packstream.PackedInt:
		if !d.require(TypeInteger) {
			return nil
		}
		return d.unpacker.Int()
	case packstream.PackedFloat:
		if !d.require(TypeFloat) {
			return nil
		}
		return d.unpacker.Float()
	case packstream.PackedStr:
		if !d.require(TypeString) {
			return nil
		}
		return d.unpacker.String()
	case packstream.PackedByteArray:
		if !d.require(TypeBytes) {
			return nil
		}
		return d.unpacker.ByteArray()
	case packstream.PackedTrue, packstream.PackedFalse:
		if !d.require(TypeBoolean) {
			return nil
		}
		return d.unpacker.Bool()
	case packstream.PackedUUID:
		if !d.require(TypeUUID) {
			return nil
		}
		return dbtype.UUID(d.unpacker.UUID())
	case packstream.PackedArray:
		return d.list()
	case packstream.PackedStruct:
		return d.structure()
	case packstream.PackedNil:
		d.malformed("null is not a Neo4j property type")
		return nil
	default:
		d.unsupported("a PackStream type this driver does not recognise")
		return nil
	}
}

func (d *decoder) list() any {
	if !d.require(TypeList) {
		return nil
	}
	length := d.unpacker.Len()
	if d.unpacker.Err != nil {
		return nil
	}
	items := make([]any, 0, min(length, 1024))
	for i := uint32(0); i < length; i++ {
		item := d.value()
		if d.err != nil {
			return nil
		}
		items = append(items, item)
	}
	return items
}

func (d *decoder) structure() any {
	tag := d.unpacker.StructTag()
	fields := d.unpacker.Len()
	if d.unpacker.Err != nil {
		return nil
	}

	switch tag {
	case 'D':
		return d.date(fields)
	case 'T':
		return d.zonedTime(fields)
	case 't':
		return d.localTime(fields)
	case 'd':
		return d.localDateTime(fields)
	case 'I':
		return d.zonedDateTimeOffset(fields)
	case 'i':
		return d.zonedDateTimeZoneId(fields)
	case 'E':
		return d.duration(fields)
	case 'X':
		return d.point2D(fields)
	case 'Y':
		return d.point3D(fields)
	case 'V':
		return d.vector(fields)
	default:
		d.unsupported("a structure tagged %#x", tag)
		return nil
	}
}

func (d *decoder) fieldCount(name string, expected, actual uint32) bool {
	if actual != expected {
		d.malformed("%s should have %d fields but has %d", name, expected, actual)
		return false
	}
	return true
}

func (d *decoder) int() int64 {
	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedInt {
		d.malformed("expected an integer field")
		return 0
	}
	return d.unpacker.Int()
}

func (d *decoder) float() float64 {
	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedFloat {
		d.malformed("expected a float field")
		return 0
	}
	return d.unpacker.Float()
}

func (d *decoder) string() string {
	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedStr {
		d.malformed("expected a string field")
		return ""
	}
	return d.unpacker.String()
}

func (d *decoder) bytes() []byte {
	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedByteArray {
		d.malformed("expected a byte array field")
		return nil
	}
	return d.unpacker.ByteArray()
}

func (d *decoder) date(fields uint32) any {
	if !d.require(TypeDate) || !d.fieldCount("Date", 1, fields) {
		return nil
	}
	days := d.int()
	if d.err != nil {
		return nil
	}
	return dbtype.Date(time.Unix(days*secondsPerDay, 0).UTC())
}

func (d *decoder) zonedTime(fields uint32) any {
	if !d.require(TypeZonedTime) || !d.fieldCount("Time", 2, fields) {
		return nil
	}
	nanos := d.int()
	offset := d.int()
	if d.err != nil {
		return nil
	}
	seconds := nanos / int64(time.Second)
	nanos -= seconds * int64(time.Second)
	zone := time.FixedZone("Offset", int(offset))
	return dbtype.Time(time.Date(0, 0, 0, 0, 0, int(seconds), int(nanos), zone))
}

func (d *decoder) localTime(fields uint32) any {
	if !d.require(TypeLocalTime) || !d.fieldCount("LocalTime", 1, fields) {
		return nil
	}
	nanos := d.int()
	if d.err != nil {
		return nil
	}
	seconds := nanos / int64(time.Second)
	nanos -= seconds * int64(time.Second)
	return dbtype.LocalTime(time.Date(0, 0, 0, 0, 0, int(seconds), int(nanos), time.Local))
}

func (d *decoder) localDateTime(fields uint32) any {
	if !d.require(TypeLocalDateTime) || !d.fieldCount("LocalDateTime", 2, fields) {
		return nil
	}
	seconds := d.int()
	nanos := d.int()
	if d.err != nil {
		return nil
	}
	// Matches how the driver hydrates a LocalDateTime off the wire.
	t := time.Unix(seconds, nanos).UTC()
	return dbtype.LocalDateTime(time.Date(
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local))
}

func (d *decoder) zonedDateTimeOffset(fields uint32) any {
	if !d.require(TypeZonedDateTime) || !d.fieldCount("DateTime", 3, fields) {
		return nil
	}
	seconds := d.int()
	nanos := d.int()
	offset := d.int()
	if d.err != nil {
		return nil
	}
	zone := time.FixedZone("Offset", int(offset))
	return time.Unix(seconds, nanos).In(zone)
}

func (d *decoder) zonedDateTimeZoneId(fields uint32) any {
	if !d.require(TypeZonedDateTime) || !d.fieldCount("DateTimeZoneId", 3, fields) {
		return nil
	}
	seconds := d.int()
	nanos := d.int()
	id := d.string()
	if d.err != nil {
		return nil
	}
	zone, err := time.LoadLocation(id)
	if err != nil {
		d.malformed("unknown time zone %q: %s", id, err)
		return nil
	}
	return time.Unix(seconds, nanos).In(zone)
}

func (d *decoder) duration(fields uint32) any {
	if !d.require(TypeDuration) || !d.fieldCount("Duration", 4, fields) {
		return nil
	}
	months := d.int()
	days := d.int()
	seconds := d.int()
	nanos := d.int()
	if d.err != nil {
		return nil
	}
	return dbtype.Duration{Months: months, Days: days, Seconds: seconds, Nanos: int(nanos)}
}

func (d *decoder) point2D(fields uint32) any {
	if !d.require(TypePoint) || !d.fieldCount("Point2D", 3, fields) {
		return nil
	}
	srid := d.int()
	x := d.float()
	y := d.float()
	if d.err != nil {
		return nil
	}
	return dbtype.Point2D{SpatialRefId: uint32(srid), X: x, Y: y}
}

func (d *decoder) point3D(fields uint32) any {
	if !d.require(TypePoint) || !d.fieldCount("Point3D", 4, fields) {
		return nil
	}
	srid := d.int()
	x := d.float()
	y := d.float()
	z := d.float()
	if d.err != nil {
		return nil
	}
	return dbtype.Point3D{SpatialRefId: uint32(srid), X: x, Y: y, Z: z}
}

func (d *decoder) vector(fields uint32) any {
	if !d.require(TypeVector) || !d.fieldCount("Vector", 2, fields) {
		return nil
	}
	marker := d.bytes()
	elements := d.bytes()
	if d.err != nil {
		return nil
	}
	if len(marker) != 1 {
		d.malformed("a Vector type marker should be 1 byte but is %d", len(marker))
		return nil
	}

	switch marker[0] {
	case 0xc1:
		return decodeVector[float64](d, elements, 8, func(b []byte) float64 {
			return math.Float64frombits(binary.BigEndian.Uint64(b))
		})
	case 0xc6:
		return decodeVector[float32](d, elements, 4, func(b []byte) float32 {
			return math.Float32frombits(binary.BigEndian.Uint32(b))
		})
	case 0xc8:
		return decodeVector[int8](d, elements, 1, func(b []byte) int8 { return int8(b[0]) })
	case 0xc9:
		return decodeVector[int16](d, elements, 2, func(b []byte) int16 {
			return int16(binary.BigEndian.Uint16(b))
		})
	case 0xca:
		return decodeVector[int32](d, elements, 4, func(b []byte) int32 {
			return int32(binary.BigEndian.Uint32(b))
		})
	case 0xcb:
		return decodeVector[int64](d, elements, 8, func(b []byte) int64 {
			return int64(binary.BigEndian.Uint64(b))
		})
	default:
		d.unsupported("a Vector with element marker %#x", marker[0])
		return nil
	}
}

func decodeVector[T dbtype.VectorElement](d *decoder, elements []byte, width int, read func([]byte) T) any {
	if len(elements)%width != 0 {
		d.malformed("Vector values should be a multiple of %d bytes but are %d", width, len(elements))
		return nil
	}
	elems := make([]T, 0, len(elements)/width)
	for i := 0; i < len(elements); i += width {
		elems = append(elems, read(elements[i:i+width]))
	}
	return dbtype.Vector[T]{Elems: elems}
}
