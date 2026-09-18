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
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
)

var (
	offsetZone = time.FixedZone("Offset", 3600)
	baseline10 = Version{Major: 1, Minor: 0}
)

// TestEncodeValueBytes pins the plaintext encoding of each property type, which other drivers
// must produce byte for byte.
func TestEncodeValueBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		wantHex  string
		wantType string
	}{
		{name: "true", value: true, wantHex: "c3", wantType: TypeBoolean},
		{name: "false", value: false, wantHex: "c2", wantType: TypeBoolean},
		{name: "zero", value: 0, wantHex: "00", wantType: TypeInteger},
		{name: "minus one", value: -1, wantHex: "ff", wantType: TypeInteger},
		{name: "int32 boundary", value: 32768, wantHex: "ca00008000", wantType: TypeInteger},
		{name: "int64", value: int64(-9223372036854775808), wantHex: "cb8000000000000000", wantType: TypeInteger},
		{name: "uint8", value: uint8(200), wantHex: "c900c8", wantType: TypeInteger},
		{name: "float", value: 3.25, wantHex: "c1400a000000000000", wantType: TypeFloat},
		{name: "float32 widens", value: float32(0.5), wantHex: "c13fe0000000000000", wantType: TypeFloat},
		{name: "empty string", value: "", wantHex: "80", wantType: TypeString},
		{name: "short string", value: "a", wantHex: "8161", wantType: TypeString},
		{name: "string", value: "hello world", wantHex: "8b68656c6c6f20776f726c64", wantType: TypeString},
		{
			// Encoded as supplied, without Unicode normalisation, so this decomposed form
			// does not match the composed one.
			name: "string is not normalised", value: "é",
			wantHex: "8365cc81", wantType: TypeString,
		},
		{name: "empty bytes", value: []byte{}, wantHex: "cc00", wantType: TypeBytes},
		{name: "bytes", value: []byte{0, 1, 2}, wantHex: "cc03000102", wantType: TypeBytes},
		{name: "list", value: []any{1, 2}, wantHex: "920102", wantType: TypeList},
		{name: "empty list", value: []any{}, wantHex: "90", wantType: TypeList},
		{name: "typed list", value: []string{"a"}, wantHex: "918161", wantType: TypeList},
		{
			name:    "uuid",
			value:   dbtype.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			wantHex: "e0000102030405060708090a0b0c0d0e0f", wantType: TypeUUID,
		},
		{
			name:    "date",
			value:   dbtype.Date(time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)),
			wantHex: "b14401", wantType: TypeDate,
		},
		{
			// Truncating division would put this on 1970-01-01.
			name:    "date before the epoch floors",
			value:   dbtype.Date(time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC)),
			wantHex: "b144ff", wantType: TypeDate,
		},
		{
			name:    "duration",
			value:   dbtype.Duration{Months: 1, Days: 2, Seconds: 3, Nanos: 4},
			wantHex: "b44501020304", wantType: TypeDuration,
		},
		{
			name:    "point 2d",
			value:   dbtype.Point2D{SpatialRefId: 7203, X: 1, Y: 2},
			wantHex: "b358c91c23c13ff0000000000000c14000000000000000", wantType: TypePoint,
		},
		{
			name:     "point 3d",
			value:    dbtype.Point3D{SpatialRefId: 9157, X: 1, Y: 2, Z: 3},
			wantHex:  "b459c923c5c13ff0000000000000c14000000000000000c1400800" + "0000000000",
			wantType: TypePoint,
		},
		{
			name:    "vector",
			value:   dbtype.Vector[int8]{Elems: []int8{1, -1}},
			wantHex: "b256cc01c8cc0201ff", wantType: TypeVector,
		},
		{
			name:    "local time",
			value:   dbtype.LocalTime(time.Date(0, 0, 0, 0, 0, 1, 2, time.UTC)),
			wantHex: "b174ca3b9aca02", wantType: TypeLocalTime,
		},
		{
			name:    "zoned date time with offset",
			value:   time.Unix(1, 2).In(offsetZone),
			wantHex: "b3490102c90e10", wantType: TypeZonedDateTime,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeValue(test.value)
			if err != nil {
				t.Fatalf("EncodeValue(%#v) returned %v", test.value, err)
			}
			if got := hex.EncodeToString(encoded.Bytes); got != test.wantHex {
				t.Errorf("encoded to %s, want %s", got, test.wantHex)
			}
			if encoded.TypeName != test.wantType {
				t.Errorf("type name is %q, want %q", encoded.TypeName, test.wantType)
			}
			if encoded.Baseline != baseline10 {
				t.Errorf("baseline is %s, want %s", encoded.Baseline, baseline10)
			}
		})
	}
}

// TestEncodeValueUsesUtcDateTimeStructures checks the UTC structures are always used, since
// the encoding does not follow the connection's Bolt version.
func TestEncodeValueUsesUtcDateTimeStructures(t *testing.T) {
	t.Parallel()

	london, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Skipf("no tzdata available: %v", err)
	}

	named, err := EncodeValue(time.Date(2026, 8, 21, 10, 0, 0, 0, london))
	if err != nil {
		t.Fatalf("EncodeValue returned %v", err)
	}
	if tag := named.Bytes[1]; tag != 'i' {
		t.Errorf("zoned date time with a zone id used tag %q, want %q", tag, 'i')
	}

	offset, err := EncodeValue(time.Date(2026, 8, 21, 10, 0, 0, 0, offsetZone))
	if err != nil {
		t.Fatalf("EncodeValue returned %v", err)
	}
	if tag := offset.Bytes[1]; tag != 'I' {
		t.Errorf("zoned date time with an offset used tag %q, want %q", tag, 'I')
	}
}

// TestEncodeValueDereferencesPointers checks a pointer encodes as the value it points at.
func TestEncodeValueDereferencesPointers(t *testing.T) {
	t.Parallel()

	value := "hello world"
	encoded, err := EncodeValue(&value)
	if err != nil {
		t.Fatalf("EncodeValue returned %v", err)
	}
	if got := hex.EncodeToString(encoded.Bytes); got != "8b68656c6c6f20776f726c64" {
		t.Errorf("encoded to %s", got)
	}
}

// TestEncodeValueRejects covers values that are not Neo4j property types.
func TestEncodeValueRejects(t *testing.T) {
	t.Parallel()

	var nilPointer *string

	tests := []struct {
		name  string
		value any
	}{
		{name: "nil", value: nil},
		{name: "nil pointer", value: nilPointer},
		{name: "map", value: map[string]any{"a": 1}},
		{name: "struct", value: struct{ A int }{A: 1}},
		{name: "channel", value: make(chan int)},
		{name: "array", value: [2]int{1, 2}},
		{name: "node", value: dbtype.Node{}},
		{name: "null in a list", value: []any{1, nil}},
		{name: "list in a list", value: []any{[]any{1}}},
		{name: "vector in a list", value: []any{dbtype.Vector[int8]{Elems: []int8{1}}}},
		{name: "map in a list", value: []any{map[string]any{}}},
		{name: "uint64 beyond int64", value: uint64(1) << 63},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := EncodeValue(test.value); err == nil {
				t.Fatalf("EncodeValue(%#v) succeeded, want an error", test.value)
			}
		})
	}
}

// TestEncodeAADAcceptsOnlyReproducibleTypes checks the AAD subset, which excludes types whose
// representation can vary between two equal values.
func TestEncodeAADAcceptsOnlyReproducibleTypes(t *testing.T) {
	t.Parallel()

	accepted := []any{
		true,
		[]byte{1},
		dbtype.Date(time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)),
		int64(1),
		dbtype.LocalTime(time.Date(0, 0, 0, 1, 0, 0, 0, time.UTC)),
		dbtype.Point2D{SpatialRefId: 7203, X: 1, Y: 2},
		dbtype.Point3D{SpatialRefId: 9157, X: 1, Y: 2, Z: 3},
		"row-42",
		dbtype.Time(time.Date(0, 0, 0, 1, 0, 0, 0, offsetZone)),
		dbtype.UUID{1},
	}
	for _, value := range accepted {
		if _, err := EncodeAAD(value); err != nil {
			t.Errorf("EncodeAAD(%T) returned %v, want success", value, err)
		}
	}

	rejected := []any{
		1.5,
		dbtype.Duration{Months: 1},
		[]any{1},
		dbtype.LocalDateTime(time.Now()),
		dbtype.Vector[int8]{Elems: []int8{1}},
		time.Now(),
	}
	for _, value := range rejected {
		_, err := EncodeAAD(value)
		if err == nil {
			t.Errorf("EncodeAAD(%T) succeeded, want an error", value)
			continue
		}
		var valueErr *ValueError
		if !errors.As(err, &valueErr) {
			t.Errorf("EncodeAAD(%T) returned %T, want a *ValueError", value, err)
		}
	}
}

// TestEncodeAADMatchesValueEncoding checks AAD encodes to the same bytes it would as a value.
func TestEncodeAADMatchesValueEncoding(t *testing.T) {
	t.Parallel()

	value := "row-42"
	asValue, err := EncodeValue(value)
	if err != nil {
		t.Fatalf("EncodeValue returned %v", err)
	}
	asAAD, err := EncodeAAD(value)
	if err != nil {
		t.Fatalf("EncodeAAD returned %v", err)
	}
	if hex.EncodeToString(asAAD.Bytes) != hex.EncodeToString(asValue.Bytes) {
		t.Errorf("AAD encoded to %x but the same value encoded to %x", asAAD.Bytes, asValue.Bytes)
	}
	if got := hex.EncodeToString(asAAD.Bytes); got != "86726f772d3432" {
		t.Errorf("AAD encoded to %s, want the bytes behind the TestKit aad-bound fixture", got)
	}
}
