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
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
)

// TestRoundTrip encodes and decodes every property type, checking each comes back unchanged
// and re-encodes to the same bytes.
func TestRoundTrip(t *testing.T) {
	t.Parallel()

	london, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Skipf("no tzdata available: %v", err)
	}

	tests := []struct {
		name  string
		value any
		want  any
	}{
		{name: "true", value: true},
		{name: "false", value: false},
		{name: "zero", value: int64(0)},
		{name: "negative", value: int64(-1)},
		{name: "int64 min", value: int64(-9223372036854775808)},
		{name: "int64 max", value: int64(9223372036854775807)},
		{name: "int widens to int64", value: 42, want: int64(42)},
		{name: "float", value: 3.25},
		{name: "negative zero float", value: math_NegZero},
		{name: "empty string", value: ""},
		{name: "string", value: "hello world"},
		{name: "multi byte string", value: "héllo wörld 🙂"},
		{name: "empty bytes", value: []byte{}},
		{name: "bytes", value: []byte{0, 1, 2, 255}},
		{name: "empty list", value: []any{}},
		{name: "list", value: []any{int64(1), int64(2)}},
		{name: "list of strings", value: []string{"a", "b"}, want: []any{"a", "b"}},
		{name: "long list", value: longList(), want: longListDecoded()},
		{
			name:  "mixed list",
			value: []any{int64(1), "a", true},
			want:  []any{int64(1), "a", true},
		},
		{name: "uuid", value: dbtype.UUID{1, 2, 3}},
		{name: "duration", value: dbtype.Duration{Months: 1, Days: 2, Seconds: 3, Nanos: 4}},
		{name: "negative duration", value: dbtype.Duration{Months: -1, Days: -2, Seconds: -3, Nanos: 4}},
		{name: "point 2d", value: dbtype.Point2D{SpatialRefId: 7203, X: 1.5, Y: -2.5}},
		{name: "point 3d", value: dbtype.Point3D{SpatialRefId: 9157, X: 1.5, Y: -2.5, Z: 0}},
		{name: "vector int8", value: dbtype.Vector[int8]{Elems: []int8{1, -1, 127}}},
		{name: "vector int16", value: dbtype.Vector[int16]{Elems: []int16{1, -32768}}},
		{name: "vector int32", value: dbtype.Vector[int32]{Elems: []int32{1, -2}}},
		{name: "vector int64", value: dbtype.Vector[int64]{Elems: []int64{1, -2}}},
		{name: "vector float32", value: dbtype.Vector[float32]{Elems: []float32{1.5, -2.5}}},
		{name: "vector float64", value: dbtype.Vector[float64]{Elems: []float64{1.5, -2.5}}},
		{name: "empty vector", value: dbtype.Vector[float64]{Elems: []float64{}}},
		{name: "date", value: dbtype.Date(time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC))},
		{name: "date before the epoch", value: dbtype.Date(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))},
		{name: "local time", value: dbtype.LocalTime(time.Date(0, 0, 0, 23, 59, 59, 999999999, time.UTC))},
		{name: "zoned time", value: dbtype.Time(time.Date(0, 0, 0, 1, 2, 3, 4, offsetZone))},
		{
			name:  "zoned time with a negative offset",
			value: dbtype.Time(time.Date(0, 0, 0, 1, 2, 3, 4, time.FixedZone("Offset", -18000))),
		},
		{
			name:  "local date time",
			value: dbtype.LocalDateTime(time.Date(2026, 8, 21, 10, 30, 0, 500, time.UTC)),
		},
		{name: "zoned date time with an offset", value: time.Date(2026, 8, 21, 10, 0, 0, 7, offsetZone)},
		{name: "zoned date time with a zone id", value: time.Date(2026, 8, 21, 10, 0, 0, 7, london)},
		{
			name:  "zoned date time before the epoch",
			value: time.Date(1900, 1, 1, 10, 0, 0, 0, offsetZone),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			want := test.want
			if want == nil {
				want = test.value
			}

			encoded, err := EncodeValue(test.value)
			if err != nil {
				t.Fatalf("EncodeValue(%#v) returned %v", test.value, err)
			}
			decoded, err := DecodeValue(encoded.Bytes, encoded.TypeName, encoded.Baseline)
			if err != nil {
				t.Fatalf("DecodeValue returned %v", err)
			}
			if !sameValue(decoded, want) {
				t.Fatalf("round tripped %#v to %#v", want, decoded)
			}

			// Re-encoding must land on the same bytes.
			reencoded, err := EncodeValue(decoded)
			if err != nil {
				t.Fatalf("re-encoding the decoded value returned %v", err)
			}
			if !reflect.DeepEqual(reencoded.Bytes, encoded.Bytes) {
				t.Errorf("re-encoded to %x, want %x", reencoded.Bytes, encoded.Bytes)
			}
		})
	}
}

// TestDecodeValueRejectsUnsupportedBaseline covers baselines outside the supported range.
func TestDecodeValueRejectsUnsupportedBaseline(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeValue("hello world")
	if err != nil {
		t.Fatalf("EncodeValue returned %v", err)
	}

	tests := []struct {
		name     string
		recorded Version
	}{
		{name: "newer major", recorded: Version{Major: 2, Minor: 0}},
		{name: "newer minor", recorded: Version{Major: 1, Minor: 1}},
		{name: "older major", recorded: Version{Major: 0, Minor: 9}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decoded, err := DecodeValue(encoded.Bytes, TypeString, test.recorded)
			if err != nil {
				t.Fatalf("DecodeValue returned %v, want an UnsupportedType", err)
			}
			unsupported, ok := decoded.(*dbtype.UnsupportedType)
			if !ok {
				t.Fatalf("DecodeValue returned %T, want *dbtype.UnsupportedType", decoded)
			}
			if unsupported.Name != TypeString {
				t.Errorf("Name is %q, want %q", unsupported.Name, TypeString)
			}
			if unsupported.MinimumProtocolVersion.Major != test.recorded.Major ||
				unsupported.MinimumProtocolVersion.Minor != test.recorded.Minor {
				t.Errorf("version is %v, want %s", unsupported.MinimumProtocolVersion, test.recorded)
			}
			if unsupported.Message == nil || *unsupported.Message == "" {
				t.Error("Message is empty, users need to be told why the value cannot be read")
			}
		})
	}
}

// TestDecodeValueRejectsUnknownEncodings checks encodings outside scheme 1.0 surface as an
// UnsupportedType rather than a wrong value or a panic.
func TestDecodeValueRejectsUnknownEncodings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
	}{
		{name: "legacy date time with an offset", plaintext: "b3460102c90e10"},
		{name: "legacy date time with a zone id", plaintext: "b3660102816c"},
		{name: "node", plaintext: "b34e0190a0"},
		{name: "unassigned struct tag", plaintext: "b17a01"},
		{name: "dictionary", plaintext: "a18161 01"},
		{name: "unknown vector element marker", plaintext: "b256cc01ffcc0201ff"},
		{name: "list holding a dictionary", plaintext: "91a0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plaintext := mustHex(t, test.plaintext)
			decoded, err := DecodeValue(plaintext, "SOMETHING", baseline10)
			if err != nil {
				t.Fatalf("DecodeValue returned %v, want an UnsupportedType", err)
			}
			unsupported, ok := decoded.(*dbtype.UnsupportedType)
			if !ok {
				t.Fatalf("DecodeValue returned %#v, want *dbtype.UnsupportedType", decoded)
			}
			if unsupported.Message == nil || *unsupported.Message == "" {
				t.Error("Message is empty")
			}
		})
	}
}

// TestDecodeValueRejectsMalformedPlaintext covers bytes that are not a valid encoding.
func TestDecodeValueRejectsMalformedPlaintext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
	}{
		{name: "empty", plaintext: ""},
		{name: "null", plaintext: "c0"},
		{name: "truncated string", plaintext: "8b68656c6c6f"},
		{name: "truncated list", plaintext: "930102"},
		{name: "trailing bytes", plaintext: "920102cb"},
		{name: "date with the wrong field count", plaintext: "b2440101"},
		{name: "point with a non float field", plaintext: "b358c91c2301c14000000000000000"},
		{name: "vector with a wide type marker", plaintext: "b256cc02c8c8cc0201ff"},
		{name: "vector with a partial element", plaintext: "b256cc01c9cc0301ff02"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plaintext := mustHex(t, test.plaintext)
			decoded, err := DecodeValue(plaintext, TypeString, baseline10)
			if err == nil {
				t.Fatalf("DecodeValue returned %#v, want an error", decoded)
			}
		})
	}
}

// TestDecodeValueRejectsUnknownZone checks an unrecognised zone fails rather than resolving
// to a different instant.
func TestDecodeValueRejectsUnknownZone(t *testing.T) {
	t.Parallel()

	// DateTimeZoneId(0, 0, "Mars/Olympus_Mons").
	plaintext := mustHex(t, "b3690000"+"d111"+hex.EncodeToString([]byte("Mars/Olympus_Mons")))
	decoded, err := DecodeValue(plaintext, TypeZonedDateTime, baseline10)
	if err == nil {
		t.Fatalf("DecodeValue returned %#v, want an error", decoded)
	}
	var malformed *MalformedError
	if !errors.As(err, &malformed) {
		t.Fatalf("DecodeValue returned %T, want a *MalformedError", err)
	}
}

var math_NegZero = negZero()

func negZero() float64 {
	zero := 0.0
	return -zero
}

func longList() []any {
	items := make([]any, 300)
	for i := range items {
		items[i] = int64(i)
	}
	return items
}

func longListDecoded() []any {
	return longList()
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	clean := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' {
			clean = append(clean, s[i])
		}
	}
	decoded, err := hex.DecodeString(string(clean))
	if err != nil {
		t.Fatalf("bad test hex %q: %v", s, err)
	}
	return decoded
}

// sameValue compares two property values. Local temporal types are hydrated into the process
// zone so only their wall clock is compared, while a zoned date time keeps instant and zone.
func sameValue(got, want any) bool {
	switch w := want.(type) {
	case time.Time:
		g, ok := got.(time.Time)
		return ok && g.Equal(w) && g.Location().String() == w.Location().String()
	case dbtype.Date:
		g, ok := got.(dbtype.Date)
		if !ok {
			return false
		}
		gy, gm, gd := time.Time(g).Date()
		wy, wm, wd := time.Time(w).Date()
		return gy == wy && gm == wm && gd == wd
	case dbtype.LocalTime:
		g, ok := got.(dbtype.LocalTime)
		return ok && sameClock(time.Time(g), time.Time(w))
	case dbtype.LocalDateTime:
		g, ok := got.(dbtype.LocalDateTime)
		if !ok {
			return false
		}
		gy, gm, gd := time.Time(g).Date()
		wy, wm, wd := time.Time(w).Date()
		return gy == wy && gm == wm && gd == wd && sameClock(time.Time(g), time.Time(w))
	case dbtype.Time:
		g, ok := got.(dbtype.Time)
		if !ok {
			return false
		}
		_, gOffset := time.Time(g).Zone()
		_, wOffset := time.Time(w).Zone()
		return gOffset == wOffset && sameClock(time.Time(g), time.Time(w))
	default:
		return reflect.DeepEqual(got, want)
	}
}

func sameClock(a, b time.Time) bool {
	ah, am, as := a.Clock()
	bh, bm, bs := b.Clock()
	return ah == bh && am == bm && as == bs && a.Nanosecond() == b.Nanosecond()
}
