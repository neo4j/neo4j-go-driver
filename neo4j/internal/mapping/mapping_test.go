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

package mapping

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStructAsMap(t *testing.T) {
	type movie struct {
		Title    string
		Released int64
	}
	type tagged struct {
		Title string `neo4j:"title"`
		Skip  string `neo4j:"-"`
		Empty string `neo4j:",omitempty"`
	}
	type base struct{ ID string }
	type embedded struct {
		base
		Name string
	}
	type Base struct{ ID string }
	type pointerEmbedded struct {
		*Base
		Name string
	}
	type withUnexported struct {
		Name   string
		secret string
	}
	type withNested struct {
		Inner movie
	}
	type withTime struct {
		At time.Time
	}
	type withPointer struct {
		Mother *string
	}
	type withSlice struct {
		Tags []string
	}
	type allOmit struct {
		A string `neo4j:",omitempty"`
		B string `neo4j:",omitempty"`
	}
	type withOmitPointer struct {
		Mother *string `neo4j:",omitempty"`
	}
	type innerNamed struct{ Name string }
	type outerNamed struct {
		innerNamed
		Name string
	}

	hello := "hi"

	cases := []struct {
		name      string
		in        any
		want      map[string]any
		notStruct bool
	}{
		{
			name: "untagged exported fields use field name verbatim",
			in:   movie{Title: "The Matrix", Released: 1999},
			want: map[string]any{"Title": "The Matrix", "Released": int64(1999)},
		},
		{
			name: "tag renames, dash skips, omitempty drops zero",
			in:   tagged{Title: "x"},
			want: map[string]any{"title": "x"},
		},
		{
			name: "tag omitempty keeps non-zero value",
			in:   tagged{Title: "x", Empty: "y", Skip: "ignored"},
			want: map[string]any{"title": "x", "Empty": "y"},
		},
		{
			name: "unexported fields are skipped",
			in:   withUnexported{Name: "n", secret: "s"},
			want: map[string]any{"Name": "n"},
		},
		{
			name: "anonymous embedded struct flattens",
			in:   embedded{base: base{ID: "1"}, Name: "Alice"},
			want: map[string]any{"ID": "1", "Name": "Alice"},
		},
		{
			name: "pointer-embedded struct flattens",
			in:   pointerEmbedded{Base: &Base{ID: "1"}, Name: "Alice"},
			want: map[string]any{"ID": "1", "Name": "Alice"},
		},
		{
			name: "nil pointer-embedded struct skips its fields",
			in:   pointerEmbedded{Name: "Alice"},
			want: map[string]any{"Name": "Alice"},
		},
		{
			name: "nested struct value passes through unchanged",
			in:   withNested{Inner: movie{Title: "Heat", Released: 1995}},
			want: map[string]any{"Inner": movie{Title: "Heat", Released: 1995}},
		},
		{
			name: "time.Time field passes through unchanged",
			in:   withTime{At: time.Unix(1700000000, 0).UTC()},
			want: map[string]any{"At": time.Unix(1700000000, 0).UTC()},
		},
		{
			name: "non-nil pointer field passes through",
			in:   withPointer{Mother: &hello},
			want: map[string]any{"Mother": &hello},
		},
		{
			name: "nil pointer field included as typed nil",
			in:   withPointer{Mother: nil},
			want: map[string]any{"Mother": (*string)(nil)},
		},
		{
			name: "slice field passes through unchanged",
			in:   withSlice{Tags: []string{"a", "b"}},
			want: map[string]any{"Tags": []string{"a", "b"}},
		},
		{
			name: "pointer to struct dereferences",
			in:   &movie{Title: "Speed", Released: 1994},
			want: map[string]any{"Title": "Speed", "Released": int64(1994)},
		},
		{
			name: "nil pointer to struct returns nil map",
			in:   (*movie)(nil),
			want: nil,
		},
		{
			name: "all fields omitempty and zero produces empty map",
			in:   allOmit{},
			want: map[string]any{},
		},
		{
			name: "nil pointer field with omitempty is skipped",
			in:   withOmitPointer{Mother: nil},
			want: map[string]any{},
		},
		{
			name: "non-nil pointer field with omitempty is included",
			in:   withOmitPointer{Mother: &hello},
			want: map[string]any{"Mother": &hello},
		},
		{
			name: "outer field shadows embedded field of the same name",
			in:   outerNamed{innerNamed: innerNamed{Name: "inner"}, Name: "outer"},
			want: map[string]any{"Name": "outer"},
		},
		{
			name:      "non-struct input returns ok=false",
			in:        "not a struct",
			notStruct: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, ok := StructAsMap(c.in)
			if c.notStruct {
				if ok {
					t.Fatalf("expected ok=false for non-struct input, got map %#v", got)
				}
				return
			}
			if !ok {
				t.Fatalf("unexpected ok=false")
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("StructAsMap mismatch\nwant: %#v\n got: %#v", c.want, got)
			}
		})
	}
}

// fakeNode stands in for dbtype.Node/Relationship: it satisfies the propertied
// interface so MapToStruct maps its properties into a nested struct field.
type fakeNode struct{ props map[string]any }

func (n fakeNode) GetProperties() map[string]any { return n.props }

func TestMapToStruct(t *testing.T) {
	type movie struct {
		Title    string
		Released int64
	}
	type tagged struct {
		Title string `neo4j:"title"`
		Skip  string `neo4j:"-"`
	}
	type withNumeric struct {
		Count   int
		Rating  float64
		Rating2 float32
		Small   int32
		Unsent  uint
	}
	type scalars struct {
		Active bool
		Blob   []byte
	}
	type withMap struct {
		Meta map[string]any
	}
	type withTypedMap struct {
		Scores map[string]int
	}
	type withMapAnyKey struct {
		Scores map[any]int
	}
	type withMapUntyped struct {
		Scores map[any]any
	}
	type status string
	type withNamed struct {
		State status
	}
	type withScalarPointer struct {
		Name *string
	}
	type withSlice struct {
		Tags []string
	}
	type withSliceUntyped struct {
		Things []any
	}
	type withPointerSlice struct {
		Cast []*movie
	}
	type withNestedSlice struct {
		Cast []movie
	}
	type withNested struct {
		Inner movie
	}
	type withPointer struct {
		Inner *movie
	}
	type withTime struct {
		At time.Time
	}
	type base struct{ ID string }
	type embedded struct {
		base
		Name string
	}
	type Base struct{ ID string }
	type pointerEmbedded struct {
		*Base
		Name string
	}
	type innerNamed struct{ Name string }
	type outerNamed struct {
		innerNamed
		Name string
	}
	type withAny struct {
		V any
	}

	name := "Trinity"

	cases := []struct {
		name string
		src  map[string]any
		want any // pointer to expected struct
	}{
		{
			name: "scalar fields by name",
			src:  map[string]any{"Title": "The Matrix", "Released": int64(1999)},
			want: &movie{Title: "The Matrix", Released: 1999},
		},
		{
			name: "empty source leaves struct zero",
			src:  map[string]any{},
			want: &movie{},
		},
		{
			name: "nil source leaves struct zero",
			src:  nil,
			want: &movie{},
		},
		{
			name: "tag renames and dash skips",
			src:  map[string]any{"title": "Speed", "Skip": "ignored"},
			want: &tagged{Title: "Speed"},
		},
		{
			name: "missing key leaves zero value",
			src:  map[string]any{"Title": "Heat"},
			want: &movie{Title: "Heat"},
		},
		{
			name: "null value leaves zero value",
			src:  map[string]any{"Title": "Heat", "Released": nil},
			want: &movie{Title: "Heat"},
		},
		{
			name: "extra key is ignored",
			src:  map[string]any{"Title": "Heat", "Released": int64(1995), "extra": "x"},
			want: &movie{Title: "Heat", Released: 1995},
		},
		{
			name: "numeric conversion from bolt int64/float64",
			src:  map[string]any{"Count": int64(3), "Rating": float64(4.5), "Rating2": float64(1.5), "Small": int64(7), "Unsent": int64(9)},
			want: &withNumeric{Count: 3, Rating: 4.5, Rating2: 1.5, Small: 7, Unsent: 9},
		},
		{
			name: "integer widens into a float field",
			src:  map[string]any{"Rating": int64(5)},
			want: &withNumeric{Rating: 5},
		},
		{
			name: "bool and byte-slice fields",
			src:  map[string]any{"Active": true, "Blob": []byte{1, 2, 3}},
			want: &scalars{Active: true, Blob: []byte{1, 2, 3}},
		},
		{
			name: "map field assigned verbatim",
			src:  map[string]any{"Meta": map[string]any{"k": int64(1)}},
			want: &withMap{Meta: map[string]any{"k": int64(1)}},
		},
		{
			name: "named type over string is converted",
			src:  map[string]any{"State": "active"},
			want: &withNamed{State: status("active")},
		},
		{
			name: "typed map converts its values",
			src:  map[string]any{"Scores": map[string]any{"a": int64(1), "b": int64(2)}},
			want: &withTypedMap{Scores: map[string]int{"a": 1, "b": 2}},
		},
		{
			name: "typed map converts its keys",
			src:  map[string]any{"Scores": map[string]any{"a": int64(1), "b": int64(2)}},
			want: &withMapAnyKey{Scores: map[any]int{"a": 1, "b": 2}},
		},
		{
			name: "typed map converts keys and values into an untyped map",
			src:  map[string]any{"Scores": map[string]any{"a": int64(1), "b": int64(2)}},
			want: &withMapUntyped{Scores: map[any]any{"a": int64(1), "b": int64(2)}},
		},
		{
			name: "scalar pointer field is allocated",
			src:  map[string]any{"Name": "Trinity"},
			want: &withScalarPointer{Name: &name},
		},
		{
			name: "null leaves a struct pointer field nil",
			src:  map[string]any{"Inner": nil},
			want: &withPointer{Inner: nil},
		},
		{
			name: "null leaves a scalar pointer field nil",
			src:  map[string]any{"Name": nil},
			want: &withScalarPointer{Name: nil},
		},
		{
			name: "scalar slice",
			src:  map[string]any{"Tags": []any{"a", "b"}},
			want: &withSlice{Tags: []string{"a", "b"}},
		},
		{
			name: "typed slice coerces into an untyped slice field",
			src:  map[string]any{"Things": []string{"a", "b"}},
			want: &withSliceUntyped{Things: []any{"a", "b"}},
		},
		{
			name: "mixed slice maps into an untyped slice field",
			src:  map[string]any{"Things": []any{"a", "b", int64(123)}},
			want: &withSliceUntyped{Things: []any{"a", "b", int64(123)}},
		},
		{
			name: "slice of struct pointers",
			src:  map[string]any{"Cast": []any{map[string]any{"Title": "A"}, map[string]any{"Title": "B"}}},
			want: &withPointerSlice{Cast: []*movie{{Title: "A"}, {Title: "B"}}},
		},
		{
			name: "slice of nested structs from maps",
			src:  map[string]any{"Cast": []any{map[string]any{"Title": "A", "Released": int64(1)}, map[string]any{"Title": "B"}}},
			want: &withNestedSlice{Cast: []movie{{Title: "A", Released: 1}, {Title: "B"}}},
		},
		{
			name: "nested struct from map",
			src:  map[string]any{"Inner": map[string]any{"Title": "Nested", "Released": int64(2)}},
			want: &withNested{Inner: movie{Title: "Nested", Released: 2}},
		},
		{
			name: "nested struct from node properties",
			src:  map[string]any{"Inner": fakeNode{props: map[string]any{"Title": "FromNode", "Released": int64(3)}}},
			want: &withNested{Inner: movie{Title: "FromNode", Released: 3}},
		},
		{
			name: "time.Time field assigned as-is",
			src:  map[string]any{"At": time.Unix(1700000000, 0).UTC()},
			want: &withTime{At: time.Unix(1700000000, 0).UTC()},
		},
		{
			name: "pointer field is allocated",
			src:  map[string]any{"Inner": map[string]any{"Title": "Ptr"}},
			want: &withPointer{Inner: &movie{Title: "Ptr"}},
		},
		{
			name: "anonymous embedded struct is populated",
			src:  map[string]any{"ID": "1", "Name": "Alice"},
			want: &embedded{base: base{ID: "1"}, Name: "Alice"},
		},
		{
			name: "nil pointer embed is allocated",
			src:  map[string]any{"ID": "1", "Name": "Alice"},
			want: &pointerEmbedded{Base: &Base{ID: "1"}, Name: "Alice"},
		},
		{
			name: "outer field shadows embedded field of the same name",
			src:  map[string]any{"Name": "outer"},
			want: &outerNamed{Name: "outer"},
		},
		{
			name: "any field takes value verbatim",
			src:  map[string]any{"V": []any{int64(1), "two"}},
			want: &withAny{V: []any{int64(1), "two"}},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := reflect.New(reflect.TypeOf(c.want).Elem()).Interface()
			if err := MapToStruct(c.src, got); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("MapToStruct mismatch\nwant: %#v\n got: %#v", c.want, got)
			}
		})
	}
}

func TestMapToStructErrors(t *testing.T) {
	t.Parallel()
	type movie struct {
		Title    string
		Released int64
	}

	t.Run("non-pointer destination", func(t *testing.T) {
		t.Parallel()
		if err := MapToStruct(map[string]any{}, movie{}); err == nil {
			t.Fatal("expected error for non-pointer destination")
		}
	})
	t.Run("nil pointer destination", func(t *testing.T) {
		t.Parallel()
		if err := MapToStruct(map[string]any{}, (*movie)(nil)); err == nil {
			t.Fatal("expected error for nil pointer destination")
		}
	})
	t.Run("pointer to non-struct", func(t *testing.T) {
		t.Parallel()
		var s string
		if err := MapToStruct(map[string]any{}, &s); err == nil {
			t.Fatal("expected error for pointer to non-struct")
		}
	})
	t.Run("does not parse numeric strings", func(t *testing.T) {
		t.Parallel()
		var m movie
		if err := MapToStruct(map[string]any{"Released": "1999"}, &m); err == nil {
			t.Fatal("expected error; a numeric string must not be parsed into an integer")
		}
	})
	t.Run("scalar into struct field", func(t *testing.T) {
		t.Parallel()
		var w struct{ Inner movie }
		if err := MapToStruct(map[string]any{"Inner": "scalar"}, &w); err == nil {
			t.Fatal("expected error mapping scalar into struct field")
		}
	})
	t.Run("scalar into slice field", func(t *testing.T) {
		t.Parallel()
		var w struct{ Nums []int }
		if err := MapToStruct(map[string]any{"Nums": "notaslice"}, &w); err == nil {
			t.Fatal("expected error mapping scalar into slice field")
		}
	})
	t.Run("type mismatch in slice element", func(t *testing.T) {
		t.Parallel()
		var w struct{ Nums []int }
		if err := MapToStruct(map[string]any{"Nums": []any{int64(1), "two"}}, &w); err == nil {
			t.Fatal("expected error for bad slice element")
		}
	})
	t.Run("scalar into map field", func(t *testing.T) {
		t.Parallel()
		var w struct{ Meta map[string]any }
		if err := MapToStruct(map[string]any{"Meta": "scalar"}, &w); err == nil {
			t.Fatal("expected error mapping scalar into map field")
		}
	})
	t.Run("integer overflows target width", func(t *testing.T) {
		t.Parallel()
		var w struct{ N int32 }
		if err := MapToStruct(map[string]any{"N": int64(3000000000)}, &w); err == nil {
			t.Fatal("expected overflow error for int64 into int32")
		}
	})
	t.Run("negative integer into unsigned", func(t *testing.T) {
		t.Parallel()
		var w struct{ N uint32 }
		if err := MapToStruct(map[string]any{"N": int64(-1)}, &w); err == nil {
			t.Fatal("expected error for negative into unsigned")
		}
	})
	t.Run("float into integer field", func(t *testing.T) {
		t.Parallel()
		var w struct{ N int }
		if err := MapToStruct(map[string]any{"N": float64(3.9)}, &w); err == nil {
			t.Fatal("expected error mapping float into integer field")
		}
	})
	t.Run("integer loses precision as float32", func(t *testing.T) {
		t.Parallel()
		var w struct{ N float32 }
		if err := MapToStruct(map[string]any{"N": int64(1<<25 + 1)}, &w); err == nil {
			t.Fatal("expected precision error for int64 into float32")
		}
	})
	t.Run("integer loses precision as float64", func(t *testing.T) {
		t.Parallel()
		var w struct{ N float64 }
		if err := MapToStruct(map[string]any{"N": int64(1<<53 + 1)}, &w); err == nil {
			t.Fatal("expected precision error for int64 into float64")
		}
	})
	t.Run("float loses precision as float32", func(t *testing.T) {
		t.Parallel()
		var w struct{ N float32 }
		if err := MapToStruct(map[string]any{"N": float64(0.1)}, &w); err == nil {
			t.Fatal("expected precision error for float64 into float32")
		}
	})
	t.Run("map with wrong key type", func(t *testing.T) {
		t.Parallel()
		var w struct{ Scores map[int]int }
		err := MapToStruct(map[string]any{"Scores": map[string]any{"key": int64(1)}}, &w)
		if err == nil {
			t.Fatal("expected error for wrong map key type")
		}
		if got := err.Error(); !strings.Contains(got, `"Scores"`) || !strings.Contains(got, "map[string]interface") || !strings.Contains(got, "map[int]int") {
			t.Fatalf("error should name the field and both map types, got: %q", got)
		}
	})
	t.Run("map with wrong value type", func(t *testing.T) {
		t.Parallel()
		var w struct{ Scores map[string]string }
		err := MapToStruct(map[string]any{"Scores": map[string]any{"key": int64(1)}}, &w)
		if err == nil {
			t.Fatal("expected error for wrong map value type")
		}
		if got := err.Error(); !strings.Contains(got, `"Scores"`) || !strings.Contains(got, "int") || !strings.Contains(got, "string") {
			t.Fatalf("error should name the field and the value types, got: %q", got)
		}
	})
	t.Run("does not coerce integer into uintptr", func(t *testing.T) {
		t.Parallel()
		var w struct{ Foo uintptr }
		err := MapToStruct(map[string]any{"Foo": int64(123456)}, &w)
		if err == nil {
			t.Fatal("expected error for uintptr field")
		}
		if got := err.Error(); !strings.Contains(got, `"Foo"`) || !strings.Contains(got, "int") || !strings.Contains(got, "uintptr") {
			t.Fatalf("error should name the field and both types, got: %q", got)
		}
	})
	t.Run("nested error reports the property path", func(t *testing.T) {
		t.Parallel()
		var w struct{ Inner movie }
		err := MapToStruct(map[string]any{"Inner": map[string]any{"Released": "nope"}}, &w)
		if err == nil {
			t.Fatal("expected error for bad nested property")
		}
		if got := err.Error(); !strings.Contains(got, "Inner") || !strings.Contains(got, "Released") {
			t.Fatalf("error should name the property path, got: %q", got)
		}
	})
}

func TestMapToStructPointerTarget(t *testing.T) {
	t.Parallel()
	type movie struct{ Title string }
	var out *movie
	if err := MapToStruct(map[string]any{"Title": "The Matrix"}, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil || out.Title != "The Matrix" {
		t.Fatalf("pointer target not allocated/populated: %#v", out)
	}
}

// unexportedBase is embedded by pointer to reproduce the case where a nil
// unexported pointer embed cannot be allocated.
type unexportedBase struct {
	Secret string `neo4j:"secret"`
}

func TestMapToStructIgnoresUnexportedField(t *testing.T) {
	t.Parallel()
	type withUnexported struct {
		Name   string
		secret string
	}
	var got withUnexported
	if err := MapToStruct(map[string]any{"Name": "n", "secret": "s"}, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "n" || got.secret != "" {
		t.Fatalf("got %#v, want Name=n and empty secret", got)
	}
}

func TestMapToStructSkipsUnallocatableEmbed(t *testing.T) {
	t.Parallel()
	type withEmbed struct {
		*unexportedBase
		Name string `neo4j:"name"`
	}
	var got withEmbed
	// Must not panic on the nil unexported *unexportedBase embed.
	if err := MapToStruct(map[string]any{"name": "Alice", "secret": "x"}, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "Alice" {
		t.Fatalf("Name = %q, want %q", got.Name, "Alice")
	}
	if got.unexportedBase != nil {
		t.Fatalf("unexported pointer embed should stay nil, got %#v", got.unexportedBase)
	}
}

func TestMapToStructDoesNotAliasSource(t *testing.T) {
	t.Parallel()
	t.Run("slice", func(t *testing.T) {
		t.Parallel()
		src := []any{"a", "b"}
		var got struct{ Tags []any }
		if err := MapToStruct(map[string]any{"Tags": src}, &got); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got.Tags[0] = "mutated"
		if src[0] != "a" {
			t.Fatalf("mapping aliased the source slice: src[0] = %v", src[0])
		}
	})
	t.Run("map", func(t *testing.T) {
		t.Parallel()
		src := map[string]any{"k": "v"}
		var got struct{ Meta map[string]any }
		if err := MapToStruct(map[string]any{"Meta": src}, &got); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got.Meta["k"] = "mutated"
		if src["k"] != "v" {
			t.Fatalf("mapping aliased the source map: src[k] = %v", src["k"])
		}
	})
}

func TestMapToStructDropsAmbiguousField(t *testing.T) {
	t.Parallel()
	type a struct {
		X string `neo4j:"x"`
	}
	type b struct {
		X string `neo4j:"x"`
	}
	type ambiguous struct {
		a
		b
		Y string `neo4j:"y"`
	}
	var got ambiguous
	if err := MapToStruct(map[string]any{"x": "dropped", "y": "kept"}, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// x is promoted from both a and b at the same depth, so it is ambiguous and skipped.
	if got.a.X != "" || got.b.X != "" {
		t.Fatalf("ambiguous field should be skipped, got a.X=%q b.X=%q", got.a.X, got.b.X)
	}
	if got.Y != "kept" {
		t.Fatalf("Y = %q, want %q", got.Y, "kept")
	}
}

func TestMapToStructAllNumericTypes(t *testing.T) {
	t.Parallel()
	type nums struct {
		I   int
		I8  int8
		I16 int16
		I32 int32
		I64 int64
		U   uint
		U8  uint8
		U16 uint16
		U32 uint32
		U64 uint64
		F32 float32
		F64 float64
	}
	src := map[string]any{
		"I": int64(1), "I8": int64(2), "I16": int64(3), "I32": int64(4), "I64": int64(5),
		"U": int64(6), "U8": int64(7), "U16": int64(8), "U32": int64(9), "U64": int64(10),
		"F32": float64(1.5), "F64": float64(2.5),
	}
	want := nums{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1.5, 2.5}
	var got nums
	if err := MapToStruct(src, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestDecodeFieldsOfCachesPerType(t *testing.T) {
	t.Parallel()
	type cached struct {
		Name string
	}
	first := decodeFieldsOf(reflect.TypeOf(cached{}))
	second := decodeFieldsOf(reflect.TypeOf(cached{}))
	if reflect.ValueOf(first).Pointer() != reflect.ValueOf(second).Pointer() {
		t.Fatal("decodeFieldsOf should return the cached map for the same type")
	}
}

func TestFieldsOfCachesPerType(t *testing.T) {
	t.Parallel()
	type cached struct {
		Name string
	}
	first := fieldsOf(reflect.TypeOf(cached{}))
	second := fieldsOf(reflect.TypeOf(cached{}))
	if &first[0] != &second[0] {
		t.Fatal("fieldsOf should return the cached slice for the same type")
	}
}

func TestParseTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in       string
		wantName string
		wantOmit bool
	}{
		{"", "", false},
		{"name", "name", false},
		{"name,omitempty", "name", true},
		{",omitempty", "", true},
		{"name,unknown", "name", false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			t.Parallel()
			name, omit := parseTag(c.in)
			if name != c.wantName || omit != c.wantOmit {
				t.Fatalf("parseTag(%q) = (%q, %v), want (%q, %v)",
					c.in, name, omit, c.wantName, c.wantOmit)
			}
		})
	}
}
