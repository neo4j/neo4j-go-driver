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
		name   string
		in     any
		want   map[string]any
		errMsg string
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
			name: "pointer-embedded struct stays as a named field",
			in:   pointerEmbedded{Base: &Base{ID: "1"}, Name: "Alice"},
			want: map[string]any{"Base": &Base{ID: "1"}, "Name": "Alice"},
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
			name:   "non-struct input errors",
			in:     "not a struct",
			errMsg: "expected struct",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, err := StructAsMap(c.in)
			if c.errMsg != "" {
				if err == nil || !strings.Contains(err.Error(), c.errMsg) {
					t.Fatalf("expected error containing %q, got %v", c.errMsg, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("StructAsMap mismatch\nwant: %#v\n got: %#v", c.want, got)
			}
		})
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
