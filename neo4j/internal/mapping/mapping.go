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

// Package mapping converts a user struct into the map[string]any shape the bolt
// layer expects for Cypher parameters. Nested struct values are returned as-is;
// the bolt layer re-enters this package for each one.
package mapping

import (
	"reflect"
	"strings"
	"sync"
)

const tagName = "neo4j"

type fieldInfo struct {
	name      string
	index     []int
	omitEmpty bool
}

var typeCache sync.Map // map[reflect.Type][]fieldInfo

// StructAsMap walks the exported fields of v (struct or pointer to struct) and
// returns a map keyed by each field's Cypher property name. The bool reports
// whether v was a struct or pointer to struct; it is false for any other type.
// A nil pointer returns (nil, true).
func StructAsMap(v any) (map[string]any, bool) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, true
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, false
	}

	fields := fieldsOf(rv.Type())
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		fv, ok := fieldByIndex(rv, f.index)
		if !ok {
			// Field is reachable only through a nil pointer embed.
			continue
		}
		if f.omitEmpty && fv.IsZero() {
			continue
		}
		out[f.name] = fv.Interface()
	}
	return out, true
}

// fieldByIndex walks index from v, dereferencing pointer embeds. It reports
// false if a nil pointer is hit mid-path.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

func fieldsOf(t reflect.Type) []fieldInfo {
	if cached, ok := typeCache.Load(t); ok {
		return cached.([]fieldInfo)
	}
	fields := buildFields(t, nil)
	typeCache.Store(t, fields)
	return fields
}

func buildFields(t reflect.Type, indexPath []int) []fieldInfo {
	var out []fieldInfo
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get(tagName)
		idx := append(append([]int(nil), indexPath...), i)

		// Flatten anonymous struct embeds, value or pointer; their inner fields
		// stay reachable through the index path even when the wrapper is unexported.
		if f.Anonymous && tag == "" {
			ft := f.Type
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				out = append(out, buildFields(ft, idx)...)
				continue
			}
		}

		if !f.IsExported() {
			continue
		}
		if tag == "-" {
			continue
		}
		name, omitEmpty := parseTag(tag)
		if name == "" {
			name = f.Name
		}
		out = append(out, fieldInfo{name: name, index: idx, omitEmpty: omitEmpty})
	}
	return out
}

func parseTag(tag string) (name string, omitEmpty bool) {
	if tag == "" {
		return "", false
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, opt := range parts[1:] {
		if opt == "omitempty" {
			omitEmpty = true
		}
	}
	return
}
