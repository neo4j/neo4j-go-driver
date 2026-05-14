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
// the bolt layer re-enters this package for any user-defined nested struct, so
// driver-known types (time.Time, dbtype.*, etc.) keep their existing encoding
// without special-casing here.
package mapping

import (
	"fmt"
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

// StructAsMap walks the exported fields of v (struct or pointer to struct)
// and returns a map keyed by each field's Cypher property name. A nil pointer
// returns (nil, nil); the caller maps that to bolt NULL.
func StructAsMap(v any) (map[string]any, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("mapping: expected struct or pointer to struct, got %T", v)
	}

	fields := fieldsOf(rv.Type())
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		fv := rv.FieldByIndex(f.index)
		if f.omitEmpty && fv.IsZero() {
			continue
		}
		out[f.name] = fv.Interface()
	}
	return out, nil
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

		// Recurse into anonymous embeds - their inner fields stay reachable
		// via FieldByIndex even when the wrapper type is unexported. Pointer
		// kind skipped to avoid nil-deref on the walk.
		if f.Anonymous && tag == "" && f.Type.Kind() == reflect.Struct {
			out = append(out, buildFields(f.Type, idx)...)
			continue
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
