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

// Package mapping converts between user structs and the map[string]any shape the
// bolt layer uses. StructAsMap flattens a struct into Cypher parameters;
// MapToStruct is the reverse, filling a struct from a result map.
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
		fv, ok := fieldByIndex(rv, f.index, false)
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

// fieldByIndex walks index from v through pointer embeds. With alloc it allocates
// nil embeds (false if one is unexported/unsettable); without, it stops false at the first nil.
func fieldByIndex(v reflect.Value, index []int, alloc bool) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				if !alloc || !v.CanSet() {
					return reflect.Value{}, false
				}
				v.Set(reflect.New(v.Type().Elem()))
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

// entity is satisfied by dbtype.Node and dbtype.Relationship; declared locally
// to avoid a dbtype import.
type entity interface{ GetProperties() map[string]any }

var decodeCache sync.Map // map[reflect.Type]map[string]fieldInfo

// MapToStruct fills dest, a non-nil pointer to a struct, from src, using the same
// neo4j tags as StructAsMap. Unmatched keys and null values leave zero values;
// a node, relationship, or map value fills a nested struct.
func MapToStruct(src map[string]any, dest any) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dest)
	}
	v = v.Elem()
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("destination must point to a struct, got %s", v.Kind())
	}
	return structFromMap(v, src)
}

func structFromMap(v reflect.Value, src map[string]any) error {
	for name, f := range decodeFieldsOf(v.Type()) {
		raw, ok := src[name]
		if !ok || raw == nil {
			continue
		}
		fv, ok := fieldByIndex(v, f.index, true)
		if !ok {
			// Field is reachable only through an unexported nil pointer embed.
			continue
		}
		if err := assign(fv, raw); err != nil {
			return fmt.Errorf("property %q: %w", name, err)
		}
	}
	return nil
}

func assign(dst reflect.Value, raw any) error {
	if raw == nil {
		return nil
	}
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assign(dst.Elem(), raw)
	}
	sv := reflect.ValueOf(raw)
	dt := dst.Type()
	switch dst.Kind() {
	case reflect.Struct:
		if sv.Type().AssignableTo(dt) {
			dst.Set(sv) // driver-native struct, e.g. time.Time or a dbtype value
			return nil
		}
		props, ok := propsOf(raw)
		if !ok {
			return typeError(dt, raw)
		}
		return structFromMap(dst, props)
	case reflect.Slice:
		return assignSlice(dst, raw)
	case reflect.Map:
		return assignMap(dst, raw)
	default:
		switch {
		case sv.Type().AssignableTo(dt):
			dst.Set(sv)
		case isNumeric(dst.Kind()) && isNumeric(sv.Kind()):
			return assignNumeric(dst, sv)
		case sv.Kind() == dst.Kind() && sv.Type().ConvertibleTo(dt):
			dst.Set(sv.Convert(dt)) // named type over the same primitive kind
		default:
			return typeError(dt, raw)
		}
		return nil
	}
}

// assignSlice builds a fresh slice so the mapped value never aliases the record.
func assignSlice(dst reflect.Value, raw any) error {
	sv := reflect.ValueOf(raw)
	if sv.Kind() != reflect.Slice {
		return typeError(dst.Type(), raw)
	}
	out := reflect.MakeSlice(dst.Type(), sv.Len(), sv.Len())
	for i := 0; i < sv.Len(); i++ {
		if err := assign(out.Index(i), sv.Index(i).Interface()); err != nil {
			return err
		}
	}
	dst.Set(out)
	return nil
}

// assignMap builds a fresh map so the mapped value never aliases the record.
func assignMap(dst reflect.Value, raw any) error {
	sv := reflect.ValueOf(raw)
	if sv.Kind() != reflect.Map {
		return typeError(dst.Type(), raw)
	}
	dt := dst.Type()
	out := reflect.MakeMapWithSize(dt, sv.Len())
	for iter := sv.MapRange(); iter.Next(); {
		key := iter.Key()
		if !key.Type().AssignableTo(dt.Key()) {
			return typeError(dt, raw)
		}
		val := reflect.New(dt.Elem()).Elem()
		if err := assign(val, iter.Value().Interface()); err != nil {
			return err
		}
		out.SetMapIndex(key, val)
	}
	dst.Set(out)
	return nil
}

// assignNumeric converts between numeric kinds, erroring on overflow rather than
// silently wrapping; a float is never coerced into an integer field.
func assignNumeric(dst, src reflect.Value) error {
	switch {
	case src.CanInt():
		n := src.Int()
		switch {
		case dst.CanInt():
			if dst.OverflowInt(n) {
				return overflowError(dst.Type(), n)
			}
			dst.SetInt(n)
		case dst.CanUint():
			if n < 0 || dst.OverflowUint(uint64(n)) {
				return overflowError(dst.Type(), n)
			}
			dst.SetUint(uint64(n))
		case dst.CanFloat():
			dst.SetFloat(float64(n))
		}
	case src.CanFloat():
		if !dst.CanFloat() {
			return typeError(dst.Type(), src.Interface())
		}
		f := src.Float()
		if dst.OverflowFloat(f) {
			return overflowError(dst.Type(), f)
		}
		dst.SetFloat(f)
	default:
		return typeError(dst.Type(), src.Interface())
	}
	return nil
}

func propsOf(raw any) (map[string]any, bool) {
	switch v := raw.(type) {
	case map[string]any:
		return v, true
	case entity:
		return v.GetProperties(), true
	default:
		return nil, false
	}
}

func typeError(dst reflect.Type, raw any) error {
	return fmt.Errorf("cannot assign %T to %s", raw, dst)
}

func overflowError(dst reflect.Type, v any) error {
	return fmt.Errorf("value %v is out of range for %s", v, dst)
}

func isNumeric(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// decodeFieldsOf indexes fields by property name, resolving field-promotion
// shadowing: the shallowest name wins, an equal-depth tie is dropped.
func decodeFieldsOf(t reflect.Type) map[string]fieldInfo {
	if cached, ok := decodeCache.Load(t); ok {
		return cached.(map[string]fieldInfo)
	}
	groups := make(map[string][]fieldInfo)
	for _, f := range fieldsOf(t) {
		groups[f.name] = append(groups[f.name], f)
	}
	byName := make(map[string]fieldInfo, len(groups))
	for name, fs := range groups {
		winner, ambiguous := fs[0], false
		for _, f := range fs[1:] {
			switch {
			case len(f.index) < len(winner.index):
				winner, ambiguous = f, false // shallower: outright winner
			case len(f.index) == len(winner.index):
				ambiguous = true // same depth: ambiguous
			}
		}
		if !ambiguous {
			byName[name] = winner
		}
	}
	decodeCache.Store(t, byName)
	return byName
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
