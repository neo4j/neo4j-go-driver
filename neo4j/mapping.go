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

package neo4j

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/mapping"
)

// As maps record into a value of type T, a struct or pointer to one, using the
// neo4j struct tags: `neo4j:"name"` renames a field, `neo4j:"-"` skips it.
// Missing or null properties leave zero values and extra properties are ignored.
// A record with a single node, relationship, or map column maps that value's
// properties; otherwise its columns map by name.
//
// Numeric properties are converted to the field's type; a conversion that would
// lose range or precision returns an error instead of silently truncating.
//
// Slice and map fields are copied; a node or relationship's properties are not,
// so a mapped value may share that data with the record.
//
// Fields promoted from embedded structs follow encoding/json's rules: a
// shallower field shadows a deeper one, and a same-depth tie is skipped.
//
// As is part of the Object Mapping preview feature (see README on what it means
// in terms of support and compatibility guarantees).
func As[T any](record *Record) (T, error) {
	var out T
	if record == nil {
		return out, &UsageError{Message: "cannot map a nil record"}
	}
	if err := mapping.MapToStruct(sourceOf(record), &out); err != nil {
		return *new(T), err
	}
	return out, nil
}

// CollectAs maps every remaining record in result into a slice of T. It is
// shorthand for CollectT(ctx, result, As[T]); see As for the mapping rules.
//
// CollectAs is part of the Object Mapping preview feature (see README on what it
// means in terms of support and compatibility guarantees).
func CollectAs[T any](ctx context.Context, result Result) ([]T, error) {
	return CollectT(ctx, result, As[T])
}

// SingleAs maps the single remaining record in result into a value of type T. It
// is shorthand for SingleT(ctx, result, As[T]); see As for the mapping rules.
//
// SingleAs is part of the Object Mapping preview feature (see README on what it
// means in terms of support and compatibility guarantees).
func SingleAs[T any](ctx context.Context, result Result) (T, error) {
	return SingleT(ctx, result, As[T])
}

// CollectRecordsAs maps a slice of records, such as EagerResult.Records, into a
// slice of T; see As for the mapping rules.
//
// CollectRecordsAs is part of the Object Mapping preview feature (see README on
// what it means in terms of support and compatibility guarantees).
func CollectRecordsAs[T any](records []*Record) ([]T, error) {
	out := make([]T, 0, len(records))
	for _, record := range records {
		v, err := As[T](record)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// sourceOf picks the map As decodes from: a lone node, relationship, or map
// column is unwrapped to its properties; otherwise the record's columns are used.
func sourceOf(record *Record) map[string]any {
	if len(record.Keys) == 1 {
		switch v := record.Values[0].(type) {
		case map[string]any:
			return v
		case Entity: // Node or Relationship
			return v.GetProperties()
		}
	}
	return record.AsMap()
}
