/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ogm

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"reflect"
	"sort"
	"strings"
	"sync"
)

const mappingTypeKey = "mapping_type"
const propertyNameKey = "name"

var mappingTypeNames = map[string]string{
	"id":         "ID",
	"element_id": "element ID",
	"labels":     "labels",
	"property":   "property",
	"properties": "bag of properties",
	"type":       "type",
}

func MapSingle[T any](ctx context.Context, session neo4j.SessionWithContext, query string, params map[string]any) (T, error) {
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return *new(T), err
	}
	record, err := result.Single(ctx)
	if err != nil {
		return *new(T), err
	}
	return mapSingleRecord[T](record)
}

// TODO: implement proper weak map: see Russ Cox impl here (https://go.dev/play/p/ATYDjKZ22mt)
var metadataCache sync.Map

type reflectionMetadata struct {
	kind          reflect.Kind
	numFields     int
	fieldMappings []map[string]string
	fieldNames    []string
}

func mapSingleRecord[T any](record *neo4j.Record) (_ T, mapErr error) {
	value := new(T)
	reflectedValue := reflect.ValueOf(value).Elem() // this is equivalent to deref(ptr(T)), i.e. accessing a T
	if reflectedValue.Kind() == reflect.Pointer {   // if T is a pointer
		nonZeroValue := reflect.New(reflectedValue.Type().Elem()) // initialize T so that it can be de-referenced
		reflectedValue.Set(nonZeroValue)                          // update the existing T
		reflectedValue = reflectedValue.Elem()                    // now manipulate deref(T) directly
	}

	rawMetadata, _ := metadataCache.LoadOrStore(reflect.TypeOf(value), newMetadata(reflectedValue))
	metadata := rawMetadata.(*reflectionMetadata)

	switch metadata.kind {
	case reflect.Struct:
		for i := 0; i < metadata.numFields; i++ {
			if err := setStructField[T](reflectedValue.Field(i), metadata.fieldNames[i], metadata.fieldMappings[i], record); err != nil {
				return *value, err
			}
		}
	case reflect.Map:
		entity, err := getSingle[neo4j.Entity](record)
		if err != nil {
			return *value, err
		}
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				mapErr = fmt.Errorf("failed setting map of type %T, expected type %T (or pointer thereof) "+
					"but this error occurred: %s", *value, entity.GetProperties(), setterPanic)
			}
		}()
		reflectedValue.Set(reflect.ValueOf(entity.GetProperties()))
	default:
		return *value, fmt.Errorf("only struct, map, struct pointer and map pointer types are supported, given: %T", *value)
	}

	return *value, nil
}

func newMetadata(value reflect.Value) *reflectionMetadata {
	kind := value.Kind()
	metadata := reflectionMetadata{kind: kind}
	if kind != reflect.Struct {
		return &metadata
	}
	numField := value.NumField()
	mappings := make([]map[string]string, numField)
	names := make([]string, numField)
	for i := 0; i < numField; i++ {
		field := value.Type().Field(i)
		names[i] = field.Name
		mapping := parseFieldMapping(field)
		if mapping == nil {
			continue
		}
		mappings[i] = mapping
	}
	metadata.numFields = numField
	metadata.fieldMappings = mappings
	metadata.fieldNames = names
	return &metadata
}

func setStructField[T any](field reflect.Value, fieldName string, mappings map[string]string, record *db.Record) (err error) {
	if mappings == nil {
		return nil
	}
	propertyName := mappings[propertyNameKey]
	mappingType := mappings[mappingTypeKey]
	if propertyName == "" && !namelessMapping(mappingType) {
		return fmt.Errorf("the property name is missing for field %q of type %T",
			fieldName, *new(T))
	}
	if propertyName != "" && namelessMapping(mappingType) {
		return fmt.Errorf("the property name %q on the field %q of %T must be removed when mapping %s",
			propertyName, fieldName, *new(T), mappingTypeNames[mappingType])
	}
	switch mappingType {
	case "element_id":
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				err = handlePanic[T, string](mappingTypeNames[mappingType], "", fieldName, setterPanic)
			}
		}()
		entity, err := getSingle[neo4j.Entity](record)
		if err != nil {
			return err
		}
		field.SetString(entity.GetElementId())
	case "id":
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				err = handlePanic[T, int64](mappingTypeNames[mappingType], "", fieldName, setterPanic)
			}
		}()
		entity, err := getSingle[neo4j.Entity](record)
		if err != nil {
			return err
		}
		//lint:ignore SA1019 will be removed when ID support is dropped from server
		field.SetInt(entity.GetId())
	case "labels":
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				err = handlePanic[T, []string](mappingTypeNames[mappingType], "", fieldName, setterPanic)
			}
		}()
		node, err := getSingle[neo4j.Node](record)
		if err != nil {
			return err
		}
		field.Set(reflect.ValueOf(node.Labels))
	case "properties":
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				err = handlePanic[T, []string](mappingTypeNames[mappingType], "", fieldName, setterPanic)
			}
		}()
		entity, err := getSingle[neo4j.Entity](record)
		if err != nil {
			return err
		}
		field.Set(reflect.ValueOf(entity.GetProperties()))
	case "property":
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				err = handlePanic[T, []string](mappingTypeNames[mappingType], propertyName, fieldName, setterPanic)
			}
		}()
		entity, err := getSingle[neo4j.Entity](record)
		if err != nil {
			return err
		}
		property := entity.GetProperties()[propertyName]
		if property == nil && !isNullable(field) {
			return fmt.Errorf("the value of property %q is nil, "+
				"but the type of the field %q of type %T is not nilable", propertyName, fieldName, *new(T))
		}
		field.Set(reflect.ValueOf(property))
	case "type":
		defer func() {
			if setterPanic := recover(); setterPanic != nil {
				err = handlePanic[T, string](mappingTypeNames[mappingType], "", fieldName, setterPanic)
			}
		}()
		relationship, err := getSingle[neo4j.Relationship](record)
		if err != nil {
			return err
		}
		relType := relationship.Type
		field.Set(reflect.ValueOf(relType))
	default:
		validMappings := keysOf(mappingTypeNames)
		sort.Strings(validMappings)
		return fmt.Errorf("unsupported mapping type %q, expected one of %s",
			mappingType,
			fmt.Sprintf(`"%s"`, strings.Join(validMappings, `", "`)))
	}
	return nil
}

func getSingle[T neo4j.Entity](record *db.Record) (T, error) {
	if err := checkSingleColumn(record); err != nil {
		return *new(T), err
	}
	entity, ok := record.Values[0].(T)
	if !ok {
		zeroValue := *new(T)
		return zeroValue, fmt.Errorf("expected query to return a %T but it returned a %T", zeroValue, record.Values[0])
	}
	return entity, nil
}

func checkSingleColumn(record *db.Record) error {
	columnCount := len(record.Values)
	if columnCount != 1 {
		return fmt.Errorf("each record should define only 1 column but got: %d", columnCount)
	}
	return nil
}

func parseFieldMapping(reflectedFieldType reflect.StructField) map[string]string {
	rawTag := reflectedFieldType.Tag.Get("neo4j")
	if rawTag == "" {
		return nil
	}
	specs := strings.Split(rawTag, ",")
	settings := make(map[string]string, len(specs))
	for _, spec := range specs {
		key, setting, _ := strings.Cut(spec, "=")
		settings[key] = setting
	}
	return settings
}

func namelessMapping(mappingType string) bool {
	return mappingType == "id" ||
		mappingType == "element_id" ||
		mappingType == "labels" ||
		mappingType == "properties" ||
		mappingType == "type"
}

func isNullable(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Chan ||
		k == reflect.Func ||
		k == reflect.Interface ||
		k == reflect.Map ||
		k == reflect.Ptr ||
		k == reflect.Slice
}

func handlePanic[STRUCT any, FIELD any](mappingType string, propertyName string, fieldName string, panic any) error {
	structInstance := new(STRUCT)
	expectedFieldInstance := new(FIELD)
	propName := ""
	if propertyName != "" {
		propName = fmt.Sprintf(" %q", propertyName)
	}
	return fmt.Errorf("failed setting %s%s to field %q of type %T, expected type %T "+
		"but this error occurred: %s",
		mappingType, propName, fieldName, *structInstance, *expectedFieldInstance, panic)
}
