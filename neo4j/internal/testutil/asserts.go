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

// Package testutil contains shared test functionality
package testutil

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

func AssertNextOnlyRecord(t *testing.T, rec *db.Record, sum *db.Summary, err error) {
	t.Helper()
	if rec == nil {
		t.Errorf("Expected record")
	}
	if sum != nil {
		t.Errorf("Didn't expect summary")
	}
	if err != nil {
		t.Errorf("Didn't expect error: %s", err)
	}
}

func AssertNextOnlySummary(t *testing.T, rec *db.Record, sum *db.Summary, err error) {
	t.Helper()
	if rec != nil {
		t.Errorf("Didn't expect record")
	}
	if sum == nil {
		t.Errorf("Expected summary")
	}
	if err != nil {
		t.Errorf("Didn't expect error")
	}
}

func AssertNextOnlyError(t *testing.T, rec *db.Record, sum *db.Summary, err error) {
	t.Helper()
	if rec != nil {
		t.Errorf("Didn't expect record")
	}
	if sum != nil {
		t.Errorf("Didn't expect summary")
	}
	if err == nil {
		t.Errorf("Expected error")
	}
}

func AssertWriteSucceeds(t *testing.T, w io.Writer, b []byte) {
	t.Helper()
	_, err := w.Write(b)
	AssertNoError(t, err)
}

func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Expected no error but was %T: %s", err, err)
	}
}

func AssertErrorMessageContains(t *testing.T, err error, msg string, args ...any) {
	AssertError(t, err)
	AssertStringContain(t, err.Error(), fmt.Sprintf(msg, args...))
}

func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Expected an error but none occurred")
	}
}

func AssertNeo4jError(t *testing.T, err error) {
	t.Helper()
	AssertError(t, err)
	_, isDbErr := err.(*db.Neo4jError)
	if !isDbErr {
		t.Errorf("Expected database error but was %T: %s", err, err)
	}
}

func AssertNil(t *testing.T, x any) {
	t.Helper()
	if x != nil && !reflect.ValueOf(x).IsNil() {
		t.Errorf("Expected nil but was %T: %s", x, x)
	}
}

func AssertNotNil(t *testing.T, x any) {
	t.Helper()
	if x == nil || reflect.ValueOf(x).IsNil() {
		t.Fatal("Expected not nil")
	}
}

func AssertTrue(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Error("Expected true but was false")
	}
}

func AssertFalse(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Error("Expected false but was true")
	}
}

func AssertLen(t *testing.T, x any, el int) {
	t.Helper()
	al := reflect.ValueOf(x).Len()
	if al != el {
		t.Errorf("Expected length %d but was %d", el, al)
	}
}

func AssertSliceEqual[T comparable](t *testing.T, x, y []T) {
	t.Helper()
	if len(x) != len(y) {
		t.Errorf("Expected slice to be equal but length differs: %v %v", x, y)
		return
	}
	for i := range x {
		if x[i] != y[i] {
			t.Errorf("Expected slice to be equal but differs at index %d: %v %v", i, x, y)
		}
	}
}

func AssertEmptyString(t *testing.T, s string) {
	t.Helper()
	if s != "" {
		t.Errorf("Expected empty string but was '%s'", s)
	}
}

func AssertStringNotEmpty(t *testing.T, s string) {
	t.Helper()
	if s == "" {
		t.Errorf("Expected non empty string")
	}
}

func AssertStringEqual(t *testing.T, as, es string) {
	t.Helper()
	if as != es {
		t.Errorf("'%s' != '%s'", as, es)
	}
}

func AssertStringContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Errorf("Expected %s to contain %s", s, sub)
	}
}

func AssertStringNotContain(t *testing.T, s, sub string) {
	t.Helper()
	if strings.Contains(s, sub) {
		t.Errorf("Expected %s to not contain %s", s, sub)
	}
}

func AssertAny[T any](t *testing.T, slice []T, predicate func(T) bool) {
	t.Helper()
	for _, e := range slice {
		if predicate(e) {
			return
		}
	}
	t.Errorf("Expected slice to contain element matching predicate")
}

func AssertAll[T any](t *testing.T, slice []T, predicate func(T) bool) {
	t.Helper()
	for _, e := range slice {
		if !predicate(e) {
			t.Errorf("Expected slice to contain only elements matching predicate")
		}
	}
}

func AssertMapHasKey[K comparable](t *testing.T, m map[K]any, key K) {
	t.Helper()
	if _, ok := m[key]; !ok {
		t.Errorf("Expected map to contain key %v", key)
	}
}

func AssertMapDoesNotHaveKey[K comparable](t *testing.T, m map[K]any, key K) {
	t.Helper()
	if _, ok := m[key]; ok {
		t.Errorf("Expected map to not contain key %v", key)
	}
}

func AssertIntEqual(t *testing.T, ai, ei int) {
	t.Helper()
	if ai != ei {
		t.Errorf("%d != %d", ai, ei)
	}
	if !reflect.DeepEqual(ai, ei) {
		t.Errorf("Differs %+v vs %+v", ai, ei)
	}
}

func AssertBoolEqual(t *testing.T, ai, ei bool) {
	t.Helper()
	if ai != ei {
		t.Errorf("%t != %t", ai, ei)
	}
	if !reflect.DeepEqual(ai, ei) {
		t.Errorf("Differs %+v vs %+v", ai, ei)
	}
}

func AssertSameType(t *testing.T, x, y any) {
	t.Helper()
	t1 := reflect.TypeOf(x)
	t2 := reflect.TypeOf(y)
	if t1 != t2 {
		t.Errorf("Expected types of %s and %s to be same but was %s and %s", x, y, t1, t2)
	}
}

func AssertDeepEquals(t *testing.T, values ...any) {
	t.Helper()
	count := len(values)
	if count == 0 {
		return
	}
	prev := values[0]
	for i := 1; i < count; i++ {
		current := values[i]
		if !reflect.DeepEqual(prev, current) {
			t.Errorf("Expected value %v (parameter %d) to equal value %v (parameter %d)", prev, i-1, current, i)
			return
		}
		prev = current
	}
}

func AssertNotDeepEquals(t *testing.T, value1, value2 any) {
	t.Helper()
	if reflect.DeepEqual(value1, value2) {
		t.Errorf("Expected value %v to not equal value %v", value1, value2)
		return
	}
}

func AssertAfter(t *testing.T, t1, t2 time.Time) {
	t.Helper()
	if !t1.After(t2) {
		t.Errorf("Expected time %v to be after time %v", t1, t2)

	}
}

func AssertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("Expected to function to panic, but did not or emitted a nil value")
		}
	}()
	f()
}

func AssertEqualsInAnyOrder(t *testing.T, actual []string, expected []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("Expected actual slice %v and expected slice %v to have same length", actual, expected)
		return
	}
	actual = actual[0:len(actual):len(actual)]
	sort.Strings(actual)
	sort.Strings(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected actual slice %v and expected slice %v to have same elements", actual, expected)
	}
}

func AssertVersionInHandshake(t *testing.T, handshake []byte, major, minor byte) {
	t.Helper()
	AssertMajorVersionInHandshake(t, handshake, major)
	var minorMatches bool
	for i := 0; i < 5; i++ {
		version := handshake[(i * 4) : (i*4)+4]
		if version[3] == major {
			if version[2] >= minor {
				minorMatches = true
				break
			}
		}
	}
	if !minorMatches {
		t.Errorf("Didn't find minor version >= %d in handshake: %X", minor, handshake)
	}
}

func AssertMajorVersionInHandshake(t *testing.T, handshake []byte, major byte) {
	t.Helper()
	var majorMatches bool
	for i := 0; i < 5; i++ {
		version := handshake[(i * 4) : (i*4)+4]
		if version[3] == major {
			majorMatches = true
		}
	}
	if !majorMatches {
		t.Errorf("Didn't find major version %d in handshake: %X", major, handshake)
	}
}
