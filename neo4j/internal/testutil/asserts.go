/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// Package testutil contains shared test functionality
package testutil

import (
	"reflect"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
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

func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Expected no error but was %T: %s", err, err)
	}
}

func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Expected an error but it wasn't")
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

func AssertNil(t *testing.T, x interface{}) {
	t.Helper()
	if x != nil && !reflect.ValueOf(x).IsNil() {
		t.Errorf("Expected nil but was %T: %s", x, x)
	}
}

func AssertNotNil(t *testing.T, x interface{}) {
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

func AssertLen(t *testing.T, x interface{}, el int) {
	t.Helper()
	al := reflect.ValueOf(x).Len()
	if al != el {
		t.Errorf("Expected length %d but was %d", el, al)
	}
}

func AssertSliceEqual(t *testing.T, x, y interface{}) {
	t.Helper()
	lenx := reflect.ValueOf(x).Len()
	leny := reflect.ValueOf(y).Len()
	if lenx != leny {
		t.Errorf("Lengths of slices differ %d vs %d", lenx, leny)
		return
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

func AssertIntEqual(t *testing.T, ai, ei int) {
	t.Helper()
	if ai != ei {
		t.Errorf("%d != %d", ai, ei)
	}
	if !reflect.DeepEqual(ai, ei) {
		t.Errorf("Differs %+v vs %+v", ai, ei)
	}
}

func AssertSameType(t *testing.T, x, y interface{}) {
	t.Helper()
	t1 := reflect.TypeOf(x)
	t2 := reflect.TypeOf(y)
	if t1 != t2 {
		t.Errorf("Expected types of %s and %s to be same but was %s and %s", x, y, t1, t2)
	}
}
