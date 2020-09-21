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

package neo4j

import (
	"context"
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

func assertErrorEq(t *testing.T, err1, err2 error) {
	t.Helper()
	if !reflect.DeepEqual(err1, err2) {
		t.Errorf("Wrong type of error, '%s' != '%s'", err1, err2)
	}
}

func assertTrue(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Errorf("Expected true")
	}
}

// Fake implementation of router
type testRouter struct {
	readers []string
	writers []string
	err     error
}

func (r *testRouter) Readers(database string) ([]string, error) {
	return r.readers, r.err
}

func (r *testRouter) Writers(database string) ([]string, error) {
	return r.writers, r.err
}

func (r *testRouter) Invalidate(database string) {
}

func (r *testRouter) CleanUp() {
}

// Fake implementation of connection pool
type testPool struct {
	borrowConn db.Connection
	err        error
	returnHook func()
}

func (p *testPool) Borrow(ctx context.Context, serverNames []string, wait bool) (db.Connection, error) {
	return p.borrowConn, p.err
}

func (p *testPool) Return(c db.Connection) {
	if p.returnHook != nil {
		p.returnHook()
	}
}

func (p *testPool) CleanUp() {
}
