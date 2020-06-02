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

package pool

import (
	"testing"
	"time"
)

type fakeConn struct {
	serverName string
	isAlive    bool
	birthdate  time.Time
	id         int
}

func (c *fakeConn) IsAlive() bool {
	return c.isAlive
}

func (c *fakeConn) ServerName() string {
	return c.serverName
}

func (c *fakeConn) Close() {
}

func (c *fakeConn) Reset() {
}

func (c *fakeConn) Birthdate() time.Time {
	return c.birthdate
}

func assertConnection(t *testing.T, c Connection, err error) {
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("No connection")
	}
}

func assertNoConnection(t *testing.T, c Connection, err error) {
	t.Helper()
	if c != nil {
		t.Fatal("Should not have connection")
	}
	if err == nil {
		t.Fatal("Should have error")
	}
}

func assertNumberOfServers(t *testing.T, p *Pool, expectedNum int) {
	t.Helper()
	actualNum := len(p.getServers())
	if actualNum != expectedNum {
		t.Fatalf("Expected number of servers to be %d but was %d", expectedNum, actualNum)
	}
}

func assertNumberOfIdle(t *testing.T, p *Pool, serverName string, expectedNum int) {
	t.Helper()
	servers := p.getServers()
	server := servers[serverName]
	if server == nil {
		t.Fatalf("Server %s not found", serverName)
	}
	actualNum := server.numIdle()
	if actualNum != expectedNum {
		t.Fatalf("Expected number of idle conns on %s to be %d but was %d", serverName, expectedNum, actualNum)
	}
}
