/*
 * Copyright (c) "Neo4j"
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

// Implementation of testkit backend
package main

import (
	"bufio"
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":9876")
	if err != nil {
		panic(err)
	}

	for {
		// Wait for a testkit frontend connection, no need to bother handling more than one
		// frontend connection at the time.
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		// Hand over connection to backend
		backend := newBackend(bufio.NewReader(conn), conn)
		backend.serve()
	}
}
