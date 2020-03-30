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

package bolt

import (
	"sync"
	"testing"
)

func TestConnectBolt3(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	// TODO: Test version handshake failure, connection should close
	// TODO: Test authentication failure, connection should close (bolt3 test?)
	// TODO: Test connect timeout

	conn, srv, cleanup := setupBoltPipe(t)
	defer cleanup()

	// Simulate server with a succesful connect
	go func() {
		// Wait for initial handshake
		handshake := srv.waitForHandshake()

		// There should be a version 3 somewhere
		foundV3 := false
		for i := 0; i < 5; i++ {
			ver := handshake[(i * 4) : (i*4)+4]
			if ver[3] == 3 {
				foundV3 = true
			}
		}
		if !foundV3 {
			t.Fatalf("Didn't find version 3 in handshake: %+v", handshake)
		}

		// Accept bolt version 3
		srv.acceptVersion(3)

		// Need unpacker, hydrator and a dechunker now
		srv.waitForHello()

		// Need packer and a chunker now
		srv.acceptHello()

		wg.Done()
	}()

	// Pass the connection to bolt connect
	boltconn, err := Connect(conn)
	if err != nil {
		t.Fatalf("Failed to connect bolt: %s", err)
	}

	boltconn.Close()

	wg.Wait()
}
