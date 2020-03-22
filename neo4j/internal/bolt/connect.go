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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/connection"
)

// Negotiate version of bolt protocol.
// Closes the provided connection if any error is encountered.
func Connect(conn net.Conn) (connection.Connection, error) {
	// TODO: Use deadline read/write

	// Perform Bolt handshake to negotiate version
	// Send handshake to server
	handshake := []byte{
		0x60, 0x60, 0xb0, 0x17, // Magic: GoGoBolt
		0x00, 0x00, 0x00, 0x03, // Versions in priority order, big endian.
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	_, err := conn.Write(handshake)
	if err != nil {
		return nil, err
	}

	// Receive accepted server version
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}

	// Parse received version and construct the correct instance
	ver := binary.BigEndian.Uint32(buf)
	switch ver {
	case 3:
		// Handover rest of connection handshaking
		boltConn := NewBolt3(conn)
		err = boltConn.connect()
		if err != nil {
			// Make sure that the connection is closed!
			conn.Close()
			return nil, err
		}
		return boltConn, nil
	case 0:
		return nil, errors.New("No matching version")
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported version: %d", ver))
	}
}
