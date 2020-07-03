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

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/connection"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/log"
)

type Logger interface {
	ConnectError(c net.Conn, err error)
	ConnectionError(c connection.Connection, err error)
	ConnectionInfo(c connection.Connection, msg string, args ...interface{})
}

// Negotiate version of bolt protocol.
// Returns instance of bolt protocol implmenting low-level abstract connection Connection interface.
func Connect(serverName string, conn net.Conn, auth map[string]interface{}, log log.Logger) (connection.Connection, error) {
	// Perform Bolt handshake to negotiate version
	// Send handshake to server
	handshake := []byte{
		0x60, 0x60, 0xb0, 0x17, // Magic: GoGoBolt
		0x00, 0x00, 0x00, 0x04, // Versions in priority order, big endian.
		0x00, 0x00, 0x00, 0x03,
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

	logId := "boltconnect@" + serverName

	// Parse received version and construct the correct instance
	ver := binary.BigEndian.Uint32(buf)
	switch ver {
	case 3:
		// Handover rest of connection handshaking
		boltConn := NewBolt3(serverName, conn, log)
		err = boltConn.connect(auth)
		if err != nil {
			return nil, err
		}
		log.Infof(logId, "Connected to %s", boltConn.ServerVersion())
		return boltConn, nil
	case 4:
		// Handover rest of connection handshaking
		boltConn := NewBolt4(serverName, conn, log)
		err = boltConn.connect(auth)
		if err != nil {
			return nil, err
		}
		return boltConn, nil
	case 0:
		err = errors.New("Server did not accept any of the requested Bolt protocol versions. Only Neo4j version 3.5 and version 4 currently supports Bolt protocol version 3")
	default:
		err = errors.New(fmt.Sprintf("Server responded with unsupported version %d", ver))
	}

	log.Error(logId, err)
	return nil, err
}
