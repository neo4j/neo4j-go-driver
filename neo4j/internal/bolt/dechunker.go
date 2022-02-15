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

package bolt

import (
	"encoding/binary"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
	"io"
	"net"
	"time"
)

// dechunkMessage takes a buffer to be reused and returns the reusable buffer
// (might have been reallocated to handle growth), the message buffer and
// error.
// If a non-default connection read timeout configuration hint is passed, the dechunker resets the connection read
// deadline as well after successfully reading a chunk (NOOP messages included)
func dechunkMessage(conn net.Conn, msgBuf []byte, readTimeout time.Duration,
	logger log.Logger, logName, logId string) ([]byte, []byte, error) {
	sizeBuf := []byte{0x00, 0x00}
	off := 0

	for {
		_, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			return msgBuf, nil, err
		}
		chunkSize := int(binary.BigEndian.Uint16(sizeBuf))
		if chunkSize == 0 {
			if off > 0 {
				return msgBuf, msgBuf[:off], nil
			}
			// Got a nop chunk
			resetConnectionReadDeadline(conn, readTimeout, logger,
				logName, logId)
			continue
		}

		// Need to expand buffer
		if (off + chunkSize) > cap(msgBuf) {
			newMsgBuf := make([]byte, (off+chunkSize)+4096)
			copy(newMsgBuf, msgBuf)
			msgBuf = newMsgBuf
		}
		// Read the chunk into buffer
		_, err = io.ReadFull(conn, msgBuf[off:(off+chunkSize)])
		if err != nil {
			return msgBuf, nil, err
		}
		off += chunkSize
		resetConnectionReadDeadline(conn, readTimeout, logger, logName, logId)
	}
}

func resetConnectionReadDeadline(conn net.Conn, readTimeout time.Duration, logger log.Logger, logName, logId string) {
	if readTimeout < 0 {
		return
	}
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		logger.Error(logName, logId, err)
	}
}
