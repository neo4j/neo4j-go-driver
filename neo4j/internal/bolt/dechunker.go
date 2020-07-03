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
	"io"
)

func dechunkMessage(rd io.Reader, msgBuf []byte) ([]byte, error) {
	sizeBuf := []byte{0x00, 0x00}
	msgBuf = msgBuf[:0]
	offset := 0

	for {
		// Read size of chunk
		_, err := io.ReadFull(rd, sizeBuf)
		if err != nil {
			return msgBuf, err
		}
		chunkSize := int(binary.BigEndian.Uint16(sizeBuf))

		// A chunk size of 0 means either end of current message or it is just a no op sent
		// from server to keep connection alive.
		if chunkSize == 0 && len(msgBuf) > 0 {
			return msgBuf, nil
		}

		msgBuf = append(msgBuf, make([]byte, chunkSize)...)
		chunkBuf := msgBuf[offset:]
		offset += chunkSize
		_, err = io.ReadFull(rd, chunkBuf)
		if err != nil {
			return msgBuf, err
		}
	}
}
