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

// Package bolt contains implementations of the database functionality.
package bolt

import (
	"context"
	"fmt"
	"net"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type protocolVersion struct {
	major byte
	minor byte
	back  byte // Number of minor versions back
}

// Supported versions in priority order
var versions = [4]protocolVersion{
	{major: 5, minor: 4, back: 4},
	{major: 4, minor: 4, back: 2},
	{major: 4, minor: 1},
	{major: 3, minor: 0},
}

// Connect initiates the negotiation of the Bolt protocol version.
// Returns the instance of bolt protocol implementing the low-level Connection interface.
func Connect(ctx context.Context,
	serverName string,
	conn net.Conn,
	auth *db.ReAuthToken,
	userAgent string,
	routingContext map[string]string,
	errorListener ConnectionErrorListener,
	logger log.Logger,
	boltLogger log.BoltLogger,
	notificationConfig db.NotificationConfig,
) (db.Connection, error) {
	// Perform Bolt handshake to negotiate version
	// Send handshake to server
	handshake := []byte{
		0x60, 0x60, 0xb0, 0x17, // Magic: GoGoBolt
		0x00, versions[0].back, versions[0].minor, versions[0].major,
		0x00, versions[1].back, versions[1].minor, versions[1].major,
		0x00, versions[2].back, versions[2].minor, versions[2].major,
		0x00, versions[3].back, versions[3].minor, versions[3].major,
	}
	if boltLogger != nil {
		boltLogger.LogClientMessage("", "<MAGIC> %#010X", handshake[0:4])
		boltLogger.LogClientMessage("", "<HANDSHAKE> %#010X %#010X %#010X %#010X", handshake[4:8], handshake[8:12], handshake[12:16], handshake[16:20])
	}
	_, err := racing.NewRacingWriter(conn).Write(ctx, handshake)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return nil, err
	}

	// Receive accepted server version
	buf := make([]byte, 4)
	_, err = racing.NewRacingReader(conn).ReadFull(ctx, buf)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return nil, err
	}

	if boltLogger != nil {
		boltLogger.LogServerMessage("", "<HANDSHAKE> %#010X", buf)
	}

	major := buf[3]
	minor := buf[2]
	var boltConn db.Connection
	switch major {
	case 3:
		boltConn = NewBolt3(serverName, conn, errorListener, logger, boltLogger)
	case 4:
		boltConn = NewBolt4(serverName, conn, errorListener, logger, boltLogger)
	case 5:
		boltConn = NewBolt5(serverName, conn, errorListener, logger, boltLogger)
	case 0:
		return nil, fmt.Errorf("server did not accept any of the requested Bolt versions (%#v)", versions)
	default:
		if major == 80 && minor == 84 {
			return nil, &errorutil.UsageError{Message: "server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
				"(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)"}
		}
		return nil, &errorutil.UsageError{Message: fmt.Sprintf("server responded with unsupported version %d.%d", major, minor)}
	}
	if err = boltConn.Connect(ctx, int(minor), auth, userAgent, routingContext, notificationConfig); err != nil {
		boltConn.Close(ctx)
		return nil, err
	}
	return boltConn, nil
}
