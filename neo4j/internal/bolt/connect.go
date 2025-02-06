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
	"io"
	"net"
	"strings"

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

func (p *protocolVersion) formatProtocol() string {
	return fmt.Sprintf("0x%04X%02X%02X", p.back, p.minor, p.major)
}

// versions lists the supported protocol versions in priority order.
// The first proposal is a marker indicating that the client wishes to use the
// new manifest-style negotiation.
var versions = [4]protocolVersion{
	{major: 0xFF, minor: 0x01, back: 0x00}, // Bolt manifest marker
	{major: 5, minor: 7, back: 7},
	{major: 4, minor: 4, back: 2},
	{major: 3, minor: 0, back: 0},
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
	readBufferSize int,
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
	// Write handshake proposals to server
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

	major := buf[3]
	minor := buf[2]

	// Log legacy handshake response.
	if !(major == 0xFF && minor == 0x01) && boltLogger != nil {
		boltLogger.LogServerMessage("", "<HANDSHAKE> %#010X", buf)
	}

	bufferedConn := bufferedConnection(conn, readBufferSize)

	// If the server selected manifest negotiation, perform the extended handshake.
	if major == 0xFF && minor == 0x01 {
		major, minor, err = performManifestNegotiation(ctx, bufferedConn, serverName, errorListener, boltLogger, buf)
		if err != nil {
			return nil, err
		}
	}

	var boltConn db.Connection
	switch major {
	case 3:
		boltConn = NewBolt3(serverName, bufferedConn, errorListener, logger, boltLogger)
	case 4:
		boltConn = NewBolt4(serverName, bufferedConn, errorListener, logger, boltLogger)
	case 5:
		boltConn = NewBolt5(serverName, bufferedConn, errorListener, logger, boltLogger)
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

// performManifestNegotiation handles the manifest-style handshake.
// Returns the negotiated protocol's major and minor version.
func performManifestNegotiation(
	ctx context.Context,
	conn io.ReadWriteCloser,
	serverName string,
	errorListener ConnectionErrorListener,
	boltLogger log.BoltLogger,
	response []byte,
) (byte, byte, error) {
	reader := racing.NewRacingReader(conn)

	// Read the protocol offerings.
	count, supported, err := readProtocolOfferings(ctx, reader, serverName, errorListener)
	if err != nil {
		return 0, 0, err
	}

	// Read the capability mask.
	_, capBytes, err := readCapabilityMask(ctx, reader, serverName, errorListener)
	if err != nil {
		return 0, 0, err
	}

	// Log the complete server handshake message.
	logManifestHandshake(boltLogger, response, count, supported, capBytes)

	// Select an acceptable protocol version.
	chosen, err := selectProtocol(supported, errorListener, serverName)
	if err != nil {
		invalidHandshake := []byte{0x00, 0x00, 0x00, 0x00, 0x00} // 4 bytes for version + 1 byte for capabilities.
		if _, err := conn.Write(invalidHandshake); err != nil {
			errorListener.OnDialError(ctx, serverName, err)
		}
		return 0, 0, err
	}

	// Send the handshake confirmation.
	if err = sendHandshakeConfirmation(ctx, conn, boltLogger, errorListener, serverName, chosen, capBytes); err != nil {
		return 0, 0, err
	}

	return chosen.major, chosen.minor, nil
}

// readProtocolOfferings reads the number of protocol offerings and returns the count and
// a slice of supported protocol versions.
func readProtocolOfferings(ctx context.Context, r racing.RacingReader, serverName string, errorListener ConnectionErrorListener) (uint64, []protocolVersion, error) {
	count, err := readVarInt(ctx, r)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return 0, nil, fmt.Errorf("failed to read manifest protocol count: %w", err)
	}
	supported := make([]protocolVersion, count)
	for i := uint64(0); i < count; i++ {
		var versionBytes [4]byte
		_, err := r.ReadFull(ctx, versionBytes[:])
		if err != nil {
			errorListener.OnDialError(ctx, serverName, err)
			return 0, nil, fmt.Errorf("failed to read manifest protocol version: %w", err)
		}
		supported[i] = protocolVersion{
			back:  versionBytes[1],
			minor: versionBytes[2],
			major: versionBytes[3],
		}
	}
	return count, supported, nil
}

// readCapabilityMask reads the capability bit mask (a Base128 VarInt) and returns both the
// raw value and its encoded byte slice.
func readCapabilityMask(ctx context.Context, r racing.RacingReader, serverName string, errorListener ConnectionErrorListener) (uint64, []byte, error) {
	capMask, err := readVarInt(ctx, r)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return 0, nil, fmt.Errorf("failed to read capability mask: %w", err)
	}
	capBytes, err := encodeVarInt(capMask)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to encode capability mask: %w", err)
	}
	return capMask, capBytes, nil
}

// logManifestHandshake logs the complete server handshake message for manifest negotiation.
// It prints the initial response, count of offerings, each supported protocol, and the capability mask.
func logManifestHandshake(boltLogger log.BoltLogger, response []byte, count uint64, supported []protocolVersion, capBytes []byte) {
	if boltLogger == nil {
		return
	}
	var supportedProtocols []string
	for _, p := range supported {
		supportedProtocols = append(supportedProtocols, p.formatProtocol())
	}
	boltLogger.LogServerMessage("", "<HANDSHAKE> %s [%d] %s %s",
		fmt.Sprintf("%#X", response),
		count,
		strings.Join(supportedProtocols, " "),
		fmt.Sprintf("%#X", capBytes))
}

// selectProtocol iterates over our protocol proposals (skipping the manifest marker)
// and returns the first candidate that is also offered by the server.
func selectProtocol(supported []protocolVersion, errorListener ConnectionErrorListener, serverName string) (protocolVersion, error) {
	proposals := versions[1:]
	for _, candidate := range proposals {
		for _, offer := range supported {
			if candidate.major == offer.major && candidate.minor == offer.minor {
				return candidate, nil
			}
		}
	}
	return protocolVersion{}, fmt.Errorf("none of the server offered Bolt versions are supported (offered: %#v)", supported)
}

// sendHandshakeConfirmation sends the chosen protocol version and capability mask back to the server.
func sendHandshakeConfirmation(ctx context.Context, conn io.ReadWriteCloser, boltLogger log.BoltLogger, errorListener ConnectionErrorListener, serverName string, chosen protocolVersion, capBytes []byte) error {
	chosenBytes := []byte{0x00, 0x00, chosen.minor, chosen.major}
	if boltLogger != nil {
		boltLogger.LogClientMessage("", "<HANDSHAKE> %#X %#X", chosenBytes, capBytes)
	}
	if _, err := conn.Write(chosenBytes); err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return err
	}
	if _, err := conn.Write(capBytes); err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return err
	}
	return nil
}

// readVarInt reads a Base128-encoded variable-length integer and returns the
// decoded unsigned integer, or an error if the value is too long or the read fails.
func readVarInt(ctx context.Context, r racing.RacingReader) (uint64, error) {
	var result uint64
	var shift uint
	var buf [1]byte
	for {
		_, err := r.Read(ctx, buf[:])
		if err != nil {
			return 0, err
		}
		b := buf[0]
		result |= uint64(b&0x7F) << shift
		// The most significant bit is the continuation flag.
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	return result, nil
}

// encodeVarInt encodes the given unsigned integer into a Base128 variable-length integer.
// Returns the encoded bytes or an error if the encoding fails.
func encodeVarInt(value uint64) ([]byte, error) {
	var buf []byte
	for {
		b := byte(value & 0x7F)
		value >>= 7
		if value != 0 {
			buf = append(buf, b|0x80)
		} else {
			buf = append(buf, b)
			break
		}
	}
	return buf, nil
}
