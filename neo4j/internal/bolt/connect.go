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
	"encoding/binary"
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
	return fmt.Sprintf("%#04X%02X%02X", p.back, p.minor, p.major)
}

// versions lists the supported protocol versions in priority order.
// The first proposal is a marker indicating that the client wishes to use the
// new manifest-style negotiation.
var versions = [4]protocolVersion{
	{major: 0xFF, minor: 0x01, back: 0x00}, // Bolt manifest marker
	{major: 5, minor: 8, back: 8},
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

	if major == 80 && minor == 84 {
		return nil, &errorutil.UsageError{Message: "server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
			"(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)"}
	}

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
	supported, err := readProtocolOfferings(ctx, reader, serverName, errorListener)
	if err != nil {
		return 0, 0, err
	}

	// Read the capability mask.
	_, capBytes, err := readCapabilityMask(ctx, reader, serverName, errorListener)
	if err != nil {
		return 0, 0, err
	}

	// Log the complete server handshake message.
	logManifestHandshake(boltLogger, response, supported, capBytes)

	// Select an acceptable protocol version.
	chosen, err := selectProtocol(supported)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		// Best-effort attempt to send an invalid handshake (ignore any error).
		invalidHandshake := []byte{0x00, 0x00, 0x00, 0x00, 0x00} // 4 bytes for version + 1 byte for capabilities.
		_, _ = racing.NewRacingWriter(conn).Write(ctx, invalidHandshake)
		return 0, 0, err
	}

	// Send the handshake confirmation.
	if err = sendHandshakeConfirmation(ctx, conn, boltLogger, errorListener, serverName, chosen, []byte{0x00}); err != nil {
		return 0, 0, err
	}

	return chosen.major, chosen.minor, nil
}

// readProtocolOfferings reads the number of protocol offerings and returns the count and
// a slice of supported protocol versions.
func readProtocolOfferings(ctx context.Context, r racing.RacingReader, serverName string, errorListener ConnectionErrorListener) ([]protocolVersion, error) {
	count, _, err := readVarInt(ctx, r)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return nil, fmt.Errorf("failed to read manifest protocol count: %w", err)
	}
	supported := make([]protocolVersion, 0, count)
	for i := uint64(0); i < count; i++ {
		var versionBytes [4]byte
		_, err := r.ReadFull(ctx, versionBytes[:])
		if err != nil {
			errorListener.OnDialError(ctx, serverName, err)
			return nil, fmt.Errorf("failed to read manifest protocol version: %w", err)
		}
		supported = append(supported, protocolVersion{
			back:  versionBytes[1],
			minor: versionBytes[2],
			major: versionBytes[3],
		})
	}
	return supported, nil
}

// readCapabilityMask reads the capability bit mask (a Base128 VarInt) and returns both the
// raw value and its encoded byte slice.
func readCapabilityMask(ctx context.Context, r racing.RacingReader, serverName string, errorListener ConnectionErrorListener) (uint64, []byte, error) {
	capMask, capBytes, err := readVarInt(ctx, r)
	if err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return 0, capBytes, fmt.Errorf("failed to read capability mask: %w", err)
	}
	return capMask, capBytes, nil
}

// logManifestHandshake logs the complete server handshake message for manifest negotiation.
// It prints the initial response, count of offerings, each supported protocol, and the capability mask.
func logManifestHandshake(boltLogger log.BoltLogger, response []byte, supported []protocolVersion, capBytes []byte) {
	if boltLogger == nil {
		return
	}
	supportedProtocols := make([]string, 0, len(supported))
	for _, p := range supported {
		supportedProtocols = append(supportedProtocols, p.formatProtocol())
	}
	boltLogger.LogServerMessage("", "<HANDSHAKE> %s [%d] %s %s",
		fmt.Sprintf("%#X", response),
		len(supported),
		strings.Join(supportedProtocols, " "),
		fmt.Sprintf("%#X", capBytes))
}

// selectProtocol iterates over our protocol proposals (skipping the manifest marker)
// and returns the first candidate whose major version matches and whose minor version
// falls within the range offered by the server.
func selectProtocol(offers []protocolVersion) (protocolVersion, error) {
	for _, candidate := range versions[1:] {
		for v := int(candidate.minor); v >= int(candidate.minor)-int(candidate.back); v-- {
			for _, offer := range offers {
				if offer.major != candidate.major {
					continue
				}
				if byte(v) <= offer.minor && byte(v) >= offer.minor-offer.back {
					return protocolVersion{major: candidate.major, minor: byte(v)}, nil
				}
			}
		}
	}
	return protocolVersion{}, fmt.Errorf("none of the server offered Bolt versions are supported (offered: %#v)", offers)
}

// sendHandshakeConfirmation sends the chosen protocol version and capability mask back to the server.
func sendHandshakeConfirmation(ctx context.Context, conn io.ReadWriteCloser, boltLogger log.BoltLogger, errorListener ConnectionErrorListener, serverName string, chosen protocolVersion, capBytes []byte) error {
	chosenBytes := []byte{0x00, 0x00, chosen.minor, chosen.major}
	if boltLogger != nil {
		boltLogger.LogClientMessage("", "<HANDSHAKE> %#X %#X", chosenBytes, capBytes)
	}
	writer := racing.NewRacingWriter(conn)
	if _, err := writer.Write(ctx, chosenBytes); err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return err
	}
	if _, err := writer.Write(ctx, capBytes); err != nil {
		errorListener.OnDialError(ctx, serverName, err)
		return err
	}
	return nil
}

// readVarInt returns a Base128-encoded variable-length integer from the reader.
func readVarInt(ctx context.Context, r racing.RacingReader) (uint64, []byte, error) {
	var buf [binary.MaxVarintLen64]byte
	// Read one byte at a time until a byte with the MSB not set is encountered.
	for i := 0; i < len(buf); i++ {
		if _, err := r.Read(ctx, buf[i:i+1]); err != nil {
			return 0, buf[:i], err
		}
		// If the continuation bit is not set, we've reached the end of the varint.
		if buf[i]&0x80 == 0 {
			value, n := binary.Uvarint(buf[:i+1])
			if n <= 0 {
				return 0, buf[:i+1], fmt.Errorf("failed to decode varint")
			}
			return value, buf[:i+1], nil
		}
	}
	return 0, buf[:], fmt.Errorf("varint too long")
}

// encodeVarInt returns the encoded unsigned integer into a Base128 variable-length integer.
func encodeVarInt(value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return buf[:n]
}
