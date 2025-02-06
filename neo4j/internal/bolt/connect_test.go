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

package bolt

import (
	"bytes"
	"context"
	"testing"

	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

var logger = log.ToVoid()

func TestConnect(ot *testing.T) {
	// TODO: Test connect timeout

	auth := &idb.ReAuthToken{
		FromSession: false,
		Manager: iauth.Token{Tokens: map[string]any{
			"scheme":      "basic",
			"principal":   "neo4j",
			"credentials": "pass",
		}},
	}

	ot.Run("Server rejects versions", func(t *testing.T) {
		// Doesn't matter what bolt version, shouldn't reach a bolt handler
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()

		// Simulate server that rejects whatever version the client supports
		go func() {
			srv.waitForHandshake()
			srv.rejectVersions()
			srv.closeConnection()
		}()

		_, err := Connect(
			context.Background(),
			"servername",
			conn,
			auth,
			"007",
			nil,
			nil,
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		AssertError(t, err)
	})

	ot.Run("Server answers with invalid version", func(t *testing.T) {
		// Doesn't matter what bolt version, shouldn't reach a bolt handler
		conn, srv, cleanup := setupBolt4Pipe(t)
		defer cleanup()

		// Simulate server that rejects whatever version the client supports
		go func() {
			srv.waitForHandshake()
			srv.acceptVersion(1, 0)
		}()

		boltconn, err := Connect(
			context.Background(),
			"servername",
			conn,
			auth,
			"007",
			nil,
			nil,
			logger,
			nil,
			idb.NotificationConfig{},
			DefaultReadBufferSize,
		)
		AssertError(t, err)
		if boltconn != nil {
			t.Error("Shouldn't returned conn")
		}
	})
}

// fakeConn is a simple in-memory implementation of io.ReadWriteCloser.
type fakeConn struct {
	r *bytes.Buffer // Data to be read (simulated server response)
	w *bytes.Buffer // Data written by the client
}

func newFakeConn(readData []byte) *fakeConn {
	return &fakeConn{
		r: bytes.NewBuffer(readData),
		w: &bytes.Buffer{},
	}
}

func (f *fakeConn) Read(p []byte) (int, error) {
	return f.r.Read(p)
}

func (f *fakeConn) Write(p []byte) (int, error) {
	return f.w.Write(p)
}

func (f *fakeConn) Close() error {
	return nil
}

// TestPerformManifestNegotiationSuccess simulates a successful manifest handshake.
// It provides a valid manifest handshake response and verifies that the negotiated
// protocol version is correct and that the handshake confirmation is written.
func TestPerformManifestNegotiationSuccess(t *testing.T) {
	ctx := context.Background()
	serverName := "testServer"
	errorListener := &noopErrorListener{}

	manifestData := []byte{
		0x03,                   // count = 3
		0x00, 0x07, 0x07, 0x05, // offering 1 --> protocol version 5.7 (back 7)
		0x00, 0x02, 0x04, 0x04, // offering 2 --> protocol version 4.4 (back 2)
		0x00, 0x00, 0x00, 0x03, // offering 3 --> protocol version 3.0
		0x8F, 0x01, // capability mask
	}
	fake := newFakeConn(manifestData)
	response := []byte{0x00, 0x00, 0x01, 0xFF}

	major, minor, err := performManifestNegotiation(ctx, fake, serverName, errorListener, nil, response)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if major != 5 || minor != 7 {
		t.Fatalf("Expected negotiated version 5.7, got %d.%d", major, minor)
	}

	expectedConfirmation := []byte{0x00, 0x00, 0x07, 0x05, 0x8F, 0x01}
	if !bytes.Equal(fake.w.Bytes(), expectedConfirmation) {
		t.Errorf("Handshake confirmation mismatch.\nExpected: % X\nGot:      % X", expectedConfirmation, fake.w.Bytes())
	}
}

// TestPerformManifestNegotiationNoSupportedVersion simulates a manifest handshake in which
// none of the server-offered protocol versions is acceptable to the client.
// It verifies that an error is returned and that the invalid handshake is sent.
func TestPerformManifestNegotiationNoSupportedVersion(t *testing.T) {
	ctx := context.Background()
	serverName := "testServer"
	errorListener := &noopErrorListener{}

	manifestData := []byte{
		0x01,                   // count = 1
		0x00, 0x00, 0xFF, 0xFF, // offering 1 --> protocol version 255.255
		0x00, // capability mask
	}
	fake := newFakeConn(manifestData)
	response := []byte{0x00, 0x00, 0x01, 0xFF}

	_, _, err := performManifestNegotiation(ctx, fake, serverName, errorListener, nil, response)
	if err == nil {
		t.Fatal("Expected error for unsupported protocol version, got nil")
	}

	// In case of no supported version, the invalid handshake is sent.
	expectedInvalid := []byte{0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(fake.w.Bytes(), expectedInvalid) {
		t.Errorf("Expected invalid handshake % X, got % X", expectedInvalid, fake.w.Bytes())
	}
}
