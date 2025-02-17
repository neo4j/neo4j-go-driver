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
	"fmt"
	"io"
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

// fakeConn is a simple implementation of io.ReadWriteCloser.
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
	t.Parallel()
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

	// Expect 0 capabilities for now as driver has no capabilities.
	expectedConfirmation := []byte{0x00, 0x00, 0x07, 0x05, 0x00}
	if !bytes.Equal(fake.w.Bytes(), expectedConfirmation) {
		t.Errorf("Handshake confirmation mismatch.\nExpected: % X\nGot:      % X", expectedConfirmation, fake.w.Bytes())
	}
}

// TestPerformManifestNegotiationNoSupportedVersion simulates a manifest handshake in which
// none of the server-offered protocol versions is acceptable to the client.
// It verifies that an error is returned and that the invalid handshake is sent.
func TestPerformManifestNegotiationNoSupportedVersion(t *testing.T) {
	t.Parallel()
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

// fakeRacingReader is a simple implementation of a racing.RacingReader.
type fakeRacingReader struct {
	r *bytes.Reader
}

func newFakeRacingReader(data []byte) *fakeRacingReader {
	return &fakeRacingReader{r: bytes.NewReader(data)}
}

func (f *fakeRacingReader) Read(_ context.Context, b []byte) (int, error) {
	return f.r.Read(b)
}

func (f *fakeRacingReader) ReadFull(_ context.Context, b []byte) (int, error) {
	return io.ReadFull(f.r, b)
}

// errorRacingReader always returns an error when Read is called.
type errorRacingReader struct{}

func (e *errorRacingReader) Read(_ context.Context, _ []byte) (int, error) {
	return 0, fmt.Errorf("read error")
}

func (e *errorRacingReader) ReadFull(_ context.Context, b []byte) (int, error) {
	return 0, fmt.Errorf("readfull error")
}

// TestEncodeVarInt tests that encodeVarInt returns the expected byte slices.
func TestEncodeVarInt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{300, []byte{0xAC, 0x02}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{^uint64(0), []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
	}

	for _, tt := range tests {
		encoded := encodeVarInt(tt.value)
		if !bytes.Equal(encoded, tt.expected) {
			t.Errorf("encodeVarInt(%d) = % X, want % X", tt.value, encoded, tt.expected)
		}
	}
}

// TestVarIntRoundTrip verifies that encoding then decoding returns the original value.
func TestVarIntRoundTrip(t *testing.T) {
	t.Parallel()
	testValues := []uint64{
		0, 1, 127, 128, 300, 16383, 16384,
		1<<32 - 1,  // max 32-bit value
		1<<63 - 1,  // max signed 64-bit value
		^uint64(0), // max unsigned 64-bit value
	}
	paddings := [][]byte{
		nil, {0x00}, {0xFF}, {0x00, 0x00}, {0xFF, 0xFF},
	}

	for _, v := range testValues {
		for _, padding := range paddings {
			t.Run(fmt.Sprintf("value %d, padding % X", v, padding), func(t *testing.T) {
				encoded := encodeVarInt(v)
				paddedEncoded := append(encoded, padding...)
				reader := newFakeRacingReader(paddedEncoded)
				decoded, readBytes, err := readVarInt(context.Background(), reader)
				if err != nil {
					t.Errorf("readVarInt error for value %d: %v", v, err)
				}
				if decoded != v {
					t.Errorf("round trip failed: encoded % X, decoded %d, expected %d", encoded, decoded, v)
				}
				AssertSliceEqual(t, encoded, readBytes)
				unreadBytes, err := io.ReadAll(reader.r)
				AssertNoError(t, err)
				AssertSliceEqual(t, unreadBytes, padding)
			})
		}
	}
}

// TestReadVarIntTooLong simulates a varint encoding that never terminates.
func TestReadVarIntTooLong(t *testing.T) {
	t.Parallel()
	// 10 bytes with continuation bit set (0x80)
	data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
	reader := newFakeRacingReader(data)
	_, readBytes, err := readVarInt(context.Background(), reader)
	if err == nil {
		t.Error("expected error for varint too long, got nil")
	} else if err.Error() != "varint too long" {
		t.Errorf("expected error 'varint too long', got %v", err)
	}
	AssertDeepEquals(t, readBytes, data)
}

// TestReadVarIntReadError verifies that a read error from the underlying reader is returned.
func TestReadVarIntReadError(t *testing.T) {
	t.Parallel()
	reader := &errorRacingReader{}
	_, readBytes, err := readVarInt(context.Background(), reader)
	if err == nil {
		t.Error("expected error from underlying reader, got nil")
	} else if err.Error() != "read error" {
		t.Errorf("expected error 'read error', got %v", err)
	}
	AssertDeepEquals(t, readBytes, []byte{})
}

func TestReadVarIntInvalid(t *testing.T) {
	t.Parallel()
	// The first 9 bytes have the continuation bit set (0x80).
	// The 10th byte triggers an invalid varint error.
	data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
	reader := newFakeRacingReader(data)
	_, readBytes, err := readVarInt(context.Background(), reader)
	if err == nil {
		t.Error("Expected error for invalid varint, got nil")
	} else if err.Error() != "failed to decode varint" {
		t.Errorf("Expected error 'failed to decode varint', got %v", err)
	}
	if !bytes.Equal(readBytes, data) {
		t.Errorf("Expected read bytes % X, got % X", data, readBytes)
	}
}

func TestSelectProtocol(ot *testing.T) {
	cases := []struct {
		name     string
		offers   []protocolVersion
		expected protocolVersion
	}{
		{
			name: "ServerEquals",
			offers: []protocolVersion{
				{back: 7, minor: 7, major: 5},
			},
			expected: protocolVersion{minor: 7, major: 5},
		},
		{
			name: "ServerGreater",
			offers: []protocolVersion{
				{back: 7, minor: 8, major: 5},
			},
			expected: protocolVersion{minor: 7, major: 5},
		},
		{
			name: "ServerLess",
			offers: []protocolVersion{
				{back: 6, minor: 6, major: 5},
				{back: 4, minor: 4, major: 4},
			},
			expected: protocolVersion{minor: 6, major: 5},
		},
	}

	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			restore := setTestVersions([4]protocolVersion{
				{major: 0xFF, minor: 0x01, back: 0x00},
				{major: 5, minor: 7, back: 7},
				{major: 4, minor: 4, back: 2},
				{major: 3, minor: 0, back: 0},
			})
			defer restore()

			candidate, err := selectProtocol(c.offers)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if candidate != c.expected {
				t.Errorf("expected %v, got %v", c.expected, candidate)
			}
		})
	}
}

// setTestVersions temporarily overrides the global versions variable and returns a
// function to restore the original value.
func setTestVersions(testVers [4]protocolVersion) func() {
	orig := versions
	versions = testVers
	return func() { versions = orig }
}
