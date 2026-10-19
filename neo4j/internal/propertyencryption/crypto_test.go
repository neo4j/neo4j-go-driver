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

package propertyencryption

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
)

// The data encryption key TestKit's deterministic fixtures are encrypted with.
const (
	deterministicKeyID  = "testkit-key"
	deterministicName   = "deterministic"
	deterministicDEKHex = "9a108cc9bfff252dba716c60dfb3dfcc1194b03b24c1373bcf266882f3d6156b"
)

type deterministicFixture struct {
	value     any
	iv        string
	aad       any
	encrypted string
}

// TestKit's deterministic fixtures.
var (
	fixtureBoolean = deterministicFixture{
		value: true,
		iv:    "000102030405060708090a0b",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc11766afe" +
			"8a94ffb2fd0e0bc10caed2471a028742" +
			"4f4f4c45414e0100a2826976cc0c0001" +
			"02030405060708090a0b866b65795f69" +
			"648b746573746b69742d6b6579",
	}
	fixtureAADBound = deterministicFixture{
		value: "aad-bound",
		iv:    "48494a4b4c4d4e4f50515253",
		aad:   "row-42",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc1a9e19aa" +
			"f51fbb711fdeb241272be57efcd076c5" +
			"6200e29a4e44da86535452494e470100" +
			"a583616164cc0786726f772d3432d019" +
			"6161645f656e636f64696e675f736368" +
			"656d655f6d616a6f7201d0196161645f" +
			"656e636f64696e675f736368656d655f" +
			"6d696e6f7200826976cc0c48494a4b4c" +
			"4d4e4f50515253866b65795f69648b74" +
			"6573746b69742d6b6579",
	}
	deterministicFixtures = []deterministicFixture{fixtureBoolean, fixtureAADBound}
)

// TestEncryptsToKnownBytes checks the exact bytes produced with the key and initialisation
// vector both pinned, covering value encoding, the cipher, metadata and structure encoding.
func TestEncryptsToKnownBytes(t *testing.T) {
	t.Parallel()

	key := fixtureKey(t)

	for _, fixture := range deterministicFixtures {
		t.Run(fmt.Sprintf("%v", fixture.value), func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeValue(fixture.value)
			if err != nil {
				t.Fatalf("EncodeValue(%#v) returned %v", fixture.value, err)
			}

			iv := mustHex(t, fixture.iv)
			metadata := Metadata{MetadataKeyID: deterministicKeyID, MetadataIV: iv}

			var aad []byte
			if fixture.aad != nil {
				encodedAAD, err := EncodeAAD(fixture.aad)
				if err != nil {
					t.Fatalf("EncodeAAD(%#v) returned %v", fixture.aad, err)
				}
				aad = encodedAAD.Bytes
				metadata[MetadataAAD] = aad
				metadata[MetadataAADEncodingSchemeMajor] = int64(encodedAAD.Baseline.Major)
				metadata[MetadataAADEncodingSchemeMinor] = int64(encodedAAD.Baseline.Minor)
			}

			cipherOutput, err := key.Seal(iv, encoded.Bytes, aad)
			if err != nil {
				t.Fatalf("Seal returned %v", err)
			}

			got, err := EncodeEncrypted(Encrypted{
				ProfileType:    ProfileTypeEnvelope,
				ProfileVersion: EnvelopeProfileVersion,
				ProfileName:    deterministicName,
				CipherOutput:   cipherOutput,
				TypeName:       encoded.TypeName,
				Baseline:       encoded.Baseline,
				Metadata:       metadata,
			})
			if err != nil {
				t.Fatalf("EncodeEncrypted returned %v", err)
			}
			if hex.EncodeToString(got) != fixture.encrypted {
				t.Errorf("encrypted %#v to\n%x\nwant\n%s", fixture.value, got, fixture.encrypted)
			}
		})
	}
}

func TestDecryptsKnownBytes(t *testing.T) {
	t.Parallel()

	key := fixtureKey(t)

	for _, fixture := range deterministicFixtures {
		t.Run(fmt.Sprintf("%v", fixture.value), func(t *testing.T) {
			t.Parallel()

			encrypted, err := DecodeEncrypted(mustHex(t, fixture.encrypted))
			if err != nil {
				t.Fatalf("DecodeEncrypted returned %v", err)
			}
			iv, _ := encrypted.Metadata.Bytes(MetadataIV)
			aad, _ := encrypted.Metadata.Bytes(MetadataAAD)

			plaintext, err := key.Open(iv, encrypted.CipherOutput, aad)
			if err != nil {
				t.Fatalf("Open returned %v", err)
			}
			decoded, err := DecodeValue(plaintext, encrypted.TypeName, encrypted.Baseline)
			if err != nil {
				t.Fatalf("DecodeValue returned %v", err)
			}
			if !sameValue(decoded, fixture.value) {
				t.Errorf("decrypted to %#v, want %#v", decoded, fixture.value)
			}
		})
	}
}

func TestOpenRejects(t *testing.T) {
	t.Parallel()

	key := fixtureKey(t)
	iv := mustHex(t, "000102030405060708090a0b")
	cipherOutput, err := key.Seal(iv, []byte{0xc3}, nil)
	if err != nil {
		t.Fatalf("Seal returned %v", err)
	}

	t.Run("tampered ciphertext", func(t *testing.T) {
		t.Parallel()
		tampered := append([]byte(nil), cipherOutput...)
		tampered[0] ^= 0xff
		_, err := key.Open(iv, tampered, nil)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("iv of the wrong size", func(t *testing.T) {
		t.Parallel()
		// AES-GCM panics on a nonce of the wrong size, and the iv comes from stored bytes.
		if _, err := key.Open(iv[:IVSize-1], cipherOutput, nil); err == nil {
			t.Fatal("Open accepted an 11 byte iv")
		}
	})
	t.Run("cipher output shorter than the tag", func(t *testing.T) {
		t.Parallel()
		_, err := key.Open(iv, cipherOutput[:15], nil)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
}

func TestSealRejectsWrongIVSize(t *testing.T) {
	t.Parallel()

	key := fixtureKey(t)
	for _, size := range []int{11, 13} {
		if _, err := key.Seal(make([]byte, size), []byte{1}, nil); err == nil {
			t.Errorf("Seal accepted a %d byte iv", size)
		}
	}
}

func TestNewDataKeyRejectsKeysThatAreNotAes256(t *testing.T) {
	t.Parallel()

	for _, size := range []int{16, 24} {
		if _, err := NewDataKey(make([]byte, size)); err == nil {
			t.Errorf("NewDataKey accepted a %d byte data encryption key", size)
		}
	}
}

func TestNewDEKIsRandom(t *testing.T) {
	t.Parallel()

	first, err := NewDEK()
	if err != nil {
		t.Fatalf("NewDEK returned %v", err)
	}
	second, err := NewDEK()
	if err != nil {
		t.Fatalf("NewDEK returned %v", err)
	}
	if len(first) != KeySize {
		t.Errorf("the key is %d bytes, want %d", len(first), KeySize)
	}
	if bytes.Equal(first, second) {
		t.Errorf("NewDEK returned %x twice", first)
	}
}

func fixtureKey(t *testing.T) *DataKey {
	t.Helper()

	key, err := NewDataKey(mustHex(t, deterministicDEKHex))
	if err != nil {
		t.Fatalf("NewDataKey returned %v", err)
	}
	return key
}
