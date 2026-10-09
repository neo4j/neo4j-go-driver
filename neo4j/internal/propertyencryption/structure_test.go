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
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// Encrypted values taken verbatim from TestKit's deterministic fixtures.
const (
	fixtureBoolean = "01b86588454e56454c4f5045018d6465" +
		"7465726d696e6973746963cc11766afe" +
		"8a94ffb2fd0e0bc10caed2471a028742" +
		"4f4f4c45414e0100a2826976cc0c0001" +
		"02030405060708090a0b866b65795f69" +
		"648b746573746b69742d6b6579"

	fixtureAADBound = "01b86588454e56454c4f5045018d6465" +
		"7465726d696e6973746963cc1a9e19aa" +
		"f51fbb711fdeb241272be57efcd076c5" +
		"6200e29a4e44da86535452494e470100" +
		"a583616164cc0786726f772d3432d019" +
		"6161645f656e636f64696e675f736368" +
		"656d655f6d616a6f7201d0196161645f" +
		"656e636f64696e675f736368656d655f" +
		"6d696e6f7200826976cc0c48494a4b4c" +
		"4d4e4f50515253866b65795f69648b74" +
		"6573746b69742d6b6579"

	fixtureKeyID = "testkit-key"
)

// TestDecodeEncryptedFixtures checks every field of a decoded fixture.
func TestDecodeEncryptedFixtures(t *testing.T) {
	t.Parallel()

	t.Run("without aad", func(t *testing.T) {
		t.Parallel()

		encrypted, err := DecodeEncrypted(mustHex(t, fixtureBoolean))
		if err != nil {
			t.Fatalf("DecodeEncrypted returned %v", err)
		}
		if encrypted.ProfileName != "deterministic" {
			t.Errorf("profile is %q", encrypted.ProfileName)
		}
		if encrypted.TypeName != TypeBoolean {
			t.Errorf("type name is %q", encrypted.TypeName)
		}
		if encrypted.Baseline != baseline10 {
			t.Errorf("baseline is %s, want 1.0", encrypted.Baseline)
		}
		// One plaintext byte plus the authentication tag.
		if len(encrypted.CipherOutput) != 17 {
			t.Errorf("cipher output is %d bytes, want 17", len(encrypted.CipherOutput))
		}
		if keyID, ok := encrypted.Metadata.String(MetadataKeyID); !ok || keyID != fixtureKeyID {
			t.Errorf("key id is %q (present: %t), want %q", keyID, ok, fixtureKeyID)
		}
		iv, ok := encrypted.Metadata.Bytes(MetadataIV)
		if !ok || len(iv) != 12 {
			t.Errorf("iv is %x (present: %t), want 12 bytes", iv, ok)
		}
		if _, ok := encrypted.Metadata.Bytes(MetadataAAD); ok {
			t.Error("aad is present, it must be omitted when the caller supplied none")
		}
		if got := len(encrypted.Metadata); got != 2 {
			t.Errorf("metadata has %d entries, want 2", got)
		}
	})

	t.Run("with aad", func(t *testing.T) {
		t.Parallel()

		encrypted, err := DecodeEncrypted(mustHex(t, fixtureAADBound))
		if err != nil {
			t.Fatalf("DecodeEncrypted returned %v", err)
		}
		if encrypted.TypeName != TypeString {
			t.Errorf("type name is %q", encrypted.TypeName)
		}
		aad, ok := encrypted.Metadata.Bytes(MetadataAAD)
		if !ok {
			t.Fatal("aad is missing")
		}
		// The stored AAD is the encoded value, not the raw text.
		if got := hex.EncodeToString(aad); got != "86726f772d3432" {
			t.Errorf("aad is %s, want the encoding of the string row-42", got)
		}
		major, ok := encrypted.Metadata.Int(MetadataAADEncodingSchemeMajor)
		if !ok || major != 1 {
			t.Errorf("aad scheme major is %d (present: %t), want 1", major, ok)
		}
		minor, ok := encrypted.Metadata.Int(MetadataAADEncodingSchemeMinor)
		if !ok || minor != 0 {
			t.Errorf("aad scheme minor is %d (present: %t), want 0", minor, ok)
		}
		if got := len(encrypted.Metadata); got != 5 {
			t.Errorf("metadata has %d entries, want 5", got)
		}
	})
}

// TestEncodeEncryptedMatchesFixtures re-encodes the fixtures byte for byte, pinning the
// structure tag, field order, metadata value types and key ordering.
func TestEncodeEncryptedMatchesFixtures(t *testing.T) {
	t.Parallel()

	for name, fixture := range map[string]string{
		"without aad": fixtureBoolean,
		"with aad":    fixtureAADBound,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			decoded, err := DecodeEncrypted(mustHex(t, fixture))
			if err != nil {
				t.Fatalf("DecodeEncrypted returned %v", err)
			}
			encoded, err := EncodeEncrypted(decoded)
			if err != nil {
				t.Fatalf("EncodeEncrypted returned %v", err)
			}
			if got := hex.EncodeToString(encoded); got != fixture {
				t.Errorf("re-encoded to\n%s\nwant\n%s", got, fixture)
			}
		})
	}
}

// TestEncodeEncryptedSortsMetadataByUtf8Bytes checks the ordering is over raw UTF-8 bytes,
// not locale or code point order. The fixture tests pin the order of the real keys.
func TestEncodeEncryptedSortsMetadataByUtf8Bytes(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeEncrypted(Encrypted{
		Metadata: Metadata{"é": "", "z": "", "Z": "", "aa": "", "a": ""},
	})
	if err != nil {
		t.Fatalf("EncodeEncrypted returned %v", err)
	}
	// Z, a, aa, z, é, each with an empty string value.
	want := "a5" + "815a80" + "816180" + "82616180" + "817a80" + "82c3a980"
	if got := hex.EncodeToString(encoded); !strings.HasSuffix(got, want) {
		t.Fatalf("metadata encoded to ...%s, want ...%s", got[len(got)-len(want):], want)
	}
}

// TestEncodeEncryptedRoundTrip covers a structure this driver produced itself.
func TestEncodeEncryptedRoundTrip(t *testing.T) {
	t.Parallel()

	metadata := Metadata{MetadataKeyID: "a-key", MetadataIV: []byte{1, 2, 3}}

	want := Encrypted{
		ProfileType:    ProfileTypeEnvelope,
		ProfileVersion: EnvelopeProfileVersion,
		ProfileName:    "profile",
		CipherOutput:   []byte{9, 8, 7},
		TypeName:       TypeString,
		Baseline:       baseline10,
		Metadata:       metadata,
	}
	encoded, err := EncodeEncrypted(want)
	if err != nil {
		t.Fatalf("EncodeEncrypted returned %v", err)
	}
	if encoded[0] != 0x01 {
		t.Errorf("encoding version byte is %#x, want 0x01", encoded[0])
	}
	if encoded[2] != 0x65 {
		t.Errorf("structure tag is %#x, want 0x65", encoded[2])
	}

	got, err := DecodeEncrypted(encoded)
	if err != nil {
		t.Fatalf("DecodeEncrypted returned %v", err)
	}
	if got.ProfileType != want.ProfileType || got.ProfileVersion != want.ProfileVersion ||
		got.ProfileName != want.ProfileName || got.TypeName != want.TypeName ||
		got.Baseline != want.Baseline {
		t.Errorf("round tripped to %+v, want %+v", got, want)
	}
	if keyID, _ := got.Metadata.String(MetadataKeyID); keyID != "a-key" {
		t.Errorf("key id round tripped to %q", keyID)
	}
}

// TestDecodeEncryptedMetadataEntries covers entries beyond the ones this profile version
// writes: an unknown key is kept, a value of another type is malformed.
func TestDecodeEncryptedMetadataEntries(t *testing.T) {
	t.Parallel()

	const prefix = fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
		fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + "a2" +
		"826976" + "cc0101" // iv: 01

	t.Run("unknown key", func(t *testing.T) {
		t.Parallel()

		decoded, err := DecodeEncrypted(mustHex(t, prefix+"827a7a"+"8161")) // zz: "a"
		if err != nil {
			t.Fatalf("DecodeEncrypted returned %v", err)
		}
		if got, _ := decoded.Metadata.String("zz"); got != "a" {
			t.Errorf("zz is %q, want a", got)
		}
	})

	for name, entry := range map[string]string{
		"boolean":   "c3",
		"null":      "c0",
		"float":     "c1400a000000000000",
		"list":      "9201c3",
		"structure": "b144" + "01",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeEncrypted(mustHex(t, prefix+"827a7a"+entry))
			var malformed *MalformedError
			if !errors.As(err, &malformed) {
				t.Fatalf("DecodeEncrypted returned %v, want a *MalformedError", err)
			}
		})
	}
}

// The fields of a well-formed encrypted value, so the cases below can name what they change.
const (
	fieldVersion        = "01"
	fieldHeader         = "b865"               // structure, 8 fields, tag "e"
	fieldProfileType    = "88454e56454c4f5045" // "ENVELOPE"
	fieldProfileVersion = "01"
	fieldProfile        = "8170"           // "p"
	fieldCipher         = "cc00"           // empty
	fieldTypeName       = "86535452494e47" // "STRING"
	fieldMajor          = "01"
	fieldMinor          = "00"
	fieldMetadata       = "a0" // empty

	validEncrypted = fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
		fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + fieldMetadata
)

// TestDecodeEncryptedRejects covers bytes that are not a valid encrypted value.
func TestDecodeEncryptedRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "unknown encoding version",
			value: "02" + fieldHeader + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata},
		{name: "not a structure", value: fieldVersion + "8161"},
		{name: "wrong structure tag",
			value: fieldVersion + "b666" + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata},
		{name: "too few fields",
			value: fieldVersion + "b765" + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor},
		{name: "profile name is not a string",
			value: fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
				"01" + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + fieldMetadata},
		{name: "metadata is not a dictionary",
			value: fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + "c3"},
		{name: "truncated", value: fieldVersion + fieldHeader + fieldProfileType +
			fieldProfileVersion + fieldProfile + fieldCipher},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeEncrypted(mustHex(t, test.value))
			var malformed *MalformedError
			if !errors.As(err, &malformed) {
				t.Fatalf("DecodeEncrypted returned %v, want a *MalformedError", err)
			}
		})
	}
}

// TestDecodeEncryptedRejectsAnotherProfile checks the profile type and version reach the error.
func TestDecodeEncryptedRejectsAnotherProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileType string
		version     string
		wantType    string
		wantVersion int64
	}{
		{
			name:        "unknown profile type",
			profileType: "8b5354415449435f4b455953", // "STATIC_KEYS"
			version:     fieldProfileVersion,
			wantType:    "STATIC_KEYS",
			wantVersion: 1,
		},
		{
			name:        "newer profile version",
			profileType: fieldProfileType,
			version:     "02",
			wantType:    ProfileTypeEnvelope,
			wantVersion: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			value := fieldVersion + fieldHeader + test.profileType + test.version +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata
			_, err := DecodeEncrypted(mustHex(t, value))
			var profileErr *UnsupportedProfileError
			if !errors.As(err, &profileErr) {
				t.Fatalf("DecodeEncrypted returned %v, want an *UnsupportedProfileError", err)
			}
			if profileErr.ProfileType != test.wantType {
				t.Errorf("ProfileType is %q, want %q", profileErr.ProfileType, test.wantType)
			}
			if profileErr.ProfileVersion != test.wantVersion {
				t.Errorf("ProfileVersion is %d, want %d", profileErr.ProfileVersion, test.wantVersion)
			}
			if !strings.Contains(err.Error(), test.wantType) {
				t.Errorf("the error does not name the profile type: %v", err)
			}
		})
	}
}

// TestDecodeEncryptedAcceptsTheUnmutatedValue checks the value the cases above mutate does
// itself decode, so none of them can pass for the wrong reason.
func TestDecodeEncryptedAcceptsTheUnmutatedValue(t *testing.T) {
	t.Parallel()

	if _, err := DecodeEncrypted(mustHex(t, validEncrypted)); err != nil {
		t.Fatalf("DecodeEncrypted returned %v", err)
	}
}
