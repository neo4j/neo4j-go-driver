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
	"testing"
)

// Encrypted values taken verbatim from TestKit's deterministic fixtures.
const (
	fixtureBoolean = "01b6658d64657465726d696e69737469" +
		"63cc11877fe22670d0d3433e2a9c4dd5" +
		"fd17994b87424f4f4c45414e0100a282" +
		"6976cc0c000102030405060708090a0b" +
		"866b65795f69648b746573746b69742d" +
		"6b6579"

	fixtureAADBound = "01b6658d64657465726d696e69737469" +
		"63cc1a3a8af0d3820a0a549d75e42e59" +
		"6a18ff85ee74fb51dce4bc0300865354" +
		"52494e470100a583616164cc0786726f" +
		"772d3432d0196161645f656e636f6469" +
		"6e675f736368656d655f6d616a6f7201" +
		"d0196161645f656e636f64696e675f73" +
		"6368656d655f6d696e6f7200826976cc" +
		"0c48494a4b4c4d4e4f50515253866b65" +
		"795f69648b746573746b69742d6b6579"

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
		if got := encrypted.Metadata.len(); got != 2 {
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
		if got := encrypted.Metadata.len(); got != 5 {
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

// TestEncodeEncryptedSortsMetadata checks metadata key ordering. Keys are inserted out of
// order because randomised map iteration would otherwise make an unsorted implementation
// fail only intermittently.
func TestEncodeEncryptedSortsMetadata(t *testing.T) {
	t.Parallel()

	var metadata Metadata
	metadata.SetString(MetadataKeyID, "0")
	metadata.SetBytes(MetadataIV, []byte{1})
	metadata.SetInt(MetadataAADEncodingSchemeMinor, 0)
	metadata.SetInt(MetadataAADEncodingSchemeMajor, 1)
	metadata.SetBytes(MetadataAAD, []byte{2})

	want := []string{
		MetadataAAD,
		MetadataAADEncodingSchemeMajor,
		MetadataAADEncodingSchemeMinor,
		MetadataIV,
		MetadataKeyID,
	}
	for attempt := 0; attempt < 20; attempt++ {
		got := metadata.sortedKeys()
		if len(got) != len(want) {
			t.Fatalf("got %d keys, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("key %d is %q, want %q", i, got[i], want[i])
			}
		}
	}
}

// TestEncodeEncryptedSortsByUtf8Bytes checks the ordering is over raw UTF-8 bytes.
func TestEncodeEncryptedSortsByUtf8Bytes(t *testing.T) {
	t.Parallel()

	var metadata Metadata
	metadata.SetString("é", "")
	metadata.SetString("z", "")
	metadata.SetString("Z", "")
	metadata.SetString("aa", "")
	metadata.SetString("a", "")

	want := []string{"Z", "a", "aa", "z", "é"}
	got := metadata.sortedKeys()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keys sorted to %q, want %q", got, want)
		}
	}
}

// TestEncodeEncryptedRoundTrip covers a structure this driver produced itself.
func TestEncodeEncryptedRoundTrip(t *testing.T) {
	t.Parallel()

	var metadata Metadata
	metadata.SetString(MetadataKeyID, "a-key")
	metadata.SetBytes(MetadataIV, []byte{1, 2, 3})

	want := Encrypted{
		ProfileName:  "profile",
		CipherOutput: []byte{9, 8, 7},
		TypeName:     TypeString,
		Baseline:     baseline10,
		Metadata:     metadata,
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
	if got.ProfileName != want.ProfileName || got.TypeName != want.TypeName ||
		got.Baseline != want.Baseline {
		t.Errorf("round tripped to %+v, want %+v", got, want)
	}
	if keyID, _ := got.Metadata.String(MetadataKeyID); keyID != "a-key" {
		t.Errorf("key id round tripped to %q", keyID)
	}
}

// TestDecodeEncryptedIgnoresUnknownMetadata covers metadata a later profile version may add.
func TestDecodeEncryptedIgnoresUnknownMetadata(t *testing.T) {
	t.Parallel()

	const prefix = fieldVersion + fieldHeader + fieldProfile + fieldCipher + fieldTypeName +
		fieldMajor + fieldMinor

	// Each case adds one metadata entry this driver has no use for, alongside the iv it
	// does read. The shapes cover every branch the skip has to walk over.
	unknown := map[string]string{
		"boolean":            "82" + "7a7a" + "c3",
		"null":               "82" + "7a7a" + "c0",
		"float":              "82" + "7a7a" + "c1400a000000000000",
		"uuid":               "82" + "7a7a" + "e0000102030405060708090a0b0c0d0e0f",
		"list":               "82" + "7a7a" + "9201c3",
		"nested list":        "82" + "7a7a" + "91" + "920102",
		"dictionary":         "82" + "7a7a" + "a1" + "8161" + "01",
		"nested dictionary":  "82" + "7a7a" + "a1" + "8161" + "a1" + "8162" + "c3",
		"structure":          "82" + "7a7a" + "b144" + "01",
		"structure of lists": "82" + "7a7a" + "b17a" + "920102",
	}

	for name, entry := range unknown {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			value := prefix + "a2" + "826976" + "cc0101" + entry
			decoded, err := DecodeEncrypted(mustHex(t, value))
			if err != nil {
				t.Fatalf("DecodeEncrypted returned %v", err)
			}
			if iv, ok := decoded.Metadata.Bytes(MetadataIV); !ok || len(iv) != 1 {
				t.Errorf("iv is %x (present: %t)", iv, ok)
			}
		})
	}
}

// The fields of a well-formed encrypted value, so the cases below can name what they change.
const (
	fieldVersion  = "01"
	fieldHeader   = "b665"           // structure, 6 fields, tag "e"
	fieldProfile  = "8170"           // "p"
	fieldCipher   = "cc00"           // empty
	fieldTypeName = "86535452494e47" // "STRING"
	fieldMajor    = "01"
	fieldMinor    = "00"
	fieldMetadata = "a0" // empty

	validEncrypted = fieldVersion + fieldHeader + fieldProfile + fieldCipher +
		fieldTypeName + fieldMajor + fieldMinor + fieldMetadata
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
			value: "02" + fieldHeader + fieldProfile + fieldCipher + fieldTypeName +
				fieldMajor + fieldMinor + fieldMetadata},
		{name: "not a structure", value: fieldVersion + "8161"},
		{name: "wrong structure tag",
			value: fieldVersion + "b666" + fieldProfile + fieldCipher + fieldTypeName +
				fieldMajor + fieldMinor + fieldMetadata},
		{name: "too few fields",
			value: fieldVersion + "b565" + fieldProfile + fieldCipher + fieldTypeName +
				fieldMajor + fieldMinor},
		{name: "profile name is not a string",
			value: fieldVersion + fieldHeader + "01" + fieldCipher + fieldTypeName +
				fieldMajor + fieldMinor + fieldMetadata},
		{name: "metadata is not a dictionary",
			value: fieldVersion + fieldHeader + fieldProfile + fieldCipher + fieldTypeName +
				fieldMajor + fieldMinor + "c3"},
		{name: "truncated", value: fieldVersion + fieldHeader + fieldProfile + fieldCipher},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decoded, err := DecodeEncrypted(mustHex(t, test.value))
			if err == nil {
				t.Fatalf("DecodeEncrypted returned %+v, want an error", decoded)
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
