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

// TestDecodeEncryptedFixtures checks every field of a decoded fixture.
func TestDecodeEncryptedFixtures(t *testing.T) {
	t.Parallel()

	t.Run("without aad", func(t *testing.T) {
		t.Parallel()

		encrypted, err := DecodeEncrypted(mustHex(t, fixtureBoolean.encrypted))
		if err != nil {
			t.Fatalf("DecodeEncrypted returned %v", err)
		}
		if encrypted.ProfileName != deterministicName {
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
		if keyID, ok := encrypted.Metadata.String(MetadataKeyID); !ok || keyID != deterministicKeyID {
			t.Errorf("key id is %q (present: %t), want %q", keyID, ok, deterministicKeyID)
		}
		iv, ok := encrypted.Metadata.Bytes(MetadataIV)
		if !ok || len(iv) != 12 {
			t.Errorf("iv is %x (present: %t), want 12 bytes", iv, ok)
		}
		if got := len(encrypted.Metadata); got != 2 {
			t.Errorf("metadata has %d entries, want 2", got)
		}
	})

	t.Run("with aad", func(t *testing.T) {
		t.Parallel()

		encrypted, err := DecodeEncrypted(mustHex(t, fixtureAADBound.encrypted))
		if err != nil {
			t.Fatalf("DecodeEncrypted returned %v", err)
		}
		if encrypted.TypeName != TypeString {
			t.Errorf("type name is %q", encrypted.TypeName)
		}
		if _, ok := encrypted.Metadata.Bytes(MetadataAAD); !ok {
			t.Fatal("aad is missing")
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

// TestEncodeEncryptedSortsMetadataByUtf8Bytes checks the ordering is over raw UTF-8 bytes,
// not locale order. The fixture tests pin the order of the real keys.
func TestEncodeEncryptedSortsMetadataByUtf8Bytes(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeEncrypted(Encrypted{
		Metadata: Metadata{"z": "", "Z": "", "aa": "", "a": ""},
	})
	if err != nil {
		t.Fatalf("EncodeEncrypted returned %v", err)
	}
	// Z, a, aa, z, each with an empty string value.
	want := "a4" + "815a80" + "816180" + "82616180" + "817a80"
	if got := hex.EncodeToString(encoded); !strings.HasSuffix(got, want) {
		t.Fatalf("metadata encoded to ...%s, want ...%s", got[len(got)-len(want):], want)
	}
}

// TestDecodeEncryptedMetadataEntries covers entries beyond the ones this profile version
// writes: an unknown key is kept, a value of another type is malformed.
func TestDecodeEncryptedMetadataEntries(t *testing.T) {
	t.Parallel()

	const prefix = fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
		fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + "a1"

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
		{name: "not a structure",
			value: fieldVersion + "9865" + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata},
		{name: "wrong structure tag",
			value: fieldVersion + "b866" + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata},
		{name: "too few fields",
			value: fieldVersion + "b765" + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata},
		{name: "too many fields",
			value: fieldVersion + "b965" + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				fieldMetadata + "c0"},
		{name: "profile name is not a string",
			value: fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
				"cc0170" + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + fieldMetadata},
		{name: "metadata is not a dictionary",
			value: fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor + "c3"},
		{name: "truncated",
			value: fieldVersion + fieldHeader + fieldProfileType + fieldProfileVersion +
				fieldProfile + fieldCipher + fieldTypeName + fieldMajor + fieldMinor +
				"a1" + "8161" + "8361"},
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

// TestDecodeEncryptedAcceptsTheUnmutatedValue checks the value the cases above mutate decodes.
func TestDecodeEncryptedAcceptsTheUnmutatedValue(t *testing.T) {
	t.Parallel()

	if _, err := DecodeEncrypted(mustHex(t, validEncrypted)); err != nil {
		t.Fatalf("DecodeEncrypted returned %v", err)
	}
}
