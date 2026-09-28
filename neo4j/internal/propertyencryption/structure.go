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
	"fmt"
	"maps"
	"slices"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/packstream"
)

const (
	// encryptedValueVersion versions the outer binary format, independently of the Encrypted
	// structure and the encoding scheme.
	encryptedValueVersion = 0x01
	encryptedTag          = 0x65
	encryptedFields       = 8

	ProfileTypeEnvelope    = "ENVELOPE"
	EnvelopeProfileVersion = 1
)

// Metadata keys written by the Envelope profile.
const (
	MetadataKeyID                  = "key_id"
	MetadataIV                     = "iv"
	MetadataAAD                    = "aad"
	MetadataAADEncodingSchemeMajor = "aad_encoding_scheme_major"
	MetadataAADEncodingSchemeMinor = "aad_encoding_scheme_minor"
)

// Encrypted carries an encrypted value together with what is needed to decrypt and interpret
// it. It is a driver-defined structure and is not sent over the wire.
type Encrypted struct {
	ProfileType    string
	ProfileVersion int64
	ProfileName    string
	CipherOutput   []byte
	TypeName       string
	Baseline       Version
	Metadata       Metadata
}

// UnsupportedProfileError reports an encrypted value produced by a profile type or profile
// version this driver does not implement.
type UnsupportedProfileError struct {
	ProfileType    string
	ProfileVersion int64
}

func (e *UnsupportedProfileError) Error() string {
	return fmt.Sprintf(
		"the encrypted value was produced by profile type %q version %d, "+
			"which this driver does not support",
		e.ProfileType, e.ProfileVersion)
}

// Metadata is the profile-specific metadata dictionary of an Encrypted structure. Values are
// string, []byte or int64.
type Metadata map[string]any

func (m Metadata) String(key string) (string, bool) {
	v, ok := m[key].(string)
	return v, ok
}

func (m Metadata) Bytes(key string) ([]byte, bool) {
	v, ok := m[key].([]byte)
	return v, ok
}

func (m Metadata) Int(key string) (int64, bool) {
	v, ok := m[key].(int64)
	return v, ok
}

// EncodeEncrypted encodes an Encrypted structure into the bytes handed to the user, a
// one-byte encoding version followed by the unchunked structure.
func EncodeEncrypted(e Encrypted) ([]byte, error) {
	var packer packstream.Packer
	packer.Begin(append(make([]byte, 0, 128), encryptedValueVersion))
	packer.StructHeader(encryptedTag, encryptedFields)
	packer.String(e.ProfileType)
	packer.Int64(e.ProfileVersion)
	packer.String(e.ProfileName)
	packer.Bytes(e.CipherOutput)
	packer.String(e.TypeName)
	packer.Int(e.Baseline.Major)
	packer.Int(e.Baseline.Minor)

	// Keys are ordered by their UTF-8 bytes, which is part of the format.
	packer.MapHeader(len(e.Metadata))
	for _, key := range slices.Sorted(maps.Keys(e.Metadata)) {
		packer.String(key)
		switch value := e.Metadata[key].(type) {
		case string:
			packer.String(value)
		case []byte:
			packer.Bytes(value)
		case int64:
			packer.Int64(value)
		default:
			return nil, fmt.Errorf("metadata entry %q has unsupported type %T", key, value)
		}
	}

	return packer.End()
}

// DecodeEncrypted decodes the bytes produced by EncodeEncrypted.
func DecodeEncrypted(value []byte) (Encrypted, error) {
	if len(value) == 0 {
		return Encrypted{}, &MalformedError{Message: "an encrypted value cannot be empty"}
	}
	if value[0] != encryptedValueVersion {
		return Encrypted{}, &MalformedError{Message: fmt.Sprintf(
			"unknown encrypted value encoding version %#x, this driver supports %#x",
			value[0], encryptedValueVersion)}
	}

	d := decoder{recorded: Implemented()}
	d.unpacker.Reset(value[1:])

	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedStruct {
		return Encrypted{}, &MalformedError{Message: "an encrypted value must hold a structure"}
	}
	if tag := d.unpacker.StructTag(); tag != encryptedTag {
		return Encrypted{}, &MalformedError{Message: fmt.Sprintf(
			"expected the Encrypted structure tag %#x but found %#x", encryptedTag, tag)}
	}
	if fields := d.unpacker.Len(); fields != encryptedFields {
		return Encrypted{}, &MalformedError{Message: fmt.Sprintf(
			"the Encrypted structure should have %d fields but has %d", encryptedFields, fields)}
	}

	encrypted := Encrypted{
		ProfileType:    d.string(),
		ProfileVersion: d.int(),
	}
	if d.err != nil {
		return Encrypted{}, d.err
	}
	// Checked before the rest is read, which may not be interpretable under another profile.
	if encrypted.ProfileType != ProfileTypeEnvelope ||
		encrypted.ProfileVersion != EnvelopeProfileVersion {
		return Encrypted{}, &UnsupportedProfileError{
			ProfileType:    encrypted.ProfileType,
			ProfileVersion: encrypted.ProfileVersion,
		}
	}

	encrypted.ProfileName = d.string()
	encrypted.CipherOutput = d.bytes()
	encrypted.TypeName = d.string()
	encrypted.Baseline = Version{Major: int(d.int()), Minor: int(d.int())}
	encrypted.Metadata = d.metadata()

	if d.err != nil {
		return Encrypted{}, d.err
	}
	if d.unpacker.Err != nil {
		return Encrypted{}, &MalformedError{Message: d.unpacker.Err.Error()}
	}
	return encrypted, nil
}

func (d *decoder) metadata() Metadata {
	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedMap {
		d.malformed("the Encrypted metadata must be a dictionary")
		return nil
	}
	entries := d.unpacker.Len()
	if d.unpacker.Err != nil {
		return nil
	}

	metadata := make(Metadata, entries)
	for i := uint32(0); i < entries; i++ {
		key := d.string()
		if d.err != nil {
			return metadata
		}
		d.unpacker.Next()
		switch d.unpacker.Curr {
		case packstream.PackedStr:
			metadata[key] = d.unpacker.String()
		case packstream.PackedByteArray:
			metadata[key] = d.unpacker.ByteArray()
		case packstream.PackedInt:
			metadata[key] = d.unpacker.Int()
		default:
			d.malformed("the Encrypted metadata entry %q is not a string, bytes or integer", key)
			return metadata
		}
	}
	return metadata
}
