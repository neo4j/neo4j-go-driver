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
	"slices"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/packstream"
)

const (
	// encryptedValueVersion versions the outer binary format, independently of the Encrypted
	// structure and the encoding scheme.
	encryptedValueVersion = 0x01
	encryptedTag          = 0x65
	encryptedFields       = 6
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
	ProfileName  string
	CipherOutput []byte
	TypeName     string
	Baseline     Version
	Metadata     Metadata
}

// Metadata is the profile-specific metadata dictionary of an Encrypted structure.
type Metadata struct {
	strings map[string]string
	bytes   map[string][]byte
	ints    map[string]int64
}

func (m *Metadata) SetString(key, value string) {
	if m.strings == nil {
		m.strings = map[string]string{}
	}
	m.strings[key] = value
}

func (m *Metadata) SetBytes(key string, value []byte) {
	if m.bytes == nil {
		m.bytes = map[string][]byte{}
	}
	m.bytes[key] = value
}

func (m *Metadata) SetInt(key string, value int64) {
	if m.ints == nil {
		m.ints = map[string]int64{}
	}
	m.ints[key] = value
}

func (m *Metadata) String(key string) (string, bool) {
	v, ok := m.strings[key]
	return v, ok
}

func (m *Metadata) Bytes(key string) ([]byte, bool) {
	v, ok := m.bytes[key]
	return v, ok
}

func (m *Metadata) Int(key string) (int64, bool) {
	v, ok := m.ints[key]
	return v, ok
}

func (m *Metadata) len() int {
	return len(m.strings) + len(m.bytes) + len(m.ints)
}

// sortedKeys returns every key in ascending order of its UTF-8 bytes. The ordering is part
// of the format.
func (m *Metadata) sortedKeys() []string {
	keys := make([]string, 0, m.len())
	for key := range m.strings {
		keys = append(keys, key)
	}
	for key := range m.bytes {
		keys = append(keys, key)
	}
	for key := range m.ints {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// EncodeEncrypted encodes an Encrypted structure into the bytes handed to the user, a
// one-byte encoding version followed by the unchunked structure.
func EncodeEncrypted(e Encrypted) ([]byte, error) {
	var packer packstream.Packer
	packer.Begin(append(make([]byte, 0, 128), encryptedValueVersion))
	packer.StructHeader(encryptedTag, encryptedFields)
	packer.String(e.ProfileName)
	packer.Bytes(e.CipherOutput)
	packer.String(e.TypeName)
	packer.Int(e.Baseline.Major)
	packer.Int(e.Baseline.Minor)

	packer.MapHeader(e.Metadata.len())
	for _, key := range e.Metadata.sortedKeys() {
		packer.String(key)
		switch {
		case has(e.Metadata.strings, key):
			packer.String(e.Metadata.strings[key])
		case has(e.Metadata.bytes, key):
			packer.Bytes(e.Metadata.bytes[key])
		default:
			packer.Int64(e.Metadata.ints[key])
		}
	}

	return packer.End()
}

func has[T any](m map[string]T, key string) bool {
	_, ok := m[key]
	return ok
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
		ProfileName:  d.string(),
		CipherOutput: d.bytes(),
		TypeName:     d.string(),
	}
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
	var metadata Metadata
	d.unpacker.Next()
	if d.unpacker.Curr != packstream.PackedMap {
		d.malformed("the Encrypted metadata must be a dictionary")
		return metadata
	}
	entries := d.unpacker.Len()
	if d.unpacker.Err != nil {
		return metadata
	}

	for i := uint32(0); i < entries; i++ {
		key := d.string()
		if d.err != nil {
			return metadata
		}
		d.unpacker.Next()
		switch d.unpacker.Curr {
		case packstream.PackedStr:
			metadata.SetString(key, d.unpacker.String())
		case packstream.PackedByteArray:
			metadata.SetBytes(key, d.unpacker.ByteArray())
		case packstream.PackedInt:
			metadata.SetInt(key, d.unpacker.Int())
		default:
			// A later profile version may add metadata this driver has no use for.
			d.skip()
			if d.err != nil {
				return metadata
			}
		}
	}
	return metadata
}

// skip advances past the value the unpacker is currently positioned on.
func (d *decoder) skip() {
	switch d.unpacker.Curr {
	case packstream.PackedInt, packstream.PackedFloat, packstream.PackedTrue,
		packstream.PackedFalse, packstream.PackedNil:
		d.discardScalar()
	case packstream.PackedStr:
		_ = d.unpacker.String()
	case packstream.PackedByteArray:
		_ = d.unpacker.ByteArray()
	case packstream.PackedUUID:
		_ = d.unpacker.UUID()
	case packstream.PackedArray:
		length := d.unpacker.Len()
		for i := uint32(0); i < length && d.unpacker.Err == nil; i++ {
			d.unpacker.Next()
			d.skip()
		}
	case packstream.PackedMap:
		entries := d.unpacker.Len()
		for i := uint32(0); i < entries*2 && d.unpacker.Err == nil; i++ {
			d.unpacker.Next()
			d.skip()
		}
	case packstream.PackedStruct:
		d.unpacker.StructTag()
		fields := d.unpacker.Len()
		for i := uint32(0); i < fields && d.unpacker.Err == nil; i++ {
			d.unpacker.Next()
			d.skip()
		}
	default:
		d.malformed("cannot skip an unrecognised PackStream marker")
	}
}

func (d *decoder) discardScalar() {
	switch d.unpacker.Curr {
	case packstream.PackedInt:
		_ = d.unpacker.Int()
	case packstream.PackedFloat:
		_ = d.unpacker.Float()
	}
}
