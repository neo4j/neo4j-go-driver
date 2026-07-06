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

package dbtype

import (
	"encoding/hex"
	"fmt"
)

// UUID is a 16-byte RFC 9562 universally unique identifier.
type UUID [16]byte

// String returns the canonical 36-character hyphenated representation.
func (u UUID) String() string {
	buf := u.canonical()
	return string(buf[:])
}

// MarshalText implements [encoding.TextMarshaler].
func (u UUID) MarshalText() ([]byte, error) {
	buf := u.canonical()
	return buf[:], nil
}

// UnmarshalText implements [encoding.TextUnmarshaler].
func (u *UUID) UnmarshalText(text []byte) error {
	parsed, err := ParseUUID(string(text))
	if err != nil {
		return err
	}
	*u = parsed
	return nil
}

func (u UUID) canonical() [36]byte {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return buf
}

// ParseUUID parses the canonical 36-character hyphenated UUID form.
func ParseUUID(s string) (UUID, error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return UUID{}, fmt.Errorf("invalid UUID format: %q", s)
	}
	var hexBuf [32]byte
	copy(hexBuf[0:8], s[0:8])
	copy(hexBuf[8:12], s[9:13])
	copy(hexBuf[12:16], s[14:18])
	copy(hexBuf[16:20], s[19:23])
	copy(hexBuf[20:32], s[24:36])
	var u UUID
	if _, err := hex.Decode(u[:], hexBuf[:]); err != nil {
		return UUID{}, fmt.Errorf("invalid UUID format: %q", s)
	}
	return u, nil
}
