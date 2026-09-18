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

// Package propertyencryption encodes Neo4j property values to bytes, encrypts them, and
// reverses both.
//
// The encoding is versioned by the Bolt Value Encoding Scheme rather than by the Bolt
// protocol, and so is kept separate from internal/bolt: an encrypted value must decode
// identically whatever protocol version its connection negotiated, and whatever changes are
// later made to the wire format.
package propertyencryption

import "fmt"

// The Bolt Value Encoding Scheme version implemented here.
const (
	schemeMajor = 1
	schemeMinor = 0
)

// Neo4j property type names, recorded with an encrypted value. They describe a value the
// driver cannot decode and MUST NOT be used to decide how a value is decoded.
const (
	TypeBoolean       = "BOOLEAN"
	TypeBytes         = "BYTES"
	TypeDate          = "DATE"
	TypeDuration      = "DURATION"
	TypeFloat         = "FLOAT"
	TypeInteger       = "INTEGER"
	TypeList          = "LIST"
	TypeLocalDateTime = "LOCAL DATETIME"
	TypeLocalTime     = "LOCAL TIME"
	TypePoint         = "POINT"
	TypeString        = "STRING"
	TypeUUID          = "UUID"
	TypeVector        = "VECTOR"
	TypeZonedDateTime = "ZONED DATETIME"
	TypeZonedTime     = "ZONED TIME"
)

// Version is a Bolt Value Encoding Scheme version.
type Version struct {
	Major int
	Minor int
}

// Implemented returns the scheme version implemented here.
func Implemented() Version {
	return Version{Major: schemeMajor, Minor: schemeMinor}
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// atLeast reports whether v is greater than or equal to other.
func (v Version) atLeast(other Version) bool {
	if v.Major != other.Major {
		return v.Major > other.Major
	}
	return v.Minor >= other.Minor
}

// max returns the greater of v and other.
func (v Version) max(other Version) Version {
	if v.atLeast(other) {
		return v
	}
	return other
}

// typeBaselines maps each property type to the scheme version that introduced its encoding.
// A new minor version adds rows for the types it introduces, for example a type added in 1.1
// gets {Major: 1, Minor: 1}, and leaves existing rows untouched.
var typeBaselines = map[string]Version{
	TypeBoolean:       {Major: 1, Minor: 0},
	TypeBytes:         {Major: 1, Minor: 0},
	TypeDate:          {Major: 1, Minor: 0},
	TypeDuration:      {Major: 1, Minor: 0},
	TypeFloat:         {Major: 1, Minor: 0},
	TypeInteger:       {Major: 1, Minor: 0},
	TypeList:          {Major: 1, Minor: 0},
	TypeLocalDateTime: {Major: 1, Minor: 0},
	TypeLocalTime:     {Major: 1, Minor: 0},
	TypePoint:         {Major: 1, Minor: 0},
	TypeString:        {Major: 1, Minor: 0},
	TypeUUID:          {Major: 1, Minor: 0},
	TypeVector:        {Major: 1, Minor: 0},
	TypeZonedDateTime: {Major: 1, Minor: 0},
	TypeZonedTime:     {Major: 1, Minor: 0},
}

// aadTypes is the subset of property types permitted as additional authenticated data,
// excluding those whose representation can differ between two equivalent values.
var aadTypes = map[string]struct{}{
	TypeBoolean:   {},
	TypeBytes:     {},
	TypeDate:      {},
	TypeInteger:   {},
	TypeLocalTime: {},
	TypePoint:     {},
	TypeString:    {},
	TypeUUID:      {},
	TypeZonedTime: {},
}

// ValueError reports a value that cannot be encoded under this scheme.
type ValueError struct {
	Message string
}

func (e *ValueError) Error() string {
	return e.Message
}
