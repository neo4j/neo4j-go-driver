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
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
)

// UnsupportedType represents a type unknown to the driver, received from the server.
// This type is used, for instance, when a newer DBMS produces a result containing a type
// that the current version of the driver does not yet understand.
//
// Note that this type may only be received from the server, but cannot be sent to the
// server (e.g., as a query parameter).
//
// The attributes exposed by this type are meant for displaying and debugging purposes.
// They may change in future versions of the server, and should not be relied upon for
// any logic in your application.
// If your application requires handling this type, you must upgrade your driver to a
// version that supports it.
type UnsupportedType struct {
	// Name is the name of the type.
	Name string
	// MinimumProtocolVersion returns the minimum required Bolt protocol version that supports this type.
	// To understand which driver version this corresponds to, refer to the driver's release notes or documentation.
	//
	// Note: Bolt versions are not generally equivalent to driver versions.
	// See https://neo4j.com/docs/go-manual/current/data-types/ for which driver version is required for new types.
	MinimumProtocolVersion db.ProtocolVersion
	// Message contains any additional information provided by the server about this type.
	Message *string
}

// String returns a string representation of the UnsupportedType.
func (u *UnsupportedType) String() string {
	return fmt.Sprintf("UnsupportedType[%s]", u.Name)
}
