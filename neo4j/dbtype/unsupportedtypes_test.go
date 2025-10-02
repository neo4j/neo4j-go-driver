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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
)

func TestUnsupportedTypes(t *testing.T) {
	t.Run("String representation of UnsupportedType with nil message", func(t *testing.T) {
		unsupported := &UnsupportedType{
			Name:                   "UUID",
			MinimumProtocolVersion: db.ProtocolVersion{Major: 6, Minor: 0},
			Message:                nil,
		}
		actual := unsupported.String()
		expect := "UnsupportedType[UUID]"
		if actual != expect {
			t.Errorf("Expected %s but was %s", expect, actual)
		}
	})

	t.Run("String representation of UnsupportedType with non-nil message", func(t *testing.T) {
		message := "This type requires a newer driver version"
		unsupported := &UnsupportedType{
			Name:                   "CustomType",
			MinimumProtocolVersion: db.ProtocolVersion{Major: 6, Minor: 0},
			Message:                &message,
		}
		actual := unsupported.String()
		expect := "UnsupportedType[CustomType]"
		if actual != expect {
			t.Errorf("Expected %s but was %s", expect, actual)
		}
	})
}
