//go:build internal_neo4j_go_driver_testkit

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
	ipe "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/propertyencryption"
)

// PinIV makes the next Encrypt use iv, and returns a function that restores normal behaviour
// and reports whether iv was used. It lets TestKit assert byte-exact ciphertext.
//
// Reusing an initialisation vector with the same key breaks AES-GCM.
func (e *Encryption) PinIV(iv []byte) func() bool {
	used := false
	e.newIV = func() ([]byte, error) {
		used = true
		return iv, nil
	}
	return func() bool {
		e.newIV = ipe.NewIV
		return used
	}
}
