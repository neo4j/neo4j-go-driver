/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ogm

import (
	"testing"
	"testing/quick"
)

func TestKeysOf(t *testing.T) {
	assertion := func(dict map[string]int) bool {
		keys := keysOf(dict)
		if len(keys) != len(dict) {
			return false
		}
		for _, key := range keys {
			if _, found := dict[key]; !found {
				return false
			}
		}
		return true
	}

	if err := quick.Check(assertion, nil); err != nil {
		t.Error(err)
	}
}
