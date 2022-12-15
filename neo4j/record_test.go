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

package neo4j_test

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"testing"
	"testing/quick"
)

func TestGetRecordValue(t *testing.T) {
	check := func(key string, value int) bool {
		record := &neo4j.Record{
			Values: []any{value},
			Keys:   []string{key},
		}

		validValue, noErr := neo4j.GetRecordValue[int](record, key)
		_, err1 := neo4j.GetRecordValue[int](record, key + "_nope")
		_, err2 := neo4j.GetRecordValue[string](record, key)

		return validValue == value && noErr == nil && err1 != nil && err2 != nil
	}
	if err := quick.Check(check, nil); err != nil {
		t.Error(err)
	}
}
