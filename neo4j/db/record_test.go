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

package db_test

import (
	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/db"
	. "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"testing/quick"
)

func TestRecordAsMap(t *testing.T) {
	checkMap := func(key string, value string) bool {
		record := db.Record{
			Keys:   []string{key},
			Values: []any{value},
		}
		result := record.AsMap()
		return len(result) == 1 && result[key] == value
	}

	if err := quick.Check(checkMap, nil); err != nil {
		t.Error(err)
	}
}

func TestRecordAsMapReturnsCopy(t *testing.T) {
	record := db.Record{
		Keys:   []string{"is_boolean"},
		Values: []any{false},
	}
	dictionary := record.AsMap()

	dictionary["is_boolean"] = true

	AssertBoolEqual(t, record.Values[0].(bool), false)

}
