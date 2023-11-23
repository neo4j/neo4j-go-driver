/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

package neo4j

import (
	"fmt"
	"time"
)

type RecordValue interface {
	bool | int64 | float64 | string |
		Point2D | Point3D |
		Date | LocalTime | LocalDateTime | Time | Duration | time.Time | /* OffsetTime == Time == dbtype.Time */
		[]byte | []any | map[string]any |
		Node | Relationship | Path
}

// GetRecordValue returns the value of the current provided record named by the specified key
// The value type T must adhere to neo4j.RecordValue
// If the key specified for the value does not exist, an error is returned
// If the value is not defined for the provided existing key, the returned boolean is true
// If the value type does not match the type specification, an error is returned
//
// Take this simple graph made of three nodes: `(:Person {name: "Arya"})`, `(:Person {name: ""})`, `(:Person)`
// and the query: `MATCH (p:Person) RETURN p.name AS name`.
// The following code illustrates when an error is returned vs. when the nil flag is set to true:
//
//	// the above query has been executed, `result` holds the cursor over the person nodes
//	for result.Next() {
//		record := result.Record()
//		name, isNil, err := neo4j.GetRecordValue[string](record, "name")
//		// for the person with the non-blank name, name == "Arya", isNil == false, err == nil
//		// for the person with the blank name, name == "", isNil == false, err == nil
//		// for the node without name, name == "", isNil == true, err == nil
//
//		_, _, err := neo4j.GetRecordValue[string](record, "invalid-key")
//		// this results in an error, since "invalid-key" is not part of the query result keys
//	}
func GetRecordValue[T RecordValue](record *Record, key string) (T, bool, error) {
	rawValue, found := record.Get(key)
	if !found {
		return *new(T), false, fmt.Errorf("record value %s not found", key)
	}
	if rawValue == nil {
		return *new(T), true, nil
	}
	value, ok := rawValue.(T)
	if !ok {
		zeroValue := *new(T)
		return zeroValue, false, fmt.Errorf("expected value to have type %T but found type %T", zeroValue, rawValue)
	}
	return value, false, nil
}
