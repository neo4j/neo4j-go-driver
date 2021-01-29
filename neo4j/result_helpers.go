/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// Single returns one and only one record from the result stream. Any error passed in
// or reported while navigating the result stream is returned without any conversion.
// If the result stream contains zero or more than one records error is returned.
func Single(from interface{}, err error) (Record, error) {
	var result Result
	var record Record
	var ok bool

	if err != nil {
		return nil, err
	}

	if result, ok = from.(Result); !ok {
		return nil, newDriverError("expected from to be a result but it was '%v'", from)
	}

	if result.Next() {
		record = result.Record()
	}

	if err := result.Err(); err != nil {
		return nil, err
	}

	if record == nil {
		return nil, newDriverError("result contains no records")
	}

	if result.Next() {
		return nil, newDriverError("result contains more than one record")
	}

	return record, nil
}

// Collect loops through the result stream, collects records into a slice and returns the
// resulting slice. Any error passed in or reported while navigating the result stream is
// returned without any conversion.
func Collect(from interface{}, err error) ([]Record, error) {
	var result Result
	var list []Record
	var ok bool

	if err != nil {
		return nil, err
	}

	if result, ok = from.(Result); !ok {
		return nil, newDriverError("expected from to be a result but it was '%v'", from)
	}

	for result.Next() {
		list = append(list, result.Record())
	}
	if err := result.Err(); err != nil {
		return nil, err
	}

	return list, nil
}
