/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
// TODO: Show example with tx func
func Single(from interface{}, err error) (*Record, error) {
	if err != nil {
		return nil, err
	}

	result, ok := from.(*Result)
	if !ok {
		return nil, newDriverError("expected from to be a result but it was '%v'", from)
	}

	return result.Single()
}

// Collect loops through the result stream, collects records into a slice and returns the
// resulting slice. Any error passed in or reported while navigating the result stream is
// returned without any conversion.
// TODO: Show example with tx func
func Collect(from interface{}, err error) ([]*Record, error) {
	if err != nil {
		return nil, err
	}

	result, ok := from.(*Result)
	if !ok {
		return nil, newDriverError("expected from to be a result but it was '%v'", from)
	}

	return result.Collect()
}
