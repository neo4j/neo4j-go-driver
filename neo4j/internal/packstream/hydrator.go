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

package packstream

// Hook for unpacking structs unknown to packstream into custom objects.
type HydratorFactory interface {
	// Given a tag the corresponding Hydrate implementations should
	// be returned or an error if no such was found.
	Hydrator(tag StructTag, numFields int) (Hydrate, error)
}

// Called by unpacker to let receiver check all fields and let the receiver return an
// hydrated instance or an error if any of the fields or number of fields are different
// expected.
type Hydrate func(fields []interface{}) (interface{}, error)
