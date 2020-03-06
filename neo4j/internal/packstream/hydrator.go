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

// Hook for providing unpacker with object to unpack structs into.
type HydratorFactory interface {
	// Given a tag the corresponding Hydrator implementations should
	// be returned or an error if no such was found.
	Hydrator(tag StructTag, numFields int) (Hydrator, error)
}

type Hydrator interface {
	AddUnpackedField(field interface{}) error
}
