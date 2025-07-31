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

package dbtype

// Numeric represents the supported numeric types for Vector elements.
//
// Numeric is part of the Vector preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type Numeric interface {
	~float64 | ~float32 | ~int8 | ~int16 | ~int32 | ~int64
}

// Vector represents a fixed-length array of numeric values.
// Currently serialized as a Bolt LIST<Int64|Float64>.
// Future versions will use native Bolt Vector serialization.
//
// Vector is part of the Vector preview feature
// (see README on what it means in terms of support and compatibility guarantees)
type Vector[T Numeric] []T
