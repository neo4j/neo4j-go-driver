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

package util

// DerefOr returns `*value` if value is not nil, else defaultValue
func DerefOr[T any](value *T, defaultValue T) T {
	if value == nil {
		return defaultValue
	}
	return *value
}

// Ptr returns the pointer to x (e.g., useful for an untyped literals)
func Ptr[T any](x T) *T {
	return &x
}
