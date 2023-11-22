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

type PropertyValue interface {
	bool | int64 | float64 | string |
		Point2D | Point3D |
		Date | LocalTime | LocalDateTime | Time | Duration | time.Time | /* OffsetTime == Time == dbtype.Time */
		[]byte | []any
}

// GetProperty returns the value matching the property of the given neo4j.Node or neo4j.Relationship
// The property type T must adhere to neo4j.PropertyValue
// If the property does not exist, an error is returned
// If the property type does not match the type specification, an error is returned
//
// Note: due to the current limited generics support, any property array value other than byte array is typed as []any.
func GetProperty[T PropertyValue](entity Entity, key string) (T, error) {
	rawValue, found := entity.GetProperties()[key]
	if !found {
		return *new(T), fmt.Errorf("could not find any property named %s", key)
	}
	value, ok := rawValue.(T)
	if !ok {
		zeroValue := *new(T)
		return zeroValue, fmt.Errorf("expected value to have type %T but found type %T", zeroValue, rawValue)
	}
	return value, nil
}
