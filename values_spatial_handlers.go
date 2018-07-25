/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"fmt"
	"math"
	"reflect"

	"github.com/neo4j-drivers/neo4j-go-connector"
)

type pointValueHandler struct {
}

func (handler *pointValueHandler) ReadableStructs() []int8 {
	return []int8{'X', 'Y'}
}

func (handler *pointValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(Point{}), reflect.TypeOf(&Point{})}
}

func (handler *pointValueHandler) Read(signature int8, values []interface{}) (interface{}, error) {
	dimension := 2
	srId := 0
	x := math.NaN()
	y := math.NaN()
	z := math.NaN()

	switch signature {
	case 'X':
		if len(values) != 3 {
			return nil, seabolt.NewValueHandlerError(fmt.Sprintf("expected Point2D struct to have %d fields but received %d", 3, len(values)))
		}

		dimension = 2
		srId = int(values[0].(int64))
		x = values[1].(float64)
		y = values[2].(float64)
	case 'Y':
		if len(values) != 4 {
			return nil, seabolt.NewValueHandlerError(fmt.Sprintf("expected Point3D struct to have %d fields but received %d", 4, len(values)))
		}

		dimension = 2
		srId = int(values[0].(int64))
		x = values[1].(float64)
		y = values[2].(float64)
		z = values[3].(float64)
	default:
		return nil, seabolt.NewValueHandlerError(fmt.Sprintf("unexpected struct signature provided to PointValueHandler: %#x", signature))
	}

	return &Point{
		dimension: dimension,
		srId:      srId,
		x:         x,
		y:         y,
		z:         z,
	}, nil
}

func (handler *pointValueHandler) Write(value interface{}) (int8, []interface{}, error) {
	var point *Point
	var ok bool

	if point, ok = value.(*Point); !ok {
		if pointVal, ok := value.(Point); ok {
			point = &pointVal
		}
	}

	if point != nil {
		switch point.dimension {
		case 2:
			return 'X', []interface{}{
				point.srId,
				point.x,
				point.y,
			}, nil
		case 3:
			return 'Y', []interface{}{
				point.srId,
				point.x,
				point.y,
				point.z,
			}, nil
		}
	}

	return 0, nil, seabolt.NewValueHandlerError(fmt.Sprintf("passed in value %v is not supported by PointValueHandler", value))
}
