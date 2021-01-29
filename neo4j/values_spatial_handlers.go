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
	"math"
	"reflect"

	"github.com/neo4j-drivers/gobolt"
)

const (
	point2DSignature int16 = 'X'
	point2DSize            = 3
	point3DSignature int16 = 'Y'
	point3DSize            = 4
)

type pointValueHandler struct {
}

func (handler *pointValueHandler) ReadableStructs() []int16 {
	return []int16{point2DSignature, point3DSignature}
}

func (handler *pointValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(Point{}), reflect.TypeOf(&Point{})}
}

func (handler *pointValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	var (
		dimension int
		srId      int
		x, y, z   float64
	)

	switch signature {
	case point2DSignature:
		if len(values) != point2DSize {
			return nil, gobolt.NewValueHandlerError("expected Point2D struct to have %d fields but received %d", point2DSize, len(values))
		}

		dimension = 2
		srId = int(values[0].(int64))
		x = values[1].(float64)
		y = values[2].(float64)
		z = math.NaN()
	case point3DSignature:
		if len(values) != point3DSize {
			return nil, gobolt.NewValueHandlerError("expected Point3D struct to have %d fields but received %d", point3DSize, len(values))
		}

		dimension = 3
		srId = int(values[0].(int64))
		x = values[1].(float64)
		y = values[2].(float64)
		z = values[3].(float64)
	default:
		return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to PointValueHandler: %#x", signature)
	}

	return &Point{
		dimension: dimension,
		srId:      srId,
		x:         x,
		y:         y,
		z:         z,
	}, nil
}

func (handler *pointValueHandler) Write(value interface{}) (int16, []interface{}, error) {
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
			return point2DSignature, []interface{}{
				point.srId,
				point.x,
				point.y,
			}, nil
		case 3:
			return point3DSignature, []interface{}{
				point.srId,
				point.x,
				point.y,
				point.z,
			}, nil
		}
	}

	return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by PointValueHandler", value)
}
