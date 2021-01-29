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
package packstream

import (
	"fmt"
	"reflect"
)

type OverflowError struct {
	msg string
}

func (e *OverflowError) Error() string {
	return e.msg
}

type UnsupportedTypeError struct {
	t reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Packing of type '%s' is not supported", e.t.String())
}

type IoError struct {
	inner error
}

func (e *IoError) Error() string {
	return fmt.Sprintf("IO error: %s", e.inner)
}

type IllegalFormatError struct {
	msg string
}

func (e *IllegalFormatError) Error() string {
	return e.msg
}
