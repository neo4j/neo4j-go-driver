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

package bolt

import (
	"fmt"
)

// Ignored response from server
type ignoredResponse struct {
	fields []interface{}
}

func (r *ignoredResponse) HydrateField(field interface{}) error {
	r.fields = append(r.fields, field)
	return nil
}

func (r *ignoredResponse) HydrationComplete() error {
	return nil
}

func (r *ignoredResponse) Error() string {
	return "ignored"
}

// Failure response from server
type failureResponse struct {
	fields []interface{}
}

func (r *failureResponse) HydrateField(field interface{}) error {
	r.fields = append(r.fields, field)
	return nil
}

func (r *failureResponse) HydrationComplete() error {
	return nil
}

func (r *failureResponse) Error() string {
	return fmt.Sprintf("failed: %+v", r.fields)
}

// Record response from server
type recordResponse struct {
	fields []interface{}
}

func (r *recordResponse) HydrateField(field interface{}) error {
	r.fields = append(r.fields, field)
	return nil
}

func (r *recordResponse) HydrationComplete() error {
	return nil
}
