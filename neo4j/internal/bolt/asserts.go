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
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/neo4j/connection"
)

func assertHandle(log func(error), id int64, h connection.Handle) error {
	hid, ok := h.(int64)
	if !ok || hid != id {
		err := errors.New("Invalid handle")
		log(err)
		return err
	}
	return nil
}

func invalidStateError(state int, expected []int) error {
	return errors.New(fmt.Sprintf("Invalid state %d, expected: %+v", state, expected))
}

func assertStates(log func(error), state int, allowed []int) error {
	for _, a := range allowed {
		if state == a {
			return nil
		}
	}
	err := invalidStateError(state, allowed)
	log(err)
	return err
}

func assertState(log func(error), state, allowed int) error {
	if state != allowed {
		err := invalidStateError(state, []int{allowed})
		log(err)
		return err
	}
	return nil
}
