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
)

type node struct {
	state int
	id    int64
	tags  []string
	props map[string]interface{}
}

func (n *node) Id() int64 {
	return n.id
}

func (n *node) Labels() []string {
	return n.tags
}

func (n *node) Props() map[string]interface{} {
	return n.props
}

func (n *node) HydrateField(field interface{}) error {
	switch n.state {
	case 0:
		id, ok := field.(int64)
		if !ok {
			return errors.New("Expected id of type int64")
		}
		n.id = id
	case 1:
		tagsx, ok := field.([]interface{})
		if !ok {
			return errors.New("Expected list of tags")
		}
		n.tags = make([]string, len(tagsx))
		for i, x := range tagsx {
			tag, ok := x.(string)
			if !ok {
				return errors.New("Tag is not string")
			}
			n.tags[i] = tag
		}
	case 2:
		props, ok := field.(map[string]interface{})
		if !ok {
			return errors.New("Expected map of props")
		}
		n.props = props
	default:
		return errors.New("Got more than expected")
	}

	n.state += 1
	return nil
}

func (n *node) HydrationComplete() error {
	if n.state != 3 {
		return errors.New("Missing fields")
	}
	return nil
}
