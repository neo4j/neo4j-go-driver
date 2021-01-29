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
	"reflect"

	"github.com/neo4j-drivers/gobolt"
)

type nodeValueHandler struct {
}

type relationshipValueHandler struct {
}

type pathValueHandler struct {
}

func (handler *nodeValueHandler) ReadableStructs() []int16 {
	return []int16{'N'}
}

func (handler *nodeValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type(nil)
}

func (handler *nodeValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	if len(values) != 3 {
		return nil, gobolt.NewValueHandlerError("expected node struct to have %d fields but received %d", 3, len(values))
	}

	idValue := values[0].(int64)
	labelsValue := values[1].([]interface{})
	props := values[2].(map[string]interface{})

	labelsSize := len(labelsValue)
	labels := make([]string, labelsSize)
	for i := 0; i < labelsSize; i++ {
		labels[i] = labelsValue[i].(string)
	}

	return &nodeValue{
		id:     idValue,
		labels: labels,
		props:  props,
	}, nil
}

func (handler *nodeValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	return 0, nil, gobolt.NewValueHandlerError("Write is not supported for node values")
}

func (handler *relationshipValueHandler) ReadableStructs() []int16 {
	return []int16{'R', 'r'}
}

func (handler *relationshipValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type(nil)
}

func (handler *relationshipValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	if signature == 'R' {
		if len(values) != 5 {
			return nil, gobolt.NewValueHandlerError("expected relationship struct to have %d fields but received %d", 5, len(values))
		}

		idValue := values[0].(int64)
		startIDValue := values[1].(int64)
		endIDValue := values[2].(int64)
		relTypeValue := values[3].(string)
		propsValue := values[4].(map[string]interface{})

		return &relationshipValue{
			id:      idValue,
			startId: startIDValue,
			endId:   endIDValue,
			relType: relTypeValue,
			props:   propsValue,
		}, nil
	}

	if len(values) != 3 {
		return nil, gobolt.NewValueHandlerError("expected unbound relationship struct to have %d fields but received %d", 3, len(values))
	}

	idValue := values[0].(int64)
	relTypeValue := values[1].(string)
	propsValue := values[2].(map[string]interface{})

	return &relationshipValue{
		id:      idValue,
		startId: int64(-1),
		endId:   int64(-1),
		relType: relTypeValue,
		props:   propsValue,
	}, nil
}

func (handler *relationshipValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	return 0, nil, gobolt.NewValueHandlerError("Write is not supported for relationship values")
}

func (handler *pathValueHandler) ReadableStructs() []int16 {
	return []int16{'P'}
}

func (handler *pathValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type(nil)
}

func (handler *pathValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	if len(values) != 3 {
		return nil, gobolt.NewValueHandlerError("expected path struct to have %d fields but received %d", 3, len(values))
	}

	uniqueNodesValue := values[0].([]interface{})
	uniqueRelsValue := values[1].([]interface{})
	segmentsValue := values[2].([]interface{})

	uniqueNodesSize := len(uniqueNodesValue)
	uniqueNodes := make([]Node, uniqueNodesSize)
	for i := 0; i < uniqueNodesSize; i++ {
		uniqueNodes[i] = uniqueNodesValue[i].(Node)
	}

	uniqueRelsSize := len(uniqueRelsValue)
	uniqueRels := make([]Relationship, uniqueRelsSize)
	for i := 0; i < uniqueRelsSize; i++ {
		uniqueRels[i] = uniqueRelsValue[i].(Relationship)
	}

	segmentsSize := len(segmentsValue) / 2
	segments := make([]segment, segmentsSize)
	nodes := make([]Node, segmentsSize+1)
	rels := make([]Relationship, segmentsSize)

	prevNode := uniqueNodes[0]
	nodes[0] = prevNode
	for i := 0; i < segmentsSize; i++ {
		relID := segmentsValue[2*i].(int64)
		nextNodeIndex := segmentsValue[2*i+1].(int64)
		nextNode := uniqueNodes[nextNodeIndex]

		var rel *relationshipValue
		if relID < 0 {
			rel = uniqueRels[(-relID)-1].(*relationshipValue)
			rel.startId = prevNode.Id()
			rel.endId = nextNode.Id()
		} else {
			rel = uniqueRels[relID-1].(*relationshipValue)
			rel.startId = prevNode.Id()
			rel.endId = nextNode.Id()
		}

		nodes[i+1] = nextNode
		rels[i] = rel
		segments[i] = &segmentValue{start: prevNode.(*nodeValue), relationship: rel, end: nextNode.(*nodeValue)}
		prevNode = nextNode
	}

	return &pathValue{segments: segments, nodes: nodes, relationships: rels}, nil
}

func (handler *pathValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	return 0, nil, gobolt.NewValueHandlerError("Write is not supported for path values")
}
