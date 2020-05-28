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
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

// Called by packstream unpacker to hydrate a packstream struct into something
// more usable by the consumer.
func hydrate(tag packstream.StructTag, fields []interface{}) (interface{}, error) {
	switch tag {
	case msgSuccess:
		return hydrateSuccess(fields)
	case msgIgnored:
		return hydrateIgnored(fields)
	case msgFailure:
		return hydrateFailure(fields)
	case msgRecord:
		return hydrateRecord(fields)
	case 'N':
		return hydrateNode(fields)
	case 'R':
		return hydrateRelationship(fields)
	case 'r':
		return hydrateRelNode(fields)
	case 'P':
		return hydratePath(fields)
	case 'X':
		return hydratePoint2d(fields)
	case 'Y':
		return hydratePoint3d(fields)
	case 'F':
		return hydrateDateTimeOffset(fields)
	case 'f':
		return hydrateDateTimeNamedZone(fields)
	case 'd':
		return hydrateLocalDateTime(fields)
	case 'D':
		return hydrateDate(fields)
	case 'T':
		return hydrateTime(fields)
	case 't':
		return hydrateLocalTime(fields)
	case 'E':
		return hydrateDuration(fields)
	default:
		return nil, errors.New(fmt.Sprintf("Unknown tag: %02x", tag))
	}
}

func hydrateNode(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Node hydrate error")
	}
	id, idok := fields[0].(int64)
	tagsx, tagsok := fields[1].([]interface{})
	props, propsok := fields[2].(map[string]interface{})
	if !idok || !tagsok || !propsok {
		return nil, errors.New("Node hydrate error")
	}
	n := &types.Node{Id: id, Props: props, Labels: make([]string, len(tagsx))}
	// Convert tags to strings
	for i, x := range tagsx {
		t, tok := x.(string)
		if !tok {
			return nil, errors.New("Node hydrate error")
		}
		n.Labels[i] = t
	}
	return n, nil
}

func hydrateRelationship(fields []interface{}) (interface{}, error) {
	if len(fields) != 5 {
		return nil, errors.New("Relationship hydrate error")
	}
	id, idok := fields[0].(int64)
	sid, sidok := fields[1].(int64)
	eid, eidok := fields[2].(int64)
	lbl, lblok := fields[3].(string)
	props, propsok := fields[4].(map[string]interface{})
	if !idok || !sidok || !eidok || !lblok || !propsok {
		return nil, errors.New("Relationship hydrate error")
	}
	return &types.Relationship{Id: id, StartId: sid, EndId: eid, Type: lbl, Props: props}, nil
}

func hydrateRelNode(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("RelNode hydrate error")
	}
	id, idok := fields[0].(int64)
	lbl, lblok := fields[1].(string)
	props, propsok := fields[2].(map[string]interface{})
	if !idok || !lblok || !propsok {
		return nil, errors.New("RelNode hydrate error")
	}
	s := &types.RelNode{Id: id, Type: lbl, Props: props}
	return s, nil
}

func hydratePath(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Path hydrate error")
	}
	nodesx, nok := fields[0].([]interface{})
	relnodesx, rok := fields[1].([]interface{})
	indsx, iok := fields[2].([]interface{})
	if !nok || !rok || !iok {
		return nil, errors.New("Path hydrate error")
	}

	nodes := make([]*types.Node, len(nodesx))
	for i, nx := range nodesx {
		n, ok := nx.(*types.Node)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		nodes[i] = n
	}

	relnodes := make([]*types.RelNode, len(relnodesx))
	for i, rx := range relnodesx {
		r, ok := rx.(*types.RelNode)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		relnodes[i] = r
	}

	indexes := make([]int, len(indsx))
	for i, ix := range indsx {
		p, ok := ix.(int64)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		indexes[i] = int(p)
	}
	// Must be even number
	if (len(indexes) & 0x01) == 1 {
		return nil, errors.New("Path hydrate error")
	}

	p := &types.Path{Nodes: nodes, RelNodes: relnodes, Indexes: indexes}
	return p, nil
}

func hydrateSuccess(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Success hydrate error")
	}
	meta, metaok := fields[0].(map[string]interface{})
	if !metaok {
		return nil, errors.New("Success hydrate error")
	}
	return &successResponse{meta: meta}, nil
}

func hydrateRecord(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Record hydrate error")
	}
	v, vok := fields[0].([]interface{})
	if !vok {
		return nil, errors.New("Record hydrate error")
	}
	return &recordResponse{values: v}, nil
}

func hydrateIgnored(fields []interface{}) (interface{}, error) {
	if len(fields) != 0 {
		return nil, errors.New("Ignored hydrate error")
	}
	return &ignoredResponse{}, nil
}

func hydrateFailure(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Failure hydrate error")
	}
	m, mok := fields[0].(map[string]interface{})
	if !mok {
		return nil, errors.New("Failure hydrate error")
	}
	code, cok := m["code"].(string)
	msg, mok := m["message"].(string)
	if !cok || !mok {
		return nil, errors.New("Failure hydrate error")
	}
	// Hydrate right into error defined in db package to avoid remapping at a later
	// state.
	return &db.DatabaseError{Code: code, Msg: msg}, nil
}

func hydratePoint2d(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Point2d hydrate error")
	}
	srId, sok := fields[0].(int64)
	x, xok := fields[1].(float64)
	y, yok := fields[2].(float64)
	if !sok || !xok || !yok {
		return nil, errors.New("Point2d hydrate error")
	}
	return &types.Point2D{SpatialRefId: uint32(srId), X: x, Y: y}, nil
}

func hydratePoint3d(fields []interface{}) (interface{}, error) {
	if len(fields) != 4 {
		return nil, errors.New("Point3d hydrate error")
	}
	srId, sok := fields[0].(int64)
	x, xok := fields[1].(float64)
	y, yok := fields[2].(float64)
	z, zok := fields[3].(float64)
	if !sok || !xok || !yok || !zok {
		return nil, errors.New("Point3d hydrate error")
	}
	return &types.Point3D{SpatialRefId: uint32(srId), X: x, Y: y, Z: z}, nil
}

func hydrateDateTimeOffset(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("DateTime hydrate error")
	}
	secs, sok := fields[0].(int64)
	nans, nok := fields[1].(int64)
	offs, ook := fields[2].(int64)
	if !sok || !nok || !ook {
		return nil, errors.New("DateTime hydrate error")
	}

	t := time.Unix(secs, nans).UTC()
	l := time.FixedZone("Offset", int(offs))
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l), nil
}

func hydrateDateTimeNamedZone(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("DateTime hydrate error")
	}
	secs, sok := fields[0].(int64)
	nans, nok := fields[1].(int64)
	zone, zok := fields[2].(string)
	if !sok || !nok || !zok {
		return nil, errors.New("DateTime hydrate error")
	}

	t := time.Unix(secs, nans).UTC()
	l, err := time.LoadLocation(zone)
	if err != nil {
		// TODO: Wrap error
		return nil, err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l), nil
}

func hydrateLocalDateTime(fields []interface{}) (interface{}, error) {
	if len(fields) != 2 {
		return nil, errors.New("LocalDateTime hydrate error")
	}
	secs, sok := fields[0].(int64)
	nans, nok := fields[1].(int64)
	if !sok || !nok {
		return nil, errors.New("LocalDateTime hydrate error")
	}
	t := time.Unix(secs, nans).UTC()
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	return types.LocalDateTime(t), nil
}

func hydrateDate(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Date hydrate error")
	}
	days, dok := fields[0].(int64)
	if !dok {
		return nil, errors.New("Date hydrate error")
	}
	secs := days * 86400
	return types.Date(time.Unix(secs, 1)), nil
}

func hydrateTime(fields []interface{}) (interface{}, error) {
	if len(fields) != 2 {
		return nil, errors.New("Time hydrate error")
	}
	nans, nok := fields[0].(int64)
	offs, ook := fields[1].(int64)
	if !nok || !ook {
		return nil, errors.New("Time hydrate error")
	}
	secs := nans / int64(time.Second)
	nans -= secs * int64(time.Second)
	l := time.FixedZone("Offset", int(offs))
	t := time.Date(0, 0, 0, 0, 0, int(secs), int(nans), l)
	return types.Time(t), nil
}

func hydrateLocalTime(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("LocalTime hydrate error")
	}
	nans, nok := fields[0].(int64)
	if !nok {
		return nil, errors.New("LocalTime hydrate error")
	}
	secs := nans / int64(time.Second)
	nans -= secs * int64(time.Second)
	t := time.Date(0, 0, 0, 0, 0, int(secs), int(nans), time.Local)
	return types.LocalTime(t), nil
}

func hydrateDuration(fields []interface{}) (interface{}, error) {
	// Always hydrate to types.Duration since that allows for longer durations than the
	// standard time.Duration even though it's probably very unusual with the need to
	// express durations for hundreds of years.
	if len(fields) != 4 {
		return nil, errors.New("Duration hydrate error")
	}
	mon, mok := fields[0].(int64)
	day, dok := fields[1].(int64)
	sec, sok := fields[2].(int64)
	nan, nok := fields[3].(int64)
	if !mok || !dok || !sok || !nok {
		return nil, errors.New("Duration hydrate error")
	}

	return types.Duration{Months: mon, Days: day, Seconds: sec, Nanos: int(nan)}, nil
}
