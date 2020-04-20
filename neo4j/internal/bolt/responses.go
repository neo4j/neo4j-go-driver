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
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/db"
)

// Server ignored request.
type ignoredResponse struct {
}

func (r *ignoredResponse) Error() string {
	return "ignored"
}

// Record response from server.
type recordResponse struct {
	values []interface{}
}

// Success response from server, success contains meta data that looks different depending
// on what request the response is for.
type successResponse struct {
	meta map[string]interface{}
}

// Extracted from SuccessResponse.meta for a RUN request.
type runSuccess struct {
	fields  []string
	t_first int64
}

func (s *successResponse) run() *runSuccess {
	fieldsx, fok := s.meta["fields"].([]interface{})
	t_first, tok := s.meta["t_first"].(int64)
	if !fok || !tok {
		return nil
	}
	fields := make([]string, len(fieldsx))
	for i, x := range fieldsx {
		s, ok := x.(string)
		if !ok {
			return nil
		}
		fields[i] = s
	}
	return &runSuccess{fields: fields, t_first: t_first}
}

// Extracted from SuccessResponse.meta for a HELLO request.
type helloSuccess struct {
	connectionId       string
	credentialsExpired bool
	server             string
}

func (s *successResponse) hello() *helloSuccess {
	id, iok := s.meta["connection_id"].(string)
	server, sok := s.meta["server"].(string)
	if !iok || !sok {
		return nil
	}
	exp, _ := s.meta["credentials_expired"].(bool)
	return &helloSuccess{connectionId: id, server: server, credentialsExpired: exp}
}

// Extracted from SuccessResponse.meta on end of stream.
// Maps directly to shared internal summary type to avoid unnecessary conversions.
func (s *successResponse) summary() *db.Summary {
	t_last, _ := s.meta["t_last"].(int64)
	qtype, tok := s.meta["type"].(string)
	bookmark, _ := s.meta["bookmark"].(string) // Optional ?
	if !tok {
		return nil
	}

	// Map statement type received to internal type
	stmntType := db.StatementTypeUnknown
	switch qtype {
	case "r":
		stmntType = db.StatementTypeRead
	case "w":
		stmntType = db.StatementTypeWrite
	case "rw":
		stmntType = db.StatementTypeReadWrite
	case "s":
		stmntType = db.StatementTypeSchemaWrite
	}

	// Optional statistics
	var counts map[string]int
	statsx, _ := s.meta["stats"].(map[string]interface{})
	if len(statsx) > 0 {
		// Convert from ugly interface{} to ints
		counts = make(map[string]int, len(statsx))
		for k, v := range statsx {
			c, _ := v.(int64)
			if c > 0 {
				counts[k] = int(c)
			}
		}
	}

	// TODO: Query plan

	return &db.Summary{
		Bookmark:  bookmark,
		TLast:     t_last,
		StmntType: stmntType,
		Counters:  counts,
	}
}
