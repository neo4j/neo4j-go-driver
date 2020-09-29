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
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

func TestStream(ot *testing.T) {
	assertNotBuffered := func(t *testing.T, buf bool, rec *db.Record, sum *db.Summary, err error) {
		if buf {
			t.Error("Expected not buffered")
		}
		if rec != nil || sum != nil || err != nil {
			t.Error("Expected record, summary and error to be nil")
		}
	}

	assertBuffered := func(t *testing.T, buf bool, rec *db.Record, sum *db.Summary, err error) {
		if !buf {
			t.Error("Expected buffered")
		}
	}

	ot.Run("Buffering", func(t *testing.T) {
		s := &stream{}

		// Empty stream, not buffered
		buffed, rec, sum, err := s.bufferedNext()
		assertNotBuffered(t, buffed, rec, sum, err)

		// Push record, buffered, record received
		s.push(&db.Record{Values: []interface{}{1}})
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		assertOnlyRecord(t, rec, sum, err)

		// Empty stream, not buffered
		buffed, rec, sum, err = s.bufferedNext()
		assertNotBuffered(t, buffed, rec, sum, err)

		// Push record and set summary, buffered
		s.push(&db.Record{Values: []interface{}{1}})
		s.sum = &db.Summary{}
		// Get the record
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		assertOnlyRecord(t, rec, sum, err)
		// Get the summary
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		assertOnlySummary(t, rec, sum, err)
		// Get the summary again
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		assertOnlySummary(t, rec, sum, err)

		// Start of with a new stream that fails
		s = &stream{}
		// Push record and set error
		s.push(&db.Record{Values: []interface{}{1}})
		s.err = errors.New("some error")
		// Get the record
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		assertOnlyRecord(t, rec, sum, err)
		// Get the error
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		assertOnlyError(t, rec, sum, err)
	})
}
