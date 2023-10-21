/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
	idb "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/db"
	"testing"

	"github.com/DaChartreux/neo4j-go-driver/v5/neo4j/db"
	. "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
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
		s.push(&db.Record{Values: []any{1}})
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		AssertNextOnlyRecord(t, rec, sum, err)

		// Empty stream, not buffered
		buffed, rec, sum, err = s.bufferedNext()
		assertNotBuffered(t, buffed, rec, sum, err)

		// Push record and set summary, buffered
		s.push(&db.Record{Values: []any{1}})
		s.sum = &db.Summary{}
		// Get the record
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Get the summary
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		AssertNextOnlySummary(t, rec, sum, err)
		// Get the summary again
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		AssertNextOnlySummary(t, rec, sum, err)

		// Start of with a new stream that fails
		s = &stream{}
		// Push record and set error
		s.push(&db.Record{Values: []any{1}})
		s.err = errors.New("some error")
		// Get the record
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		AssertNextOnlyRecord(t, rec, sum, err)
		// Get the error
		buffed, rec, sum, err = s.bufferedNext()
		assertBuffered(t, buffed, rec, sum, err)
		AssertNextOnlyError(t, rec, sum, err)
	})
}

func TestOpenStreams(ot *testing.T) {
	ot.Run("Attach/Detach", func(t *testing.T) {
		streams := &openstreams{}

		// Detaching empty should not crash
		streams.detach(nil, nil)

		// Attach uncompleted stream, should set current
		s := &stream{}
		streams.attach(s)
		AssertNotNil(t, streams.curr)
		AssertIntEqual(t, streams.num, 1)

		// Finishing the stream with a summary should detach it and
		// since it is the one and only stream detach should indicate everything closed.
		streams.detach(&db.Summary{}, nil)
		AssertNil(t, streams.curr)
		AssertIntEqual(t, streams.num, 0)

		// Attach and detach a stream with an error, should set error on stream
		s = &stream{}
		streams.attach(s)
		streams.detach(nil, errInvalidStream)
		AssertNil(t, streams.curr)
		AssertIntEqual(t, streams.num, 0)
		AssertNotNil(t, s.err)

		// Going into pause/resume territory
		// Add two uncompleted streams
		s = &stream{}
		s2 := &stream{}
		streams.attach(s)
		streams.pause()
		streams.attach(s2)
		AssertIntEqual(t, streams.num, 2)
		// Detaching the second stream should just close that stream
		streams.detach(&db.Summary{}, nil)
		AssertIntEqual(t, streams.num, 1)
		AssertNil(t, streams.curr)

		// Resume and detach the first stream
		streams.resume(s)
		AssertNotNil(t, streams.curr)
		AssertIntEqual(t, streams.num, 1)
		streams.detach(&db.Summary{}, nil)
		AssertNil(t, streams.curr)
		AssertIntEqual(t, streams.num, 0)
	})

	ot.Run("Pause/Resume", func(t *testing.T) {
		streams := &openstreams{}
		// Attach uncompleted stream
		s1 := &stream{}
		streams.attach(s1)

		streams.pause()
		AssertNil(t, streams.curr)

		streams.resume(s1)
		AssertNotNil(t, streams.curr)
	})

	ot.Run("Reset", func(t *testing.T) {
		streams := &openstreams{}
		s := &stream{}
		streams.attach(s)
		AssertNoError(t, streams.isSafe(s))

		// Should not call callbacks
		streams.reset()
		AssertNil(t, streams.curr)
		AssertIntEqual(t, streams.num, 0)
		// Stream should not be safe after reset
		AssertError(t, streams.isSafe(s))
	})

	ot.Run("getUnsafe/isSafe", func(t *testing.T) {
		streams := &openstreams{}
		streams.reset()

		// Should fail to retrieve even a unsafe stream from these
		fails := []idb.StreamHandle{nil, 1, 0, ""}
		for _, x := range fails {
			_, err := streams.getUnsafe(x)
			AssertError(t, err)
		}

		// Should succeed to retrieve an unsafe stream from a stream instance
		unsafe, err := streams.getUnsafe(&stream{})
		AssertNotNil(t, unsafe)
		AssertNoError(t, err)

		// The stream instance should not be safe
		AssertError(t, streams.isSafe(unsafe))

		// Attaching a stream should make it safe
		s1 := &stream{}
		streams.attach(s1)
		AssertNoError(t, streams.isSafe(s1))

		// Pausing s1 and attaching another stream should make both streams safe
		streams.pause()
		s2 := &stream{}
		streams.attach(s2)
		AssertNoError(t, streams.isSafe(s1))
		AssertNoError(t, streams.isSafe(s2))

		// Resetting the streams should make both unsafe
		streams.reset()
		AssertError(t, streams.isSafe(s1))
		AssertError(t, streams.isSafe(s2))
	})
}
