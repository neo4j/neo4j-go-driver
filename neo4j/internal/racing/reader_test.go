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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package racing_test

import (
	"bytes"
	"context"
	"fmt"
	rio "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestRacingReader(outer *testing.T) {

	type readFn func(context.Context, []byte) (int, error)

	type testCase struct {
		qualifier     string
		readOperation func(rio.RacingReader) readFn
	}

	testCases := []testCase{
		{qualifier: "Read", readOperation: func(reader rio.RacingReader) readFn { return reader.Read }},
		{qualifier: "ReadFull", readOperation: func(reader rio.RacingReader) readFn { return reader.ReadFull }},
	}

	for _, testCase := range testCases {
		outer.Run(fmt.Sprintf(`[%s] reads fine with non-cancelling contexts`, testCase.qualifier), func(t *testing.T) {
			source := []byte{1, 2, 3}
			racingReader := rio.NewRacingReader(bytes.NewBuffer(source))
			result := make([]byte, 2)

			n, err := testCase.readOperation(racingReader)(context.Background(), result)

			if err != nil {
				t.Errorf("expected nil error, got %v", err)
			}
			expectedN := len(result)
			if n != expectedN {
				t.Errorf("expected %d bytes to be written, got %d", expectedN, n)
			}
			if !reflect.DeepEqual(result, source[:expectedN]) {
				t.Errorf("expected %v bytes, got %v", source, result)
			}
		})

		outer.Run(fmt.Sprintf(`[%s] fails reading when context is already canceled`, testCase.qualifier), func(t *testing.T) {
			reader := &bytes.Buffer{}
			racingReader := rio.NewRacingReader(reader)
			result := make([]byte, 2)

			n, err := testCase.readOperation(racingReader)(canceledContext(), result)

			if err != context.Canceled {
				t.Errorf("expected cancelation error, got %v", err)
			}
			if n > 0 {
				t.Errorf("expected no bytes to be written, got %d", n)
			}
			if len(reader.Bytes()) > 0 {
				t.Errorf("expected empty slice, got %v", reader.Bytes())
			}
		})

		outer.Run(fmt.Sprintf(`[%s] completes before read occurs`, testCase.qualifier), func(t *testing.T) {
			reader := &slowFailingReader{sleep: 50 * time.Millisecond}
			racingReader := rio.NewRacingReader(reader)
			result := make([]byte, 2)
			ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancelFunc()

			n, err := testCase.readOperation(racingReader)(ctx, result)

			if n > 0 {
				t.Errorf("expected 0 written bytes, got %d", n)
			}
			if err != context.DeadlineExceeded {
				t.Errorf("expected deadline exceeded error, got %v", err)
			}
		})

		outer.Run("connection read times out after 1 successful read", func(t *testing.T) {
			timeout := 100 * time.Millisecond
			server, client := net.Pipe()
			defer closePipe(t, server, client)
			go func() {
				time.Sleep(timeout / 2)
				AssertWriteSucceeds(t, server, []byte{0xca, 0xfe})
				time.Sleep(timeout * 2)
				_, _ = server.Write([]byte{0xba, 0xba})
			}()
			reader := rio.NewRacingReader(client)
			ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
			defer cancelFunc()

			response1 := make([]byte, 2)
			n1, err1 := reader.Read(ctx, response1)
			response2 := make([]byte, 2)
			n2, err2 := reader.Read(ctx, response2)

			AssertIntEqual(t, n1, 2)
			AssertNoError(t, err1)
			AssertIntEqual(t, n2, 0)
			if err2 != context.DeadlineExceeded {
				t.Fatalf("expected underlying connection's read to time out")
			}
		})

	}

}

type slowFailingReader struct {
	sleep time.Duration
}

func (hw *slowFailingReader) Read([]byte) (int, error) {
	time.Sleep(hw.sleep)
	return 0, fmt.Errorf("not gonna read")
}

func closePipe(t *testing.T, srv, cli net.Conn) {
	AssertNoError(t, srv.Close())
	AssertNoError(t, cli.Close())
}
