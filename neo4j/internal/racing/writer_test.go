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

func TestRacingWriter(outer *testing.T) {

	payload := []byte{1, 2, 3}

	outer.Run("writes fine with non-cancelling contexts", func(t *testing.T) {
		writer := &bytes.Buffer{}
		racingWriter := rio.NewRacingWriter(writer)

		n, err := racingWriter.Write(context.Background(), payload)

		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		if n != 3 {
			t.Errorf("expected %d bytes to be written, got %d", len(payload), n)
		}
		if !reflect.DeepEqual(writer.Bytes(), payload) {
			t.Errorf("expected %v bytes, got %v", payload, writer.Bytes())
		}
	})

	outer.Run("fails writing when context is already canceled", func(t *testing.T) {
		writer := &bytes.Buffer{}
		racingWriter := rio.NewRacingWriter(writer)

		n, err := racingWriter.Write(canceledContext(), payload)

		if err != context.Canceled {
			t.Errorf("expected cancelation error, got %v", err)
		}
		if n > 0 {
			t.Errorf("expected no bytes to be written, got %d", n)
		}
		if len(writer.Bytes()) > 0 {
			t.Errorf("expected empty slice, got %v", writer.Bytes())
		}
	})

	outer.Run("completes before write occurs", func(t *testing.T) {
		writer := &slowFailingWriter{sleep: 2 * time.Minute}
		racingWriter := rio.NewRacingWriter(writer)
		ctx, cancelFunc := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancelFunc()

		n, err := racingWriter.Write(ctx, payload)

		if n > 0 {
			t.Errorf("expected 0 written bytes, got %d", n)
		}
		if err != context.DeadlineExceeded {
			t.Errorf("expected deadline exceeded error, got %v", err)
		}
	})

	outer.Run("connection write times out after 1 successful write", func(t *testing.T) {
		timeout := 400 * time.Millisecond
		server, client := net.Pipe()
		defer closePipe(t, server, client)
		go func() {
			time.Sleep(timeout / 2)
			response1 := make([]byte, 2)
			n, err := client.Read(response1)
			AssertNoError(t, err)
			AssertIntEqual(t, n, 2)
			AssertDeepEquals(t, response1, []byte{0xca, 0xfe})
			time.Sleep(timeout)
			response2 := make([]byte, 2)
			_, _ = client.Read(response2)
		}()
		writer := rio.NewRacingWriter(server)
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()

		n1, err1 := writer.Write(ctx, []byte{0xca, 0xfe})
		n2, err2 := writer.Write(ctx, []byte{0xca, 0xfe})

		AssertIntEqual(t, n1, 2)
		AssertNoError(t, err1)
		AssertIntEqual(t, n2, 0)
		if err2 != context.DeadlineExceeded {
			t.Fatalf("expected underlying connection's write to time out")
		}
	})

}

func canceledContext() context.Context {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	return ctx
}

type slowFailingWriter struct {
	sleep time.Duration
}

func (hw *slowFailingWriter) Write([]byte) (int, error) {
	time.Sleep(hw.sleep)
	return 0, fmt.Errorf("not gonna write")
}
