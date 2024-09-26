/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
	"context"
	"errors"
	rio "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"time"
)

func TestRacingSleep(outer *testing.T) {
	outer.Parallel()

	outer.Run("sleeps for the full duration", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		start := time.Now()
		err := rio.Sleep(ctx, 100*time.Millisecond)
		end := time.Now()

		AssertNoError(t, err)
		AssertTrue(t, end.Sub(start) >= 100*time.Millisecond)
	})

	outer.Run("reacts to context cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		start := time.Now()
		cancel()
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		err := rio.Sleep(ctx, 1000*time.Millisecond)
		end := time.Now()

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got %+v", err)
		}
		AssertTrue(t, end.Sub(start) < 1000*time.Millisecond)
	})

	outer.Run("reacts to context deadline", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := rio.Sleep(ctx, 1000*time.Millisecond)
		end := time.Now()

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context deadline exceeded error, got %+v", err)
		}
		AssertTrue(t, end.Sub(start) < 1000*time.Millisecond)

	})
}
