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

package neo4j_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	bm "github.com/neo4j/neo4j-go-driver/v5/neo4j/bookmarks"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
)

func TestExecuteRead(outer *testing.T) {
	outer.Parallel()

	ctx := context.Background()
	session := &fakeSession{}

	outer.Run("returns type-safe result from underlying session read execution", func(t *testing.T) {
		result, err := neo4j.ExecuteRead[int](ctx, session, func(tx neo4j.ManagedTransaction) (int, error) {
			return 42, nil
		})

		AssertNoError(t, err)
		AssertIntEqual(t, result, 42)
	})

	outer.Run("returns underlying session read execution error", func(t *testing.T) {
		result, err := neo4j.ExecuteRead[int](ctx, session, func(tx neo4j.ManagedTransaction) (int, error) {
			return -1, fmt.Errorf("nope")
		})

		AssertErrorMessageContains(t, err, "nope")
		AssertIntEqual(t, result, 0) // value is ignored - default is returned
	})
}

func TestExecuteWrite(outer *testing.T) {
	outer.Parallel()

	ctx := context.Background()
	session := &fakeSession{}

	outer.Run("returns type-safe result from underlying session write execution", func(t *testing.T) {
		result, err := neo4j.ExecuteWrite[string](ctx, session, func(tx neo4j.ManagedTransaction) (string, error) {
			return "much wow", nil
		})

		AssertNoError(t, err)
		AssertStringEqual(t, result, "much wow")
	})

	outer.Run("returns underlying session write execution error", func(t *testing.T) {
		result, err := neo4j.ExecuteWrite[*struct{}](ctx, session, func(tx neo4j.ManagedTransaction) (*struct{}, error) {
			return nil, fmt.Errorf("nope")
		})

		AssertErrorMessageContains(t, err, "nope")
		AssertNil(t, result) // value is ignored - default is returned
	})
}

type fakeSession struct {
	neo4j.SessionWithContext
}

func (f *fakeSession) LastBookmarks() bm.Bookmarks {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) lastBookmark() string {
	panic("implement me")
}

func (f *fakeSession) BeginTransaction(ctx context.Context, configurers ...func(*neo4j.TransactionConfig)) (neo4j.ExplicitTransaction, error) {
	panic("implement me")
}

func (f *fakeSession) ExecuteRead(_ context.Context, work neo4j.ManagedTransactionWork, _ ...func(*neo4j.TransactionConfig)) (any, error) {
	return work(&FakeTransaction{})
}

func (f *fakeSession) ExecuteWrite(_ context.Context, work neo4j.ManagedTransactionWork, _ ...func(*neo4j.TransactionConfig)) (any, error) {
	return work(&FakeTransaction{})
}

func (f *fakeSession) Run(context.Context, string, map[string]any, ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	panic("implement me")
}

func (f *fakeSession) Close(context.Context) error {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) legacy() neo4j.Session {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) getServerInfo(context.Context) (neo4j.ServerInfo, error) {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) verifyAuthentication(context.Context) error {
	panic("implement me")
}

type FakeTransaction struct {
	neo4j.ManagedTransaction
}

func (f *FakeTransaction) Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *FakeTransaction) legacy() neo4j.Transaction {
	panic("implement me")
}
