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
	"bytes"
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"net"
	"reflect"
	"testing"
)

func TestMessageQueue(outer *testing.T) {
	outer.Parallel()

	outer.Run("appends", func(inner *testing.T) {
		inner.Parallel()

		writer := outgoing{}

		handlers := map[byte]responseHandler{
			// at least 1 non-nil function is needed per handler to ensure proper equality check
			// see assertEqualResponseHandlers
			msgHello:    {onSuccess: func(*success) {}},
			msgLogon:    {onFailure: func(context.Context, *db.Neo4jError) {}},
			msgRoute:    {onIgnored: func(*ignored) {}},
			msgBegin:    {onUnknown: func(any) {}},
			msgRun:      {onRecord: func(*db.Record) {}},
			msgPullN:    {onSuccess: func(*success) {}},
			msgCommit:   {onFailure: func(context.Context, *db.Neo4jError) {}},
			msgRollback: {onIgnored: func(*ignored) {}},
			msgDiscardN: {onUnknown: func(any) {}},
			msgReset:    {onRecord: func(*db.Record) {}},
			msgGoodbye:  {onSuccess: func(*success) {}},
		}
		tests := []struct {
			description         string
			action              func(*messageQueue)
			expectedMessageType byte
			skipsHandler        bool
		}{
			{
				description: "HELLO",
				action: func(queue *messageQueue) {
					queue.appendHello(nil, handlers[msgHello])
				},
				expectedMessageType: msgHello,
			},
			{
				description: "LOGON",
				action: func(queue *messageQueue) {
					queue.appendLogon(nil, handlers[msgLogon])
				},
				expectedMessageType: msgLogon,
			},
			{
				description: "ROUTE",
				action: func(queue *messageQueue) {
					queue.appendRoute(nil, nil, nil, handlers[msgRoute])
					// note: appendRouteV43 is also ROUTE, so skipping it
				},
				expectedMessageType: msgRoute,
			},
			{
				description: "BEGIN",
				action: func(queue *messageQueue) {
					queue.appendBegin(nil, handlers[msgBegin])
				},
				expectedMessageType: msgBegin,
			},
			{
				description: "RUN",
				action: func(queue *messageQueue) {
					queue.appendRun("", nil, nil, handlers[msgRun])
				},
				expectedMessageType: msgRun,
			},
			{
				description: "PULL",
				action: func(queue *messageQueue) {
					queue.appendPullN(0, handlers[msgPullN])
					// note: appendPullNQid is also PULL, so skipping it
				},
				expectedMessageType: msgPullN,
			},
			{
				description: "COMMIT",
				action: func(queue *messageQueue) {
					queue.appendCommit(handlers[msgCommit])
				},
				expectedMessageType: msgCommit,
			},
			{
				description: "ROLLBACK",
				action: func(queue *messageQueue) {
					queue.appendRollback(handlers[msgRollback])
				},
				expectedMessageType: msgRollback,
			},
			{
				description: "DISCARD",
				action: func(queue *messageQueue) {
					queue.appendDiscardN(0, handlers[msgDiscardN])
					// note: appendDiscardNQid is also DISCARD, so skipping it
				},
				expectedMessageType: msgDiscardN,
			},
			{
				description: "RESET",
				action: func(queue *messageQueue) {
					queue.appendReset(handlers[msgReset])
				},
				expectedMessageType: msgReset,
			},
			{
				description: "GOODBYE",
				action: func(queue *messageQueue) {
					queue.appendGoodbye()
				},
				expectedMessageType: msgGoodbye,
				skipsHandler:        true,
			},
		}

		queue := newMessageQueue(nil, nil, &writer, nil, nil)

		for _, test := range tests {
			inner.Run(test.description, func(t *testing.T) {
				initialLength := queue.handlers.Len()

				test.action(&queue)

				actualLength := queue.handlers.Len()
				if test.skipsHandler {
					AssertIntEqual(t, actualLength, initialLength)
				} else {
					AssertIntEqual(t, actualLength, initialLength+1)
					expectedHandler := handlers[test.expectedMessageType]
					enqueuedHandler := queue.handlers.Back().Value.(responseHandler)
					assertEqualResponseHandlers(t, expectedHandler, enqueuedHandler)
				}
				AssertTrue(t, bytes.Contains(writer.chunker.buf, []byte{test.expectedMessageType}))
			})
		}

	})

	outer.Run("queues handlers", func(inner *testing.T) {
		ctx := context.Background()
		client, server := net.Pipe()

		reader := incoming{
			buf: make([]byte, 4096),
			hyd: hydrator{
				boltMajor: 5,
				useUtc:    true,
			},
			connReadTimeout: -1,
		}
		writer := &outgoing{}

		queue := newMessageQueue(client, &reader, nil, func() {}, nil)

		inner.Run("receives single message, executes the first handler", func(t *testing.T) {
			defer func() {
				queue.handlers.Init()
			}()
			done := make(chan any)
			called := make(chan bool)
			queue.enqueueCallback(responseHandler{onSuccess: func(s *success) { called <- true }})
			queue.enqueueCallback(responseHandler{onSuccess: func(s *success) { t.Errorf("don't call me") }})

			go func() {
				AssertTrue(t, <-called)
				done <- struct{}{}
			}()
			go func() {
				AssertNoError(t, queue.receive(ctx))
			}()

			writer.appendX(msgSuccess, map[string]any{})
			writer.send(ctx, server)
			<-done
		})

		inner.Run("receives all messages, executes all handlers", func(t *testing.T) {
			done := make(chan any)
			called := make(chan bool)
			queue.enqueueCallback(responseHandler{onSuccess: func(s *success) { called <- true }})
			queue.enqueueCallback(responseHandler{onIgnored: func(*ignored) { called <- true }})
			queue.enqueueCallback(responseHandler{onRecord: func(*db.Record) { called <- true }})

			go func() {
				AssertTrue(t, <-called)
				AssertTrue(t, <-called)
				AssertTrue(t, <-called)
				done <- struct{}{}
			}()
			go func() {
				AssertNoError(t, queue.receiveAll(ctx))
			}()

			writer.appendX(msgSuccess, map[string]any{})
			writer.send(ctx, server)
			writer.appendX(msgIgnored)
			writer.send(ctx, server)
			writer.appendX(msgRecord, []any{})
			writer.send(ctx, server)
			<-done
		})

		inner.Run("returns error when nil callback called for", func(inner *testing.T) {
			inner.Parallel()

			type testCase struct {
				description      string
				writerWork       func(*outgoing)
				expectedErrorMsg string
			}

			testCases := []testCase{
				{
					description: "RECORD",
					writerWork: func(o *outgoing) {
						writer.appendX(msgRecord, []any{})
						writer.send(ctx, server)
					},
					expectedErrorMsg: "this response handler does not support RECORD",
				},
				{
					description: "SUCCESS",
					writerWork: func(o *outgoing) {
						writer.appendX(msgSuccess, map[string]any{})
						writer.send(ctx, server)
					},
					expectedErrorMsg: "this response handler does not support SUCCESS",
				},
				{
					description: "FAILURE",
					writerWork: func(o *outgoing) {
						writer.appendX(msgFailure, map[string]any{})
						writer.send(ctx, server)
					},
					expectedErrorMsg: "this response handler does not support FAILURE",
				},
				{
					description: "IGNORED",
					writerWork: func(o *outgoing) {
						writer.appendX(msgIgnored)
						writer.send(ctx, server)
					},
					expectedErrorMsg: "this response handler does not support IGNORED",
				},
			}

			for _, test := range testCases {
				inner.Run(fmt.Sprintf("%s response", test.description), func(t *testing.T) {
					done := make(chan any)
					queue.enqueueCallback(responseHandler{})

					go func() {
						err := queue.receive(ctx)

						AssertErrorMessageContains(inner, err, test.expectedErrorMsg)
						done <- struct{}{}
					}()

					test.writerWork(writer)
					<-done
				})
			}
		})
	})

}

func assertEqualResponseHandlers(t *testing.T, handler1, handler2 responseHandler) {
	t.Helper()
	// reflect.DeepEqual does not support comparison of non-nil functions (the same function is not equal to itself)
	// reflect.DeepEqual only considers equal a nil function with another nil function
	// reflect.DeepEqual therefore considers an empty responseHandler (all nil functions) to equal to any other empty responseHandler
	if !functionEqual(handler1.onSuccess, handler2.onSuccess) {
		t.Errorf("expected onSuccess callbacks to be equal")
	}
	if !functionEqual(handler1.onIgnored, handler2.onIgnored) {
		t.Errorf("expected onIgnored callbacks to be equal")
	}
	if !functionEqual(handler1.onRecord, handler2.onRecord) {
		t.Errorf("expected onRecord callbacks to be equal")
	}
	if !functionEqual(handler1.onFailure, handler2.onFailure) {
		t.Errorf("expected onFailure callbacks to be equal")
	}
	if !functionEqual(handler1.onUnknown, handler2.onUnknown) {
		t.Errorf("expected onUnknown callbacks to be equal")
	}
}

func functionEqual(func1, func2 any) bool {
	return reflect.ValueOf(func1).Pointer() == reflect.ValueOf(func2).Pointer()
}
