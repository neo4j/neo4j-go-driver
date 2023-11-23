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

package bolt

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"net"
)

type messageQueue struct {
	in               *incoming
	out              *outgoing
	handlers         list.List // List[responseHandler]
	targetConnection net.Conn
	err              error

	onNextMessage func()
	onIoErr       func(context.Context, error)
}

func newMessageQueue(
	target net.Conn,
	in *incoming, out *outgoing,
	onNext func(),
	onIoErr func(context.Context, error),
) messageQueue {
	return messageQueue{
		in:               in,
		out:              out,
		handlers:         list.List{},
		targetConnection: target,
		onNextMessage:    onNext,
		onIoErr:          onIoErr,
	}
}

func (q *messageQueue) appendHello(hello map[string]any, helloHandler responseHandler) {
	q.out.appendHello(hello)
	q.enqueueCallback(helloHandler)
}

func (q *messageQueue) appendLogoff(logoffHandler responseHandler) {
	q.out.appendLogoff()
	q.enqueueCallback(logoffHandler)
}
func (q *messageQueue) appendLogon(token map[string]any, logonHandler responseHandler) {
	q.out.appendLogon(token)
	q.enqueueCallback(logonHandler)
}

func (q *messageQueue) appendRoute(routingContext map[string]string, bookmarks []string, extras map[string]any, handler responseHandler) {
	q.out.appendRoute(routingContext, bookmarks, extras)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendRouteV43(routingContext map[string]string, bookmarks []string, database string, handler responseHandler) {
	q.out.appendRouteToV43(routingContext, bookmarks, database)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendBegin(meta map[string]any, handler responseHandler) {
	q.out.appendBegin(meta)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendRun(cypher string, params, meta map[string]any, runHandler responseHandler) {
	q.out.appendRun(cypher, params, meta)
	q.enqueueCallback(runHandler)
}

func (q *messageQueue) appendPullN(fetchSize int, handler responseHandler) {
	q.out.appendPullN(fetchSize)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendPullNQid(fetchSize int, qid int64, handler responseHandler) {
	q.out.appendPullNQid(fetchSize, qid)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendCommit(handler responseHandler) {
	q.out.appendCommit()
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendRollback(handler responseHandler) {
	q.out.appendRollback()
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendDiscardNQid(fetchSize int, qid int64, handler responseHandler) {
	q.out.appendDiscardNQid(fetchSize, qid)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendDiscardN(fetchSize int, handler responseHandler) {
	q.out.appendDiscardN(fetchSize)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendReset(handler responseHandler) {
	q.out.appendReset()
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendGoodbye() {
	q.out.appendGoodbye()
	// no response expected here
}

func (q *messageQueue) appendTelemetry(api int, handler responseHandler) {
	q.out.appendTelemetry(api)
	q.enqueueCallback(handler)
}

func (q *messageQueue) send(ctx context.Context) {
	q.out.send(ctx, q.targetConnection)
}

func (q *messageQueue) receiveAll(ctx context.Context) error {
	for {
		if q.handlers.Len() == 0 {
			return nil
		}
		if err := q.receive(ctx); err != nil {
			return err
		}
	}
}

func (q *messageQueue) receive(ctx context.Context) error {
	res := q.receiveMsg(ctx)
	if q.err != nil {
		return q.err
	}

	if q.handlers.Len() == 0 {
		return errors.New("no more response callback to apply")
	}
	handler := q.pop()
	switch message := res.(type) {
	case *db.Record:
		onRecord := handler.onRecord
		if onRecord == nil {
			return errors.New("protocol violation: the server sent an unexpected RECORD response")
		}
		onRecord(message)
	case *success:
		onSuccess := handler.onSuccess
		if onSuccess == nil {
			return errors.New("protocol violation: the server sent an unexpected SUCCESS response")
		}
		onSuccess(message)
	case *db.Neo4jError:
		onFailure := handler.onFailure
		if onFailure == nil {
			return errors.New("protocol violation: the server sent an unexpected FAILURE response")
		}
		onFailure(ctx, message)
		return message
	case *ignored:
		onIgnored := handler.onIgnored
		if onIgnored == nil {
			return errors.New("protocol violation: the server sent an unexpected IGNORED response")
		}
		onIgnored(message)
	default:
		panic(fmt.Errorf("did not expect message %v", res))
	}
	return nil
}

func (q *messageQueue) pushFront(handler responseHandler) {
	q.handlers.PushFront(handler)
}

func (q *messageQueue) pop() responseHandler {
	return q.handlers.Remove(q.handlers.Front()).(responseHandler)
}

func (q *messageQueue) receiveMsg(ctx context.Context) any {
	// Potentially dangerous to receive when an error has occurred, could hang.
	// Important, a lot of code has been simplified relying on this check.
	if q.err != nil {
		return nil
	}

	msg, err := q.in.next(ctx, q.targetConnection)
	q.err = err
	if err != nil {
		q.onIoErr(ctx, err)
	} else {
		q.onNextMessage()
	}
	return msg
}

func (q *messageQueue) enqueueCallback(handler responseHandler) {
	q.handlers.PushBack(handler)
}

func (q *messageQueue) setLogId(logId string) {
	q.in.hyd.logId = logId
	q.out.logId = logId
}

func (q *messageQueue) setBoltLogger(logger log.BoltLogger) {
	q.in.hyd.boltLogger = logger
	q.out.boltLogger = logger
}

func (q *messageQueue) isEmpty() bool {
	return q.handlers.Len() == 0
}
