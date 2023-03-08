package bolt

import (
	"container/list"
	"context"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"net"
)

type messageQueue struct {
	in               incoming
	out              outgoing
	callbacks        list.List // List[responseHandler]
	targetConnection net.Conn
	err              error

	onNextMessage    func()
	onNextMessageErr func(error)
}

func newMessageQueue(
	target net.Conn,
	in incoming, out outgoing,
	onNext func(),
	onNextErr func(error)) messageQueue {

	return messageQueue{
		in:               in,
		out:              out,
		callbacks:        list.List{},
		targetConnection: target,
		onNextMessage:    onNext,
		onNextMessageErr: onNextErr,
	}
}

func (q *messageQueue) appendHello(hello map[string]any, helloHandler responseHandler) {
	q.out.appendHello(hello)
	q.enqueueCallback(helloHandler)
}

func (q *messageQueue) appendLogon(token map[string]any, logonHandler responseHandler) {
	q.out.appendLogon(token)
	q.enqueueCallback(logonHandler)
}

func (q *messageQueue) appendRoute(routingContext map[string]string, bookmarks []string, extras map[string]any, handler responseHandler) {
	q.out.appendRoute(routingContext, bookmarks, extras)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendBegin(meta map[string]any, handler responseHandler) {
	q.out.appendBegin(meta)
	q.enqueueCallback(handler)
}

func (q *messageQueue) appendRun(cypher string, params, meta map[string]any, fetchSize int,
	runHandler responseHandler, pullHandler responseHandler) {

	q.out.appendRun(cypher, params, meta)
	q.enqueueCallback(runHandler)
	q.appendPullN(fetchSize, pullHandler)
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

func (q *messageQueue) send(ctx context.Context) {
	q.out.send(ctx, q.targetConnection)
}

func (q *messageQueue) receiveAll(ctx context.Context) error {
	for {
		if q.callbacks.Len() == 0 {
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

	if q.callbacks.Len() == 0 {
		return errors.New("no more response callback to apply")
	}
	callback := q.pop()
	switch message := res.(type) {
	case *db.Record:
		callback.onRecord(message)
	case *success:
		callback.onSuccess(message)
	case *db.Neo4jError:
		callback.onFailure(message)
		return message
	case *ignored:
		callback.onIgnored(message)
	default:
		callback.onUnknown(message)
	}
	return nil
}

func (q *messageQueue) pushFront(handler responseHandler) {
	q.callbacks.PushFront(handler)
}

func (q *messageQueue) pop() responseHandler {
	return q.callbacks.Remove(q.callbacks.Front()).(responseHandler)
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
		q.onNextMessageErr(err)
	} else {
		q.onNextMessage()
	}
	return msg
}

func (q *messageQueue) enqueueCallback(callbacks responseHandler) {
	q.callbacks.PushBack(callbacks)
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
	return q.callbacks.Len() == 0
}
