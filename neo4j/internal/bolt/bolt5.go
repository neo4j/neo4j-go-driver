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
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/boltagent"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/telemetry"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

const (
	bolt5Ready        = iota // Ready for use
	bolt5Streaming           // Receiving result from auto commit query
	bolt5Tx                  // Transaction pending
	bolt5StreamingTx         // Receiving result from a query within a transaction
	bolt5Failed              // Recoverable error, needs reset
	bolt5Dead                // Non recoverable protocol or connection error
	bolt5Unauthorized        // Initial state, not sent hello message with authentication
)

// Default fetch size
const bolt5FetchSize = 1000

type internalTx5 struct {
	mode               idb.AccessMode
	bookmarks          []string
	timeout            time.Duration
	txMeta             map[string]any
	databaseName       string
	impersonatedUser   string
	notificationConfig idb.NotificationConfig
}

func (i *internalTx5) toMeta(logger log.Logger, logId string) map[string]any {
	if i == nil {
		return nil
	}
	meta := map[string]any{}
	if i.mode == idb.ReadMode {
		meta["mode"] = "r"
	}
	if len(i.bookmarks) > 0 {
		meta["bookmarks"] = i.bookmarks
	}
	ms := i.timeout.Milliseconds()
	if i.timeout.Nanoseconds()%int64(time.Millisecond) > 0 {
		ms++
		logger.Infof(log.Bolt5, logId, "The transaction timeout was rounded up to the next millisecond due to a fractional millisecond value in the config.")
	}
	if ms > 0 {
		meta["tx_timeout"] = ms
	}
	if len(i.txMeta) > 0 {
		meta["tx_metadata"] = i.txMeta
	}
	if i.databaseName != idb.DefaultDatabase {
		meta["db"] = i.databaseName
	}
	if i.impersonatedUser != "" {
		meta["imp_user"] = i.impersonatedUser
	}
	i.notificationConfig.ToMeta(meta)
	return meta
}

type bolt5 struct {
	state            int
	txId             idb.TxHandle
	streams          openstreams
	conn             net.Conn
	serverName       string
	queue            messageQueue
	connId           string
	logId            string
	serverVersion    string
	bookmark         string // Last bookmark
	birthDate        time.Time
	log              log.Logger
	databaseName     string
	err              error // Last fatal error
	minor            int
	lastQid          int64 // Last seen qid
	idleDate         time.Time
	auth             map[string]any
	authManager      auth.TokenManager
	resetAuth        bool
	errorListener    ConnectionErrorListener
	telemetryEnabled bool
}

func NewBolt5(
	serverName string,
	conn net.Conn,
	errorListener ConnectionErrorListener,
	logger log.Logger,
	boltLog log.BoltLogger,
) *bolt5 {
	now := itime.Now()
	b := &bolt5{
		state:         bolt5Unauthorized,
		conn:          conn,
		serverName:    serverName,
		birthDate:     now,
		idleDate:      now,
		log:           logger,
		streams:       openstreams{},
		lastQid:       -1,
		errorListener: errorListener,
	}
	b.queue = newMessageQueue(
		conn,
		&incoming{
			buf: make([]byte, 4096),
			hyd: hydrator{
				boltLogger: boltLog,
				boltMajor:  5,
				useUtc:     true,
			},
			connReadTimeout: -1,
		},
		&outgoing{
			chunker:    newChunker(),
			packer:     packstream.Packer{},
			onPackErr:  func(err error) { b.setError(err, true) },
			onIoErr:    b.onIoError,
			boltLogger: boltLog,
			useUtc:     true,
		},
		b.onNextMessage,
		b.onIoError,
	)
	return b
}

func (b *bolt5) checkStreams() {
	if b.streams.num <= 0 {
		// Perform state transition from streaming, if in that state otherwise keep the current
		// state as we are in some kind of bad shape
		switch b.state {
		case bolt5StreamingTx:
			b.state = bolt5Tx
		case bolt5Streaming:
			b.state = bolt5Ready
		}
	}
}

func (b *bolt5) ServerName() string {
	return b.serverName
}

func (b *bolt5) ServerVersion() string {
	return b.serverVersion
}

// Sets b.err and b.state to bolt5Failed or bolt5Dead when fatal is true.
func (b *bolt5) setError(err error, fatal bool) {
	// Has no effect, can reduce nested ifs
	if err == nil {
		return
	}

	// No previous error
	if b.err == nil {
		b.err = err
		b.state = bolt5Failed
	}

	// Increase severity even if it was a previous error
	if fatal {
		if ctxErr := handleTerminatedContextError(err, b.conn); ctxErr != nil {
			b.err = ctxErr
		}
		b.state = bolt5Dead
	}

	// Forward error to current stream if there is one
	if b.streams.curr != nil {
		b.streams.detach(nil, err)
		b.checkStreams()
	}

	// Do not log big cypher statements as errors
	neo4jErr, casted := err.(*db.Neo4jError)
	if casted && neo4jErr.Classification() == "ClientError" {
		b.log.Debugf(log.Bolt5, b.logId, "%s", err)
	} else {
		b.log.Error(log.Bolt5, b.logId, err)
	}
}

func (b *bolt5) Connect(
	ctx context.Context,
	minor int,
	auth *idb.ReAuthToken,
	userAgent string,
	routingContext map[string]string,
	notificationConfig idb.NotificationConfig,
) error {
	if err := b.assertState(bolt5Unauthorized); err != nil {
		return err
	}

	b.minor = minor

	if err := checkReAuth(auth, b); err != nil {
		return err
	}
	token, err := auth.Manager.GetAuthToken(ctx)
	if err != nil {
		return err
	}
	b.auth = token.Tokens
	b.authManager = auth.Manager

	hello := map[string]any{
		"user_agent": userAgent,
	}
	if routingContext != nil {
		hello["routing"] = routingContext
	}
	// On bolt >= 5.3 add bolt agent information to hello
	if b.minor >= 3 {
		info := boltagent.New()
		hello["bolt_agent"] = map[string]string{
			"product":  info.Product(),
			"platform": info.Platform(),
			"language": info.Language(),
		}
	}
	if b.minor == 0 {
		// Merge authentication keys into hello, avoid overwriting existing keys
		for k, v := range token.Tokens {
			_, exists := hello[k]
			if !exists {
				hello[k] = v
			}
		}
	}

	if err := checkNotificationFiltering(notificationConfig, b); err != nil {
		return err
	}
	notificationConfig.ToMeta(hello)
	b.queue.appendHello(hello, b.helloResponseHandler())
	if b.minor > 0 {
		b.queue.appendLogon(token.Tokens, b.logonResponseHandler())
	}
	if b.queue.send(ctx); b.err != nil {
		return b.err
	}
	if err := b.queue.receiveAll(ctx); err != nil {
		return err
	}
	if b.err != nil { // onNextMessageErr kicked in
		return b.err
	}

	b.state = bolt5Ready
	b.streams.reset()
	b.log.Infof(log.Bolt5, b.logId, "Connected")
	return nil
}

func (b *bolt5) TxBegin(
	ctx context.Context,
	txConfig idb.TxConfig,
	syncMessages bool,
) (idb.TxHandle, error) {
	// Ok, to begin transaction while streaming auto-commit, just empty the stream and continue.
	if b.state == bolt5Streaming {
		if b.bufferStream(ctx); b.err != nil {
			return 0, b.err
		}
	}
	// Makes all outstanding streams invalid
	b.streams.reset()

	if err := b.assertState(bolt5Ready); err != nil {
		return 0, err
	}
	if err := checkNotificationFiltering(txConfig.NotificationConfig, b); err != nil {
		return 0, err
	}

	tx := internalTx5{
		mode:               txConfig.Mode,
		bookmarks:          txConfig.Bookmarks,
		timeout:            txConfig.Timeout,
		txMeta:             txConfig.Meta,
		databaseName:       b.databaseName,
		impersonatedUser:   txConfig.ImpersonatedUser,
		notificationConfig: txConfig.NotificationConfig,
	}

	b.queue.appendBegin(tx.toMeta(b.log, b.logId), b.beginResponseHandler())
	if syncMessages {
		if b.queue.send(ctx); b.err != nil {
			return 0, b.err
		}
		if err := b.queue.receiveAll(ctx); err != nil {
			return 0, err
		}
	}

	if b.err != nil { // onNextMessageErr kicked in
		return 0, b.err
	}

	b.state = bolt5Tx
	b.txId = idb.TxHandle(time.Now().Unix())
	return b.txId, nil
}

// Should NOT set b.err or change b.state as this is used to guard against
// misuse from clients that stick to their connections when they shouldn't.
func (b *bolt5) assertTxHandle(h1, h2 idb.TxHandle) error {
	if h1 != h2 {
		err := errors.New(errorutil.InvalidTransactionError)
		b.log.Error(log.Bolt5, b.logId, err)
		return err
	}
	return nil
}

// Should NOT set b.err or b.state since the connection is still valid
func (b *bolt5) assertState(allowed ...int) error {
	// Forward prior error instead, this former error is probably the
	// root cause of any state error. Like a call to Run with malformed
	// cypher causes an error and another call to Commit would cause the
	// state to be wrong. Do not log this.
	if b.err != nil {
		return b.err
	}
	for _, a := range allowed {
		if b.state == a {
			return nil
		}
	}
	err := fmt.Errorf("invalid state %d, expected: %+v", b.state, allowed)
	b.log.Error(log.Bolt5, b.logId, err)
	return err
}

func (b *bolt5) TxCommit(ctx context.Context, txh idb.TxHandle) error {
	if err := b.assertTxHandle(b.txId, txh); err != nil {
		return err
	}

	// Consume pending stream if any to turn state from streamingtx to tx
	// Access to the streams outside tx boundary is not allowed, therefore we should discard
	// the stream (not buffer).
	if b.discardAllStreams(ctx); b.err != nil {
		return b.err
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt5Tx); err != nil {
		return err
	}

	b.queue.appendCommit(b.commitResponseHandler())
	if b.queue.send(ctx); b.err != nil {
		return b.err
	}
	if err := b.queue.receiveAll(ctx); err != nil {
		return err
	}
	if b.err != nil {
		return b.err
	}

	// Transition into ready state
	b.state = bolt5Ready
	return nil
}

func (b *bolt5) TxRollback(ctx context.Context, txh idb.TxHandle) error {
	if err := b.assertTxHandle(b.txId, txh); err != nil {
		return err
	}

	// Can not send rollback while still streaming, consume to turn state into tx
	// Access to the streams outside tx boundary is not allowed, therefore we should discard
	// the stream (not buffer).
	if b.discardAllStreams(ctx); b.err != nil {
		return b.err
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt5Tx); err != nil {
		return err
	}

	b.queue.appendRollback(b.rollbackResponseHandler())
	if b.queue.send(ctx); b.err != nil {
		return b.err
	}
	if err := b.queue.receiveAll(ctx); err != nil {
		return err
	}
	if b.err != nil {
		return b.err
	}

	b.state = bolt5Ready
	return nil
}

// Discards all records in current stream if in streaming state and there is a current stream.
func (b *bolt5) discardStream(ctx context.Context) {
	if b.state != bolt5Streaming && b.state != bolt5StreamingTx {
		return
	}

	stream := b.streams.curr
	if stream == nil {
		return
	}

	stream.discarding = true // pull response handler will discard any accumulated record for this stream
	discarded := false
	for {
		if err := b.queue.receiveAll(ctx); err != nil {
			return
		}
		if b.err != nil {
			return
		}
		if stream.sum != nil || stream.err != nil {
			return
		}
		if stream.endOfBatch && discarded {
			b.streams.remove(stream)
			b.checkStreams()
			return
		}
		discarded = true
		stream.fetchSize = -1 // request infinite batch to consume the rest
		if b.state == bolt5StreamingTx && stream.qid != b.lastQid {
			b.queue.appendDiscardNQid(stream.fetchSize, stream.qid, b.discardResponseHandler(stream))
		} else {
			b.queue.appendDiscardN(stream.fetchSize, b.discardResponseHandler(stream))
		}
		if b.queue.send(ctx); b.err != nil {
			return
		}
	}
}

func (b *bolt5) discardAllStreams(ctx context.Context) {
	if b.state != bolt5Streaming && b.state != bolt5StreamingTx {
		return
	}

	// Discard current
	b.discardStream(ctx)
	b.streams.reset()
	b.checkStreams()
}

// bufferStream pulls all the records of the current stream if there is a current stream.
func (b *bolt5) bufferStream(ctx context.Context) {
	stream := b.streams.curr
	if stream == nil {
		return
	}

	for {
		if err := b.queue.receiveAll(ctx); err != nil {
			return
		}
		if b.err != nil {
			return
		}
		if stream.sum != nil || stream.err != nil {
			return
		}
		if stream.endOfBatch {
			stream.fetchSize = -1
			b.appendPullN(stream)
			if b.queue.send(ctx); b.err != nil {
				return
			}
		}
	}
}

// pauseStream pulls all the records of the current stream ongoing batch of records and unsets the stream as current
func (b *bolt5) pauseStream(ctx context.Context) {
	stream := b.streams.curr
	if stream == nil {
		return
	}

	if err := b.queue.receiveAll(ctx); err != nil {
		return
	}
	if b.err != nil {
		return
	}
	if stream.sum != nil || stream.err != nil {
		return
	}
	if stream.endOfBatch {
		b.streams.pause()
	}
}

// resumeStream marks the current stream as current and requests PULL
func (b *bolt5) resumeStream(ctx context.Context, s *stream) {
	b.streams.resume(s)
	b.appendPullN(s)
	b.queue.send(ctx)
}

func (b *bolt5) run(ctx context.Context, cypher string, params map[string]any, rawFetchSize int, tx *internalTx5) (*stream, error) {
	// If already streaming, consume the whole thing first
	if b.state == bolt5Streaming {
		if b.bufferStream(ctx); b.err != nil {
			return nil, b.err
		}
	} else if b.state == bolt5StreamingTx {
		if b.pauseStream(ctx); b.err != nil {
			return nil, b.err
		}
	}

	if err := b.assertState(bolt5Tx, bolt5Ready, bolt5StreamingTx); err != nil {
		return nil, err
	}

	fetchSize := b.normalizeFetchSize(rawFetchSize)
	stream := &stream{fetchSize: fetchSize}
	b.queue.appendRun(cypher, params, tx.toMeta(b.log, b.logId), b.runResponseHandler(stream))
	b.queue.appendPullN(fetchSize, b.pullResponseHandler(stream))
	if b.queue.send(ctx); b.err != nil {
		return nil, b.err
	}
	// only read response for RUN
	for !stream.attached {
		if err := b.queue.receive(ctx); err != nil {
			// rely on RESET to deal with unhandled PULL response
			return nil, err
		}
		if b.err != nil {
			return nil, b.err
		}
	}

	if b.state == bolt5Ready {
		b.state = bolt5Streaming
	} else if b.state == bolt5Tx {
		b.state = bolt5StreamingTx
	}
	return stream, nil
}

func (b *bolt5) normalizeFetchSize(fetchSize int) int {
	if fetchSize < 0 {
		return -1
	}
	if fetchSize == 0 {
		return bolt5FetchSize
	}
	return fetchSize
}

func (b *bolt5) Run(
	ctx context.Context,
	cmd idb.Command,
	txConfig idb.TxConfig,
) (idb.StreamHandle, error) {
	if err := b.assertState(bolt5Streaming, bolt5Ready); err != nil {
		return nil, err
	}
	if err := checkNotificationFiltering(txConfig.NotificationConfig, b); err != nil {
		return nil, err
	}

	tx := internalTx5{
		mode:               txConfig.Mode,
		bookmarks:          txConfig.Bookmarks,
		timeout:            txConfig.Timeout,
		txMeta:             txConfig.Meta,
		databaseName:       b.databaseName,
		impersonatedUser:   txConfig.ImpersonatedUser,
		notificationConfig: txConfig.NotificationConfig,
	}
	stream, err := b.run(ctx, cmd.Cypher, cmd.Params, cmd.FetchSize, &tx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (b *bolt5) RunTx(ctx context.Context, txh idb.TxHandle, cmd idb.Command) (idb.StreamHandle, error) {
	if err := b.assertTxHandle(b.txId, txh); err != nil {
		return nil, err
	}

	stream, err := b.run(ctx, cmd.Cypher, cmd.Params, cmd.FetchSize, nil)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (b *bolt5) Keys(streamHandle idb.StreamHandle) ([]string, error) {
	// Don't care about if the stream is the current or even if it belongs to this connection.
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return nil, err
	}
	return stream.keys, nil
}

// Next reads one record from the stream.
func (b *bolt5) Next(ctx context.Context, streamHandle idb.StreamHandle) (
	*db.Record, *db.Summary, error) {
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return nil, nil, err
	}

	for {
		buf, rec, sum, err := stream.bufferedNext()
		if buf {
			return rec, sum, err
		}
		if stream.endOfBatch {
			b.appendPullN(stream)
			if b.queue.send(ctx); b.err != nil {
				return nil, nil, b.err
			}
			stream.endOfBatch = false
		}
		if b.queue.isEmpty() {
			return nil, nil, errors.New("there should be more results to pull")
		}
		err = b.queue.receive(ctx)
		if err != nil {
			return nil, nil, err
		}
		if b.err != nil {
			return nil, nil, b.err
		}
	}
}

func (b *bolt5) Consume(ctx context.Context, streamHandle idb.StreamHandle) (
	*db.Summary, error) {
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return nil, err
	}

	// If the stream already is complete we don't care about whom it belongs to
	if stream.sum != nil || stream.err != nil {
		return stream.sum, stream.err
	}

	// Make sure the stream is safe (tied to this bolt instance and scope)
	if err = b.streams.isSafe(stream); err != nil {
		return nil, err
	}

	// We should be streaming otherwise it is an internal error, shouldn't be
	// a safe stream while not streaming.
	if err = b.assertState(bolt5Streaming, bolt5StreamingTx); err != nil {
		return nil, err
	}

	// If the stream isn't current, we need to pause the current one.
	if stream != b.streams.curr {
		b.pauseStream(ctx)
		if b.err != nil {
			return nil, b.err
		}
		b.resumeStream(ctx, stream)
	}

	// If the stream is current, discard everything up to next batch and discard the
	// stream on the server.
	b.discardStream(ctx)
	return stream.sum, stream.err
}

func (b *bolt5) Buffer(ctx context.Context,
	streamHandle idb.StreamHandle) error {
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return err
	}

	// If the stream already is complete we don't care about whom it belongs to
	if stream.sum != nil || stream.err != nil {
		return stream.Err()
	}

	// Make sure the stream is safe
	// Do NOT set b.err for this error
	if err = b.streams.isSafe(stream); err != nil {
		return err
	}

	// We should be streaming otherwise it is an internal error, shouldn't be
	// a safe stream while not streaming.
	if err = b.assertState(bolt5Streaming, bolt5StreamingTx); err != nil {
		return err
	}

	// If the stream isn't current, we need to pause the current one.
	if stream != b.streams.curr {
		b.pauseStream(ctx)
		if b.err != nil {
			return b.err
		}
		b.resumeStream(ctx, stream)
	}

	b.bufferStream(ctx)
	return stream.Err()
}

func (b *bolt5) Bookmark() string {
	return b.bookmark
}

func (b *bolt5) IsAlive() bool {
	return b.state != bolt5Dead
}

func (b *bolt5) HasFailed() bool {
	return b.state == bolt5Failed
}

func (b *bolt5) Birthdate() time.Time {
	return b.birthDate
}

func (b *bolt5) IdleDate() time.Time {
	return b.idleDate
}

func (b *bolt5) Reset(ctx context.Context) {
	defer func() {
		b.log.Debugf(log.Bolt5, b.logId, "Resetting connection internal state")
		b.txId = 0
		b.bookmark = ""
		b.databaseName = idb.DefaultDatabase
		b.err = nil
		b.lastQid = -1
		b.streams.reset()
	}()

	if b.state == bolt5Ready {
		// No need for reset
		return
	}

	b.ForceReset(ctx)
}

func (b *bolt5) ForceReset(ctx context.Context) {
	if b.state == bolt5Dead {
		return
	}

	// Reset any pending error, should be matching bolt5_failed, so
	// it should be recoverable.
	b.err = nil

	if err := b.queue.receiveAll(ctx); b.err != nil || err != nil {
		return
	}
	b.queue.appendReset(b.resetResponseHandler())
	if b.queue.send(ctx); b.err != nil {
		return
	}
	if err := b.queue.receive(ctx); b.err != nil || err != nil {
		return
	}
}

func (b *bolt5) GetRoutingTable(ctx context.Context,
	routingContext map[string]string, bookmarks []string, database, impersonatedUser string) (*idb.RoutingTable, error) {
	if err := b.assertState(bolt5Ready); err != nil {
		return nil, err
	}

	b.log.Infof(log.Bolt5, b.logId, "Retrieving routing table")
	extras := map[string]any{}
	if database != idb.DefaultDatabase {
		extras["db"] = database
	}
	if impersonatedUser != "" {
		extras["imp_user"] = impersonatedUser
	}

	var routingTable *idb.RoutingTable
	b.queue.appendRoute(routingContext, bookmarks, extras, b.routeResponseHandler(&routingTable))
	if b.queue.send(ctx); b.err != nil {
		return nil, b.err
	}
	if err := b.queue.receiveAll(ctx); err != nil {
		return nil, err
	}
	if b.err != nil {
		return nil, b.err
	}
	return routingTable, nil
}

func (b *bolt5) SetBoltLogger(boltLogger log.BoltLogger) {
	b.queue.setBoltLogger(boltLogger)
}

func (b *bolt5) ReAuth(ctx context.Context, auth *idb.ReAuthToken) error {
	if b.minor == 0 {
		return b.fallbackReAuth(ctx, auth)
	}
	return b.reAuth(ctx, auth)
}

func (b *bolt5) fallbackReAuth(ctx context.Context, auth *idb.ReAuthToken) error {
	if err := checkReAuth(auth, b); err != nil {
		return err
	}
	if b.resetAuth {
		b.log.Infof(log.Bolt5, b.logId, "Closing connection because auth token expired (informed by other connection)")
		b.Close(ctx)
		return nil
	}
	token, err := auth.Manager.GetAuthToken(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b.auth, token.Tokens) {
		b.log.Infof(log.Bolt5, b.logId, "Closing connection because auth token expired (informed by auth manager)")
		b.Close(ctx)
	}
	return nil
}

func (b *bolt5) reAuth(ctx context.Context, auth *idb.ReAuthToken) error {
	token, err := auth.Manager.GetAuthToken(ctx)
	if err != nil {
		return err
	}
	if b.resetAuth {
		b.log.Infof(
			log.Bolt5, b.logId,
			"Re-authenticating connection because auth token expired (informed by other connection)")
		b.queue.appendLogoff(b.logoffResponseHandler())
		b.queue.appendLogon(token.Tokens, b.logonResponseHandler())
	} else if !reflect.DeepEqual(b.auth, token.Tokens) {
		b.log.Infof(
			log.Bolt5, b.logId,
			"Re-authenticating connection because auth token expired (informed by auth manager)")
		b.queue.appendLogoff(b.logoffResponseHandler())
		b.queue.appendLogon(token.Tokens, b.logonResponseHandler())
	} else if auth.ForceReAuth {
		b.log.Infof(
			log.Bolt5, b.logId,
			"Re-authenticating connection because auth token expired (forced by verifyAuthentication)")
		b.queue.appendLogoff(b.logoffResponseHandler())
		b.queue.appendLogon(token.Tokens, b.logonResponseHandler())
	} else {
		return nil
	}

	if b.queue.send(ctx); b.err != nil {
		return b.err
	}
	b.auth = token.Tokens
	b.authManager = auth.Manager
	if auth.ForceReAuth {
		if err := b.queue.receiveAll(ctx); err != nil {
			return err
		}
		if b.err != nil {
			return b.err
		}
	}
	return nil
}

// Close closes the underlying connection.
// Beware: could be called on another thread when driver is closed.
func (b *bolt5) Close(ctx context.Context) {
	b.log.Infof(log.Bolt5, b.logId, "Close")
	if b.state != bolt5Dead {
		b.state = bolt5Dead
		b.queue.appendGoodbye()
		b.queue.send(ctx)
	}
	if err := b.conn.Close(); err != nil {
		b.log.Warnf(log.Driver, b.serverName, "could not close underlying socket")
	}
}

func (b *bolt5) SelectDatabase(database string) {
	b.databaseName = database
}

func (b *bolt5) Database() string {
	return b.databaseName
}

func (b *bolt5) Version() db.ProtocolVersion {
	return db.ProtocolVersion{
		Major: 5,
		Minor: b.minor,
	}
}

func (b *bolt5) ResetAuth() {
	b.resetAuth = true
}

func (b *bolt5) GetCurrentAuth() (auth.TokenManager, iauth.Token) {
	token := iauth.Token{Tokens: b.auth}
	return b.authManager, token
}

func (b *bolt5) Telemetry(api telemetry.API, onSuccess func()) {
	if b.telemetryEnabled && b.Version().Minor >= 4 {
		b.queue.appendTelemetry(api.AsInt(), b.telemetryResponseHandler(func(*success) {
			if onSuccess != nil {
				onSuccess()
			}
		}))
	}
}

func (b *bolt5) appendPullN(stream *stream) {
	if b.state == bolt5Streaming {
		b.queue.appendPullN(stream.fetchSize, b.pullResponseHandler(stream))
	} else if b.state == bolt5StreamingTx {
		if stream.qid == b.lastQid {
			b.queue.appendPullN(stream.fetchSize, b.pullResponseHandler(stream))
		} else {
			b.queue.appendPullNQid(stream.fetchSize, stream.qid, b.pullResponseHandler(stream))
		}
	}
}

func (b *bolt5) helloResponseHandler() responseHandler {
	return b.expectedSuccessHandler(b.onHelloSuccess)
}

func (b *bolt5) logoffResponseHandler() responseHandler {
	return b.expectedSuccessHandler(onSuccessNoOp)
}

func (b *bolt5) logonResponseHandler() responseHandler {
	return b.expectedSuccessHandler(onSuccessNoOp)
}

func (b *bolt5) routeResponseHandler(table **idb.RoutingTable) responseHandler {
	return b.expectedSuccessHandler(func(routeSuccess *success) {
		*table = routeSuccess.routingTable
	})
}

func (b *bolt5) beginResponseHandler() responseHandler {
	return b.expectedSuccessHandler(onSuccessNoOp)
}

func (b *bolt5) runResponseHandler(stream *stream) responseHandler {
	return b.expectedSuccessHandler(func(runSuccess *success) {
		stream.attached = true
		stream.keys = runSuccess.fields
		stream.qid = runSuccess.qid
		stream.tfirst = runSuccess.tfirst
		if runSuccess.qid > -1 {
			b.lastQid = runSuccess.qid
		}
		b.streams.attach(stream)
	})
}

func (b *bolt5) commitResponseHandler() responseHandler {
	return b.expectedSuccessHandler(b.onCommitSuccess)
}

func (b *bolt5) rollbackResponseHandler() responseHandler {
	return b.expectedSuccessHandler(onSuccessNoOp)
}

func (b *bolt5) discardResponseHandler(stream *stream) responseHandler {
	return responseHandler{
		onIgnored: func(*ignored) {
			stream.err = fmt.Errorf("stream interrupted while discarding results")
			b.streams.remove(stream)
			b.checkStreams()
		},
		onSuccess: func(discardSuccess *success) {
			if discardSuccess.hasMore {
				stream.endOfBatch = true
				return
			}
			summary := b.extractSummary(discardSuccess, stream)
			if len(summary.Bookmark) > 0 {
				b.bookmark = summary.Bookmark
			}
			stream.sum = summary
			b.streams.remove(stream)
			b.checkStreams()
		},
		onFailure: func(ctx context.Context, failure *db.Neo4jError) {
			stream.err = failure
			b.onFailure(ctx, failure) // Will detach the stream
		},
	}
}

func (b *bolt5) pullResponseHandler(stream *stream) responseHandler {
	return responseHandler{
		onRecord: func(record *db.Record) {
			if stream.discarding {
				stream.emptyRecords()
			} else {
				record.Keys = stream.keys
				stream.push(record)
			}
			b.queue.pushFront(b.pullResponseHandler(stream))
		},
		onIgnored: func(*ignored) {
			stream.err = fmt.Errorf("stream interrupted while pulling results")
			b.streams.remove(stream)
			b.checkStreams()
		},
		onSuccess: func(pullSuccess *success) {
			if stream.discarding {
				stream.emptyRecords()
			}
			if pullSuccess.hasMore {
				stream.endOfBatch = true
				return
			}
			summary := b.extractSummary(pullSuccess, stream)
			if len(summary.Bookmark) > 0 {
				b.bookmark = summary.Bookmark
			}
			stream.sum = summary
			b.streams.remove(stream)
			b.checkStreams()
		},
		onFailure: func(ctx context.Context, failure *db.Neo4jError) {
			stream.err = failure
			b.onFailure(ctx, failure) // Will detach the stream
		},
	}
}

func (b *bolt5) resetResponseHandler() responseHandler {
	return responseHandler{
		onSuccess: func(resetSuccess *success) {
			b.state = bolt5Ready
		},
		onFailure: func(ctx context.Context, failure *db.Neo4jError) {
			_ = b.errorListener.OnNeo4jError(ctx, b, failure)
			b.state = bolt5Dead
		},
	}
}

func (b *bolt5) telemetryResponseHandler(onSuccess func(*success)) responseHandler {
	return b.expectedSuccessHandler(onSuccess)
}

func (b *bolt5) expectedSuccessHandler(onSuccess func(*success)) responseHandler {
	return responseHandler{
		onSuccess: onSuccess,
		onFailure: b.onFailure,
		onIgnored: onIgnoredNoOp,
	}
}

func (b *bolt5) onHelloSuccess(helloSuccess *success) {
	b.connId = helloSuccess.connectionId
	b.serverVersion = helloSuccess.server

	connectionLogId := fmt.Sprintf("%s@%s", b.connId, b.serverName)
	b.logId = connectionLogId
	b.queue.setLogId(connectionLogId)
	b.initializeReadTimeoutHint(helloSuccess.configurationHints)
	b.initializeTelemetryHint(helloSuccess.configurationHints)
}

func (b *bolt5) onCommitSuccess(commitSuccess *success) {
	if len(commitSuccess.bookmark) > 0 {
		b.bookmark = commitSuccess.bookmark
	}
}

func (b *bolt5) onNextMessage() {
	b.idleDate = itime.Now()
}

func (b *bolt5) onFailure(ctx context.Context, failure *db.Neo4jError) {
	var err error
	err = failure
	if callbackErr := b.errorListener.OnNeo4jError(ctx, b, failure); callbackErr != nil {
		err = errorutil.CombineErrors(callbackErr, failure)
	}
	b.setError(err, isFatalError(failure))
}

func (b *bolt5) onIoError(ctx context.Context, err error) {
	if b.state != bolt5Failed && b.state != bolt5Dead {
		// Don't call callback when connections break after sending RESET.
		// The server chooses to close the connection on some errors.
		b.errorListener.OnIoError(ctx, b, err)
	}
	b.setError(err, true)
}

func (b *bolt5) initializeReadTimeoutHint(hints map[string]any) {
	readTimeoutHint, ok := hints[readTimeoutHintName]
	if !ok {
		return
	}
	readTimeout, ok := readTimeoutHint.(int64)
	if !ok {
		b.log.Infof(log.Bolt5, b.logId, `invalid %q value: %v, ignoring hint. Only strictly positive integer values are accepted`, readTimeoutHintName, readTimeoutHint)
		return
	}
	if readTimeout <= 0 {
		b.log.Infof(log.Bolt5, b.logId, `invalid %q integer value: %d. Only strictly positive values are accepted"`, readTimeoutHintName, readTimeout)
		return
	}
	b.queue.in.connReadTimeout = time.Duration(readTimeout) * time.Second
}

const readTelemetryHintName = "telemetry.enabled"

func (b *bolt5) initializeTelemetryHint(hints map[string]any) {
	readTelemetryHint, ok := hints[readTelemetryHintName]
	if !ok {
		return
	}
	readTelemetry, ok := readTelemetryHint.(bool)
	if !ok {
		b.log.Infof(log.Bolt5, b.logId, `invalid %q value: %v, ignoring hint. Only boolean values are accepted`, readTelemetryHintName, readTelemetryHint)
		return
	}
	b.telemetryEnabled = readTelemetry
}

func (b *bolt5) extractSummary(success *success, stream *stream) *db.Summary {
	summary := success.summary()
	summary.Agent = b.serverVersion
	summary.Major = 5
	summary.Minor = b.minor
	summary.ServerName = b.serverName
	summary.TFirst = stream.tfirst
	return summary
}
