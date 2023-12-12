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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collections"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/telemetry"
	itime "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/time"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

const (
	bolt4_ready        = iota // Ready for use
	bolt4_streaming           // Receiving result from auto commit query
	bolt4_tx                  // Transaction pending
	bolt4_streamingtx         // Receiving result from a query within a transaction
	bolt4_failed              // Recoverable error, needs reset
	bolt4_dead                // Non recoverable protocol or connection error
	bolt4_unauthorized        // Initial state, not sent hello message with authentication
)

// Default fetch size
const bolt4_fetchsize = 1000

type internalTx4 struct {
	mode             idb.AccessMode
	bookmarks        []string
	timeout          time.Duration
	txMeta           map[string]any
	databaseName     string
	impersonatedUser string
}

func (i *internalTx4) toMeta(logger log.Logger, logId string) map[string]any {
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
	return meta
}

type bolt4 struct {
	state         int
	txId          idb.TxHandle
	streams       openstreams
	conn          net.Conn
	serverName    string
	connId        string
	logId         string
	serverVersion string
	bookmark      string // Last bookmark
	birthDate     time.Time
	log           log.Logger
	databaseName  string
	err           error // Last fatal error
	minor         int
	lastQid       int64 // Last seen qid
	idleDate      time.Time
	queue         messageQueue
	auth          map[string]any
	authManager   auth.TokenManager
	resetAuth     bool
	errorListener ConnectionErrorListener
}

func NewBolt4(
	serverName string,
	conn net.Conn,
	errorListener ConnectionErrorListener,
	logger log.Logger,
	boltLog log.BoltLogger,
) *bolt4 {
	now := itime.Now()
	b := &bolt4{
		state:         bolt4_unauthorized,
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
				boltMajor:  4,
			},
			connReadTimeout: -1,
		},
		&outgoing{
			chunker:    newChunker(),
			packer:     packstream.Packer{},
			onPackErr:  func(err error) { b.setError(err, true) },
			onIoErr:    b.onIoError,
			boltLogger: boltLog,
		},
		b.onNextMessage,
		b.onIoError,
	)

	return b
}

func (b *bolt4) checkStreams() {
	if b.streams.num <= 0 {
		// Perform state transition from streaming, if in that state otherwise keep the current
		// state as we are in some kind of bad shape
		switch b.state {
		case bolt4_streamingtx:
			b.state = bolt4_tx
		case bolt4_streaming:
			b.state = bolt4_ready
		}
	}
}

func (b *bolt4) ServerName() string {
	return b.serverName
}

func (b *bolt4) ServerVersion() string {
	return b.serverVersion
}

// Sets b.err and b.state to bolt4_failed or bolt4_dead when fatal is true.
func (b *bolt4) setError(err error, fatal bool) {
	// Has no effect, can reduce nested ifs
	if err == nil {
		return
	}

	// No previous error
	if b.err == nil {
		b.err = err
		b.state = bolt4_failed
	}

	// Increase severity even if it was a previous error
	if fatal {
		if ctxErr := handleTerminatedContextError(err, b.conn); ctxErr != nil {
			b.err = ctxErr
		}
		b.state = bolt4_dead
	}

	// Forward error to current stream if there is one
	if b.streams.curr != nil {
		b.streams.detach(nil, err)
		b.checkStreams()
	}

	// Do not log big cypher statements as errors
	neo4jErr, casted := err.(*db.Neo4jError)
	if casted && neo4jErr.Classification() == "ClientError" {
		b.log.Debugf(log.Bolt4, b.logId, "%s", err)
	} else {
		b.log.Error(log.Bolt4, b.logId, err)
	}
}

func (b *bolt4) Connect(
	ctx context.Context,
	minor int,
	auth *idb.ReAuthToken,
	userAgent string,
	routingContext map[string]string,
	notificationConfig idb.NotificationConfig,
) error {
	if err := b.assertState(bolt4_unauthorized); err != nil {
		return err
	}

	b.minor = minor

	if err := checkReAuth(auth, b); err != nil {
		return err
	}

	// Prepare hello message
	hello := map[string]any{
		"user_agent": userAgent,
	}
	// On bolt >= 4.1 add routing to enable/disable routing
	if b.minor >= 1 {
		if routingContext != nil {
			hello["routing"] = routingContext
		}
	}
	checkUtcPatch := b.minor >= 3
	if checkUtcPatch {
		hello["patch_bolt"] = []string{"utc"}
	}
	// Merge authentication keys into hello, avoid overwriting existing keys
	token, err := auth.Manager.GetAuthToken(ctx)
	if err != nil {
		return err
	}
	b.auth = token.Tokens
	b.authManager = auth.Manager
	for k, v := range token.Tokens {
		_, exists := hello[k]
		if !exists {
			hello[k] = v
		}
	}

	if err := checkNotificationFiltering(notificationConfig, b); err != nil {
		return err
	}

	b.queue.appendHello(hello, b.helloResponseHandler(checkUtcPatch))
	if b.queue.send(ctx); b.err != nil {
		return b.err
	}
	if err := b.queue.receive(ctx); err != nil {
		return err
	}
	if b.err != nil { // onNextMessageErr kicked in
		return b.err
	}

	// Transition into ready state
	b.state = bolt4_ready
	b.streams.reset()
	b.log.Infof(log.Bolt4, b.logId, "Connected")
	return nil
}

func (b *bolt4) checkImpersonationAndVersion(impersonatedUser string) error {
	if impersonatedUser != "" && b.minor < 4 {
		return &db.FeatureNotSupportedError{Server: b.serverName, Feature: "user impersonation", Reason: "requires at least server v4.4"}
	}
	return nil
}

func (b *bolt4) TxBegin(
	ctx context.Context,
	txConfig idb.TxConfig,
	syncMessages bool,
) (idb.TxHandle, error) {
	// Ok, to begin transaction while streaming auto-commit, just empty the stream and continue.
	if b.state == bolt4_streaming {
		if b.bufferStream(ctx); b.err != nil {
			return 0, b.err
		}
	}
	// Makes all outstanding streams invalid
	b.streams.reset()

	if err := b.assertState(bolt4_ready); err != nil {
		return 0, err
	}
	if err := b.checkImpersonationAndVersion(txConfig.ImpersonatedUser); err != nil {
		return 0, err
	}
	if err := checkNotificationFiltering(txConfig.NotificationConfig, b); err != nil {
		return 0, err
	}

	tx := internalTx4{
		mode:             txConfig.Mode,
		bookmarks:        txConfig.Bookmarks,
		timeout:          txConfig.Timeout,
		txMeta:           txConfig.Meta,
		databaseName:     b.databaseName,
		impersonatedUser: txConfig.ImpersonatedUser,
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

	b.state = bolt4_tx
	b.txId = idb.TxHandle(time.Now().Unix())
	return b.txId, nil
}

// Should NOT set b.err or change b.state as this is used to guard against
// misuse from clients that stick to their connections when they shouldn't.
func (b *bolt4) assertTxHandle(h1, h2 idb.TxHandle) error {
	if h1 != h2 {
		err := errors.New(errorutil.InvalidTransactionError)
		b.log.Error(log.Bolt4, b.logId, err)
		return err
	}
	return nil
}

// Should NOT set b.err or b.state since the connection is still valid
func (b *bolt4) assertState(allowed ...int) error {
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
	b.log.Error(log.Bolt4, b.logId, err)
	return err
}

func (b *bolt4) TxCommit(ctx context.Context, txh idb.TxHandle) error {
	if err := b.assertTxHandle(b.txId, txh); err != nil {
		return err
	}

	// Consume pending stream if any to turn state from streamingtx to tx
	// Access to streams outside tx boundary is not allowed, therefore we should discard
	// the stream (not buffer).
	if b.discardAllStreams(ctx); b.err != nil {
		return b.err
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt4_tx); err != nil {
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
	b.state = bolt4_ready
	return nil
}

func (b *bolt4) TxRollback(ctx context.Context, txh idb.TxHandle) error {
	if err := b.assertTxHandle(b.txId, txh); err != nil {
		return err
	}

	// Can not send rollback while still streaming, consume to turn state into tx
	// Access to streams outside tx boundary is not allowed, therefore we should discard
	// the stream (not buffer).
	if b.discardAllStreams(ctx); b.err != nil {
		return b.err
	}

	// Should be in vanilla tx state now
	if err := b.assertState(bolt4_tx); err != nil {
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

	b.state = bolt4_ready
	return nil
}

// Discards all records in current stream if in streaming state and there is a current stream.
func (b *bolt4) discardStream(ctx context.Context) {
	if b.state != bolt4_streaming && b.state != bolt4_streamingtx {
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
		if b.state == bolt4_streamingtx && stream.qid != b.lastQid {
			b.queue.appendDiscardNQid(stream.fetchSize, stream.qid, b.discardResponseHandler(stream))
		} else {
			b.queue.appendDiscardN(stream.fetchSize, b.discardResponseHandler(stream))
		}
		if b.queue.send(ctx); b.err != nil {
			return
		}
	}
}

func (b *bolt4) discardAllStreams(ctx context.Context) {
	if b.state != bolt4_streaming && b.state != bolt4_streamingtx {
		return
	}

	// Discard current
	b.discardStream(ctx)
	b.streams.reset()
	b.checkStreams()
}

func (b *bolt4) appendPullN(stream *stream) {
	if b.state == bolt4_streaming {
		b.queue.appendPullN(stream.fetchSize, b.pullResponseHandler(stream))
	} else if b.state == bolt4_streamingtx {
		if stream.qid == b.lastQid {
			b.queue.appendPullN(stream.fetchSize, b.pullResponseHandler(stream))
		} else {
			b.queue.appendPullNQid(stream.fetchSize, stream.qid, b.pullResponseHandler(stream))
		}
	}
}

// Collects all records in current stream if in streaming state and there is a current stream.
func (b *bolt4) bufferStream(ctx context.Context) {
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

// Prepares the current stream for being switched out by collecting all records in the current
// stream up until the next batch. Assumes that we are in a streaming state.
func (b *bolt4) pauseStream(ctx context.Context) {
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

func (b *bolt4) resumeStream(ctx context.Context, s *stream) {
	b.streams.resume(s)
	b.appendPullN(s)
	b.queue.send(ctx)
}

func (b *bolt4) run(ctx context.Context, cypher string, params map[string]any, rawFetchSize int, tx *internalTx4) (*stream, error) {
	// If already streaming, consume the whole thing first
	if b.state == bolt4_streaming {
		if b.bufferStream(ctx); b.err != nil {
			return nil, b.err
		}
	} else if b.state == bolt4_streamingtx {
		if b.pauseStream(ctx); b.err != nil {
			return nil, b.err
		}
	}

	if err := b.assertState(bolt4_tx, bolt4_ready, bolt4_streamingtx); err != nil {
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
	if err := b.queue.receive(ctx); err != nil {
		// rely on RESET to deal with unhandled PULL response
		return nil, err
	}
	if b.err != nil {
		return nil, b.err
	}

	// Change state to streaming
	if b.state == bolt4_ready {
		b.state = bolt4_streaming
	} else {
		b.state = bolt4_streamingtx
	}

	return stream, nil
}

func (b *bolt4) normalizeFetchSize(fetchSize int) int {
	if fetchSize < 0 {
		return -1
	}
	if fetchSize == 0 {
		return bolt4_fetchsize
	}
	return fetchSize
}

func (b *bolt4) Run(
	ctx context.Context,
	cmd idb.Command,
	txConfig idb.TxConfig,
) (idb.StreamHandle, error) {
	if err := b.assertState(bolt4_streaming, bolt4_ready); err != nil {
		return nil, err
	}
	if err := b.checkImpersonationAndVersion(txConfig.ImpersonatedUser); err != nil {
		return 0, err
	}
	if err := checkNotificationFiltering(txConfig.NotificationConfig, b); err != nil {
		return nil, err
	}

	tx := internalTx4{
		mode:             txConfig.Mode,
		bookmarks:        txConfig.Bookmarks,
		timeout:          txConfig.Timeout,
		txMeta:           txConfig.Meta,
		databaseName:     b.databaseName,
		impersonatedUser: txConfig.ImpersonatedUser,
	}
	stream, err := b.run(ctx, cmd.Cypher, cmd.Params, cmd.FetchSize, &tx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (b *bolt4) RunTx(ctx context.Context, txh idb.TxHandle,
	cmd idb.Command) (idb.StreamHandle, error) {
	if err := b.assertTxHandle(b.txId, txh); err != nil {
		return nil, err
	}

	stream, err := b.run(ctx, cmd.Cypher, cmd.Params, cmd.FetchSize, nil)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (b *bolt4) Keys(streamHandle idb.StreamHandle) ([]string, error) {
	// Don't care about if the stream is the current or even if it belongs to this connection.
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return nil, err
	}
	return stream.keys, nil
}

// Next reads one record from the stream.
func (b *bolt4) Next(ctx context.Context, streamHandle idb.StreamHandle) (
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

func (b *bolt4) Consume(ctx context.Context, streamHandle idb.StreamHandle) (
	*db.Summary, error) {
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return nil, err
	}

	// If the stream already is complete we don't care about who it belongs to
	if stream.sum != nil || stream.err != nil {
		return stream.sum, stream.err
	}

	// Make sure the stream is safe (tied to this bolt instance and scope)
	if err = b.streams.isSafe(stream); err != nil {
		return nil, err
	}

	// We should be streaming otherwise it is an internal error, shouldn't be
	// a safe stream while not streaming.
	if err = b.assertState(bolt4_streaming, bolt4_streamingtx); err != nil {
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

func (b *bolt4) Buffer(ctx context.Context,
	streamHandle idb.StreamHandle) error {
	// Do NOT set b.err for this error
	stream, err := b.streams.getUnsafe(streamHandle)
	if err != nil {
		return err
	}

	// If the stream already is complete we don't care about who it belongs to
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
	if err = b.assertState(bolt4_streaming, bolt4_streamingtx); err != nil {
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

func (b *bolt4) Bookmark() string {
	return b.bookmark
}

func (b *bolt4) IsAlive() bool {
	return b.state != bolt4_dead
}

func (b *bolt4) HasFailed() bool {
	return b.state == bolt4_failed
}

func (b *bolt4) Birthdate() time.Time {
	return b.birthDate
}

func (b *bolt4) IdleDate() time.Time {
	return b.idleDate
}

func (b *bolt4) Reset(ctx context.Context) {
	defer func() {
		b.log.Debugf(log.Bolt4, b.logId, "Resetting connection internal state")
		b.txId = 0
		b.bookmark = ""
		b.databaseName = idb.DefaultDatabase
		b.err = nil
		b.lastQid = -1
		b.streams.reset()
	}()

	if b.state == bolt4_ready {
		// No need for reset
		return
	}
	b.ForceReset(ctx)
}

func (b *bolt4) ForceReset(ctx context.Context) {
	if b.state == bolt4_dead {
		return
	}
	// Reset any pending error, should be matching bolt4_failed, so
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

func (b *bolt4) GetRoutingTable(ctx context.Context,
	routingContext map[string]string, bookmarks []string, database, impersonatedUser string) (*idb.RoutingTable, error) {
	if err := b.assertState(bolt4_ready); err != nil {
		return nil, err
	}

	b.log.Infof(log.Bolt4, b.logId, "Retrieving routing table")
	if b.minor > 3 {
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

	if err := b.checkImpersonationAndVersion(impersonatedUser); err != nil {
		return nil, err
	}

	if b.minor > 2 {
		var routingTable *idb.RoutingTable
		b.queue.appendRouteV43(routingContext, bookmarks, database, b.routeResponseHandler(&routingTable))
		if b.queue.send(ctx); b.err != nil {
			return nil, b.err
		}
		if err := b.queue.receiveAll(ctx); err != nil {
			return nil, err
		}
		if b.err != nil {
			return nil, b.err
		}
		routingTable.DatabaseName = database
		return routingTable, nil
	}
	return b.callGetRoutingTable(ctx, routingContext, bookmarks, database)
}

func (b *bolt4) callGetRoutingTable(ctx context.Context,
	routingContext map[string]string, bookmarks []string, database string) (*idb.RoutingTable, error) {
	// The query should run in system database, preserve current setting and restore it when
	// done.
	originalDatabaseName := b.databaseName
	b.databaseName = "system"
	defer func() { b.databaseName = originalDatabaseName }()

	// Query for the users default database or a specific database
	runCommand := idb.Command{
		Cypher:    "CALL dbms.routing.getRoutingTable($context)",
		Params:    map[string]any{"context": routingContext},
		FetchSize: -1,
	}
	if database != idb.DefaultDatabase {
		runCommand.Cypher = "CALL dbms.routing.getRoutingTable($context, $database)"
		runCommand.Params["database"] = database
	}
	txConfig := idb.TxConfig{Mode: idb.ReadMode, Bookmarks: bookmarks, Timeout: idb.DefaultTxConfigTimeout}
	streamHandle, err := b.Run(ctx, runCommand, txConfig)
	if err != nil {
		return nil, err
	}
	rec, _, err := b.Next(ctx, streamHandle)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, errors.New("no routing table record")
	}
	// Just empty the stream, ignore the summary should leave the connection in ready state
	_, _, _ = b.Next(ctx, streamHandle)

	table := parseRoutingTableRecord(rec)
	if table == nil {
		return nil, errors.New("unable to parse routing table")
	}
	// On this version we will not receive the database name
	table.DatabaseName = database
	return table, nil
}

// Close closes the underlying connection.
// Beware: could be called on another thread when driver is closed.
func (b *bolt4) Close(ctx context.Context) {
	b.log.Infof(log.Bolt4, b.logId, "Close")
	if b.state != bolt4_dead {
		b.queue.appendGoodbye()
		b.queue.send(ctx)
	}
	if err := b.conn.Close(); err != nil {
		b.log.Warnf(log.Driver, b.serverName, "could not close underlying socket")
	}
	b.state = bolt4_dead
}

func (b *bolt4) SelectDatabase(database string) {
	b.databaseName = database
}

func (b *bolt4) Database() string {
	return b.databaseName
}

func (b *bolt4) SetBoltLogger(boltLogger log.BoltLogger) {
	b.queue.setBoltLogger(boltLogger)
}

func (b *bolt4) ReAuth(ctx context.Context, auth *idb.ReAuthToken) error {
	if err := checkReAuth(auth, b); err != nil {
		return err
	}
	if b.resetAuth {
		b.log.Infof(log.Bolt4, b.logId, "Closing connection because auth token expired (informed by other connection)")
		b.Close(ctx)
		return nil
	}
	token, err := auth.Manager.GetAuthToken(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b.auth, token.Tokens) {
		b.log.Infof(log.Bolt4, b.logId, "Closing connection because auth token expired (informed by auth manager)")
		b.Close(ctx)
	}
	return nil
}

func (b *bolt4) Version() db.ProtocolVersion {
	return db.ProtocolVersion{
		Major: 4,
		Minor: b.minor,
	}
}

func (b *bolt4) ResetAuth() {
	b.resetAuth = true
}

func (b *bolt4) GetCurrentAuth() (auth.TokenManager, iauth.Token) {
	token := iauth.Token{Tokens: b.auth}
	return b.authManager, token
}

func (b *bolt4) Telemetry(telemetry.API, func()) {
	// TELEMETRY not support by this protocol version, so we ignore it.
}

func (b *bolt4) helloResponseHandler(checkUtcPatch bool) responseHandler {
	return b.expectedSuccessHandler(b.onHelloSuccess(checkUtcPatch))
}

func (b *bolt4) beginResponseHandler() responseHandler {
	return b.expectedSuccessHandler(onSuccessNoOp)
}

func (b *bolt4) commitResponseHandler() responseHandler {
	return b.expectedSuccessHandler(b.onCommitSuccess)
}

func (b *bolt4) rollbackResponseHandler() responseHandler {
	return b.expectedSuccessHandler(onSuccessNoOp)
}

func (b *bolt4) discardResponseHandler(stream *stream) responseHandler {
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

func (b *bolt4) pullResponseHandler(stream *stream) responseHandler {
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
			b.onFailure(ctx, failure) // will detach the stream
		},
	}

}

func (b *bolt4) runResponseHandler(stream *stream) responseHandler {
	return b.expectedSuccessHandler(func(runSuccess *success) {
		stream.keys = runSuccess.fields
		stream.qid = runSuccess.qid
		stream.tfirst = runSuccess.tfirst
		if runSuccess.qid > -1 {
			b.lastQid = runSuccess.qid
		}
		b.streams.attach(stream)
	})
}

func (b *bolt4) resetResponseHandler() responseHandler {
	return responseHandler{
		onSuccess: func(resetSuccess *success) {
			b.state = bolt4_ready
		},
		onFailure: func(ctx context.Context, failure *db.Neo4jError) {
			_ = b.errorListener.OnNeo4jError(ctx, b, failure)
			b.state = bolt4_dead
		},
	}
}

func (b *bolt4) routeResponseHandler(table **idb.RoutingTable) responseHandler {
	return b.expectedSuccessHandler(func(routeSuccess *success) {
		*table = routeSuccess.routingTable
	})
}

func (b *bolt4) onHelloSuccess(checkUtcPatch bool) func(*success) {
	return func(helloSuccess *success) {
		if checkUtcPatch {
			useUtc := collections.SliceContains(helloSuccess.patches, "utc")
			b.queue.in.hyd.useUtc = useUtc
			b.queue.out.useUtc = useUtc
		}
		b.connId = helloSuccess.connectionId
		b.serverVersion = helloSuccess.server

		connectionLogId := fmt.Sprintf("%s@%s", b.connId, b.serverName)
		b.logId = connectionLogId
		b.queue.setLogId(connectionLogId)
		b.initializeReadTimeoutHint(helloSuccess.configurationHints)
	}
}

func (b *bolt4) onCommitSuccess(commitSuccess *success) {
	if len(commitSuccess.bookmark) > 0 {
		b.bookmark = commitSuccess.bookmark
	}
}

func (b *bolt4) expectedSuccessHandler(onSuccess func(*success)) responseHandler {
	return responseHandler{
		onSuccess: onSuccess,
		onFailure: b.onFailure,
		onIgnored: onIgnoredNoOp,
	}
}

func (b *bolt4) onNextMessage() {
	b.idleDate = itime.Now()
}

func (b *bolt4) onFailure(ctx context.Context, failure *db.Neo4jError) {
	var err error
	err = failure
	if callbackErr := b.errorListener.OnNeo4jError(ctx, b, failure); callbackErr != nil {
		err = errorutil.CombineErrors(failure, callbackErr)
	}
	b.setError(err, isFatalError(failure))
}

func (b *bolt4) onIoError(ctx context.Context, err error) {
	if b.state != bolt4_failed && b.state != bolt4_dead {
		// Don't call callback when connections break after sending RESET.
		// The server chooses to close the connection on some errors.
		b.errorListener.OnIoError(ctx, b, err)
	}
	b.setError(err, true)
}

const readTimeoutHintName = "connection.recv_timeout_seconds"

func (b *bolt4) initializeReadTimeoutHint(hints map[string]any) {
	readTimeoutHint, ok := hints[readTimeoutHintName]
	if !ok {
		return
	}
	readTimeout, ok := readTimeoutHint.(int64)
	if !ok {
		b.log.Warnf(log.Bolt4, b.logId, `invalid %q value: %v, ignoring hint. Only strictly positive integer values are accepted`, readTimeoutHintName, readTimeoutHint)
		return
	}
	if readTimeout <= 0 {
		b.log.Warnf(log.Bolt4, b.logId, `invalid %q integer value: %d. Only strictly positive values are accepted"`, readTimeoutHintName, readTimeout)
		return
	}
	b.log.Infof(log.Bolt4, b.logId, `received "connection.recv_timeout_seconds" hint value of %d second(s)`, readTimeout)
	b.queue.in.connReadTimeout = time.Duration(readTimeout) * time.Second
}

func (b *bolt4) extractSummary(success *success, stream *stream) *db.Summary {
	summary := success.summary()
	summary.Agent = b.serverVersion
	summary.Major = 4
	summary.Minor = b.minor
	summary.ServerName = b.serverName
	summary.TFirst = stream.tfirst
	return summary
}

func isFatalError(err *db.Neo4jError) bool {
	// Treat expired auth as fatal so that pool is cleaned up of old connections
	return err != nil && err.Code == "Status.Security.AuthorizationExpired"
}
