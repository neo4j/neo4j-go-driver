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

package testutil

import (
	"context"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	iauth "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/telemetry"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

type Next struct {
	Record  *db.Record
	Summary *db.Summary
	Err     error
}

type RecordedTx struct {
	Origin    string
	Mode      idb.AccessMode
	Bookmarks []string
	Timeout   time.Duration
	Meta      map[string]any
}

type ConnFake struct {
	Name                    string
	ConnectionVersion       db.ProtocolVersion
	Alive                   bool
	Birth                   time.Time
	Table                   *idb.RoutingTable
	Err                     error
	Id                      int
	TxBeginErr              error
	TxBeginHandle           idb.TxHandle
	RunErr                  error
	RunStream               idb.StreamHandle
	RunTxErr                error
	RunTxStream             idb.StreamHandle
	Nexts                   []Next
	Bookm                   string
	TxCommitErr             error
	TxCommitHook            func()
	TxRollbackErr           error
	ConsumeSum              *db.Summary
	ConsumeErr              error
	ConsumeHook             func()
	RecordedTxs             []RecordedTx // Appended to by Run/TxBegin
	BufferErr               error
	BufferHook              func()
	DatabaseName            string
	Idle                    time.Time
	ServerVersionValue      string
	ForceResetHook          func()
	ReAuthHook              func(context.Context, *idb.ReAuthToken) error
	SsrEnabled              bool
	PinHomeDatabaseCallback func(context.Context, string)
}

func (c *ConnFake) Connect(
	context.Context,
	int,
	*idb.ReAuthToken,
	string,
	map[string]string,
	idb.NotificationConfig,
) error {
	return nil
}

func (c *ConnFake) ServerName() string {
	return c.Name
}

func (c *ConnFake) IsAlive() bool {
	return c.Alive
}

func (c *ConnFake) HasFailed() bool {
	return false
}

func (c *ConnFake) Reset(context.Context) {
}

func (c *ConnFake) ForceReset(context.Context) {
	c.ForceResetHook()
}

func (c *ConnFake) Close(ctx context.Context) {
}

func (c *ConnFake) Birthdate() time.Time {
	return c.Birth
}

func (c *ConnFake) IdleDate() time.Time {
	return c.Idle
}

func (c *ConnFake) Bookmark() string {
	return c.Bookm
}

func (c *ConnFake) ServerVersion() string {
	return c.ServerVersionValue
}

func (c *ConnFake) Buffer(context.Context, idb.StreamHandle) error {
	if c.BufferHook != nil {
		c.BufferHook()
	}
	return c.BufferErr
}

func (c *ConnFake) Consume(context.Context, idb.StreamHandle) (*db.Summary, error) {
	if c.ConsumeHook != nil {
		c.ConsumeHook()
	}
	return c.ConsumeSum, c.ConsumeErr
}

func (c *ConnFake) GetRoutingTable(_ context.Context, _ map[string]string, _ []string, database, _ string) (*idb.RoutingTable, error) {
	if c.Table != nil {
		c.Table.DatabaseName = database
	}
	return c.Table, c.Err
}

func (c *ConnFake) TxBegin(_ context.Context, txConfig idb.TxConfig, syncMessages bool) (idb.TxHandle, error) {
	c.RecordedTxs = append(c.RecordedTxs, RecordedTx{Origin: "TxBegin", Mode: txConfig.Mode, Bookmarks: txConfig.Bookmarks, Timeout: txConfig.Timeout, Meta: txConfig.Meta})
	return c.TxBeginHandle, c.TxBeginErr
}

func (c *ConnFake) TxRollback(context.Context, idb.TxHandle) error {
	return c.TxRollbackErr
}

func (c *ConnFake) TxCommit(context.Context, idb.TxHandle) error {
	if c.TxCommitHook != nil {
		c.TxCommitHook()
	}
	return c.TxCommitErr
}

func (c *ConnFake) Run(_ context.Context, _ idb.Command, txConfig idb.TxConfig) (idb.StreamHandle, error) {

	c.RecordedTxs = append(c.RecordedTxs, RecordedTx{Origin: "Run", Mode: txConfig.Mode, Bookmarks: txConfig.Bookmarks, Timeout: txConfig.Timeout, Meta: txConfig.Meta})
	return c.RunStream, c.RunErr
}

func (c *ConnFake) RunTx(context.Context, idb.TxHandle, idb.Command) (idb.StreamHandle, error) {
	return c.RunTxStream, c.RunTxErr
}

func (c *ConnFake) Keys(idb.StreamHandle) ([]string, error) {
	return nil, nil
}

func (c *ConnFake) Next(context.Context, idb.StreamHandle) (*db.Record, *db.Summary, error) {
	if len(c.Nexts) >= 1 {
		next := c.Nexts[0]
		// moves to next record only if the current record is not an error or summary
		// this emulates the stream buffering of a real connection
		if next.Err == nil && next.Summary == nil {
			c.Nexts = c.Nexts[1:]
		}
		return next.Record, next.Summary, next.Err
	}
	return nil, nil, nil
}

func (c *ConnFake) SelectDatabase(database string) {
	c.DatabaseName = database
}

func (c *ConnFake) Database() string {
	return c.DatabaseName
}

func (c *ConnFake) SetBoltLogger(log.BoltLogger) {
}

func (c *ConnFake) ReAuth(ctx context.Context, token *idb.ReAuthToken) error {
	if c.ReAuthHook != nil {
		return c.ReAuthHook(ctx, token)
	}
	return nil
}

func (c *ConnFake) Version() db.ProtocolVersion {
	return c.ConnectionVersion
}

func (c *ConnFake) ResetAuth() {
}

func (c *ConnFake) GetCurrentAuth() (auth.TokenManager, iauth.Token) {
	return nil, iauth.Token{}
}

func (c *ConnFake) Telemetry(telemetry.API, func()) {}

func (c *ConnFake) SetPinHomeDatabaseCallback(callback func(ctx context.Context, database string)) {
	c.PinHomeDatabaseCallback = callback
}

func (c *ConnFake) IsSsrEnabled() bool {
	return c.SsrEnabled
}
