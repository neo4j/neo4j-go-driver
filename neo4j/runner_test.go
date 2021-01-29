/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"fmt"
	"testing"
	"time"

	"github.com/neo4j-drivers/gobolt"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestRunner(t *testing.T) {
	createMocksWithParams := func(t *testing.T, mode AccessMode, autoClose bool) (*gomock.Controller, *MockConnection, *statementRunner) {
		ctrl := gomock.NewController(t)
		connection := NewMockConnection(ctrl)
		connector := MockedConnector(connection)
		driver := newGoboltWithConnector("bolt://localhost", connector)
		runner := newRunner(driver, mode, autoClose)

		connection.EXPECT().RemoteAddress().Return("localhost:7687", nil).AnyTimes()
		connection.EXPECT().Server().Return("neo4j/3.5.0", nil).AnyTimes()

		return ctrl, connection, runner
	}

	createMocks := func(t *testing.T) (*gomock.Controller, *MockConnection, *statementRunner) {
		return createMocksWithParams(t, AccessModeWrite, true)
	}

	t.Run("shouldStartWithNilConnection", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		assert.Nil(t, runner.connection)
	})

	t.Run("shouldStartWithEmptyBookmark", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		bookmark := runner.lastBookmark

		assert.Empty(t, bookmark)
	})

	t.Run("shouldFailWhenConnectionIsNilOnAssertConnection", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		err := runner.assertConnection()

		assert.EqualError(t, err, "unexpected state: expected an active connection bound to this runner")
	})

	t.Run("shouldNotFailWhenConnectionIsNotNilOnAssertConnection", func(t *testing.T) {
		ctrl, connection, runner := createMocks(t)
		defer ctrl.Finish()
		runner.connection = connection

		assert.NoError(t, runner.assertConnection())
	})

	t.Run("shouldFailWhenConnectionIsNotNilOnAssertNoConnection", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		assert.NoError(t, runner.assertNoConnection())
	})

	t.Run("shouldNotFailWhenConnectionIsNotNilOnAssertNoConnection", func(t *testing.T) {
		ctrl, connection, runner := createMocks(t)
		defer ctrl.Finish()

		runner.connection = connection

		err := runner.assertNoConnection()

		assert.EqualError(t, err, "unexpected state: expected no connection bound to this runner")
	})

	t.Run("shouldConstructConnectionOnEnsureConnection", func(t *testing.T) {
		ctrl, connection, runner := createMocks(t)
		defer ctrl.Finish()

		err := runner.ensureConnection()

		assert.NoError(t, err)
		assert.Equal(t, runner.connection, connection)
	})

	t.Run("shouldPropagateErrorOnEnsureConnection", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()

		connector := NewMockConnector(mockCtrl)
		driver := newGoboltWithConnector("bolt://localhost", connector)
		runner := newRunner(driver, AccessModeWrite, true)

		failure := fmt.Errorf("an unexpected error")

		connector.EXPECT().Acquire(gobolt.AccessModeWrite).Return(nil, failure)

		err := runner.ensureConnection()

		assert.Equal(t, err, failure)
	})

	t.Run("shouldNotFailWhenConnectionIsNilOnClose", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		assert.NoError(t, runner.close())
	})

	t.Run("shouldQueryLastBookmarkAndCloseConnectionOnClose", func(t *testing.T) {
		ctrl, connection, runner := createMocks(t)
		defer ctrl.Finish()

		_ = runner.ensureConnection()

		gomock.InOrder(
			connection.EXPECT().LastBookmark().Return("a bookmark", nil),
			connection.EXPECT().Close(),
		)

		// When
		err := runner.close()

		// Expect
		assert.NoError(t, err)
		assert.Nil(t, runner.connection)
		assert.Equal(t, runner.lastBookmark, "a bookmark")
	})

	t.Run("shouldReturnEmptyWhenInitialisedOnLastSeenBookmark", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		bookmark, err := runner.lastSeenBookmark()

		assert.NoError(t, err)
		assert.Empty(t, bookmark)
	})

	t.Run("shouldReturnStoredBookmarkOnLastSeenBookmark", func(t *testing.T) {
		ctrl, _, runner := createMocks(t)
		defer ctrl.Finish()

		runner.lastBookmark = "a bookmark"

		bookmark, err := runner.lastSeenBookmark()

		assert.NoError(t, err)
		assert.Equal(t, bookmark, "a bookmark")
	})

	t.Run("shouldReturnBookmarkFromConnectionOnLastSeenBookmark", func(t *testing.T) {
		ctrl, connection, runner := createMocks(t)
		defer ctrl.Finish()

		_ = runner.ensureConnection()

		runner.lastBookmark = "a bookmark 1"
		connection.EXPECT().LastBookmark().Return("a bookmark 2", nil)

		bookmark, err := runner.lastSeenBookmark()

		assert.NoError(t, err)
		assert.Equal(t, bookmark, "a bookmark 2")
	})

	t.Run("closeAll", func(t *testing.T) {
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldCallReceiveAll", func(t *testing.T) {
			receiveAllCalls := 0
			receiveAllOverride := func(runner *statementRunner) error {
				receiveAllCalls++
				return nil
			}

			ctrl, _, runner := createMocks(t)
			runner.receiveAllHandler = receiveAllOverride
			defer ctrl.Finish()

			assert.NoError(t, runner.receiveAllAndClose())
			assert.Equal(t, receiveAllCalls, 1)
		})

		t.Run("shouldCallReceiveAllAndPropogateError", func(t *testing.T) {
			receiveAllOverride := func(runner *statementRunner) error {
				return failure
			}

			ctrl, _, runner := createMocks(t)
			runner.receiveAllHandler = receiveAllOverride
			defer ctrl.Finish()

			assert.Equal(t, runner.receiveAllAndClose(), failure)
		})

		t.Run("shouldCallCloseConnection", func(t *testing.T) {
			closeConnectionCalls := 0
			closeConnectionOverride := func(runner *statementRunner) error {
				closeConnectionCalls++
				return nil
			}

			ctrl, _, runner := createMocks(t)
			runner.closeHandler = closeConnectionOverride
			defer ctrl.Finish()

			assert.NoError(t, runner.receiveAllAndClose())
			assert.Equal(t, closeConnectionCalls, 1)
		})

		t.Run("shouldCallCloseConnectionAndPropogateError", func(t *testing.T) {
			closeConnectionOverride := func(runner *statementRunner) error {
				return failure
			}

			ctrl, _, runner := createMocks(t)
			runner.closeHandler = closeConnectionOverride
			defer ctrl.Finish()

			assert.Equal(t, runner.receiveAllAndClose(), failure)
		})

		t.Run("shouldGivePrecedenceToReceiveAllErrors", func(t *testing.T) {
			receiveAllOverride := func(runner *statementRunner) error {
				return failure
			}
			closeConnectionOverride := func(runner *statementRunner) error {
				return fmt.Errorf("some other unexpected error")
			}

			ctrl, _, runner := createMocks(t)
			runner.receiveAllHandler = receiveAllOverride
			runner.closeHandler = closeConnectionOverride
			defer ctrl.Finish()

			assert.Equal(t, runner.receiveAllAndClose(), failure)
		})
	})

	t.Run("runStatement", func(t *testing.T) {
		statementText := "some cypher statement"
		statementParams := map[string]interface{}{"param1": 1, "param2": false, "param3": "string"}
		statement := neoStatement{text: statementText, params: statementParams}
		bookmarks := []string{"bookmark 1", "bookmark 2"}
		txTimeout := 1 * time.Minute
		txMetadata := map[string]interface{}{"a": 1, "b": true, "c": "something"}
		txConfig := TransactionConfig{Timeout: txTimeout, Metadata: txMetadata}
		runHandle := gobolt.RequestHandle(1)
		pullAllHandle := gobolt.RequestHandle(2)
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldFailWhenEnsureConnectionFails", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			connector := NewMockConnector(ctrl)
			driver := newGoboltWithConnector("bolt://localhost", connector)
			runner := newRunner(driver, AccessModeWrite, true)

			connector.EXPECT().Acquire(gobolt.AccessModeWrite).Return(nil, failure)

			result, err := runner.runStatement(&statement, bookmarks, txConfig)

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenRunFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			connection.EXPECT().Run(statementText, statementParams, bookmarks, txTimeout, txMetadata).Return(runHandle, failure)

			result, err := runner.runStatement(&statement, bookmarks, txConfig)

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenPullAllFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			gomock.InOrder(
				connection.EXPECT().Run(statementText, statementParams, bookmarks, txTimeout, txMetadata).Return(runHandle, nil),
				connection.EXPECT().PullAll().Return(pullAllHandle, failure),
			)

			result, err := runner.runStatement(&statement, bookmarks, txConfig)

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenFlushFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			gomock.InOrder(
				connection.EXPECT().Run(statementText, statementParams, bookmarks, txTimeout, txMetadata).Return(runHandle, nil),
				connection.EXPECT().PullAll().Return(pullAllHandle, nil),
				connection.EXPECT().Flush().Return(failure),
			)

			result, err := runner.runStatement(&statement, bookmarks, txConfig)

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldInvokeRunPullAllAndFlush", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			gomock.InOrder(
				connection.EXPECT().Run(statementText, statementParams, bookmarks, txTimeout, txMetadata).Return(runHandle, nil),
				connection.EXPECT().PullAll().Return(pullAllHandle, nil),
				connection.EXPECT().Flush(),
			)

			result, err := runner.runStatement(&statement, bookmarks, txConfig)

			assert.NoError(t, err)

			assert.NotNil(t, result)
			assert.Equal(t, result.runHandle, runHandle)
			assert.Equal(t, result.resultHandle, pullAllHandle)
			assert.Equal(t, result.summary.server.Address(), "localhost:7687")
			assert.Equal(t, result.summary.server.Version(), "neo4j/3.5.0")
		})

	})

	t.Run("beginTransaction", func(t *testing.T) {
		bookmarks := []string{"bookmark 1", "bookmark 2"}
		txTimeout := 1 * time.Minute
		txMetadata := map[string]interface{}{"a": 1, "b": true, "c": "something"}
		beginHandle := gobolt.RequestHandle(1)
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldFailWhenThereIsActiveConnection", func(t *testing.T) {
			ctrl, _, runner := createMocks(t)
			defer ctrl.Finish()

			_ = runner.ensureConnection()

			result, err := runner.beginTransaction(bookmarks, TransactionConfig{Timeout: txTimeout, Metadata: txMetadata})

			assert.EqualError(t, err, "unexpected state: expected no connection bound to this runner")
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenEnsureConnectionFails", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			connector := NewMockConnector(ctrl)
			driver := newGoboltWithConnector("bolt://localhost", connector)
			runner := newRunner(driver, AccessModeWrite, true)

			connector.EXPECT().Acquire(gobolt.AccessModeWrite).Return(nil, failure)

			result, err := runner.beginTransaction(bookmarks, TransactionConfig{Timeout: txTimeout, Metadata: txMetadata})

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenBeginFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			gomock.InOrder(
				connection.EXPECT().Begin(bookmarks, txTimeout, txMetadata).Return(beginHandle, failure),
				connection.EXPECT().LastBookmark(),
				connection.EXPECT().Close())

			result, err := runner.beginTransaction(bookmarks, TransactionConfig{Timeout: txTimeout, Metadata: txMetadata})

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenFlushFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			gomock.InOrder(
				connection.EXPECT().Begin(bookmarks, txTimeout, txMetadata).Return(beginHandle, nil),
				connection.EXPECT().Flush().Return(failure),
				connection.EXPECT().LastBookmark(),
				connection.EXPECT().Close())

			result, err := runner.beginTransaction(bookmarks, TransactionConfig{Timeout: txTimeout, Metadata: txMetadata})

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldInvokeBeginAndFlush", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			gomock.InOrder(
				connection.EXPECT().Begin(bookmarks, txTimeout, txMetadata).Return(beginHandle, nil),
				connection.EXPECT().Flush())

			result, err := runner.beginTransaction(bookmarks, TransactionConfig{Timeout: txTimeout, Metadata: txMetadata})

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, result.resultHandle, beginHandle)
		})
	})

	t.Run("commitTransaction", func(t *testing.T) {
		commitHandle := gobolt.RequestHandle(1)
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldFailWhenThereIsNoActiveConnection", func(t *testing.T) {
			ctrl, _, runner := createMocks(t)
			defer ctrl.Finish()

			result, err := runner.commitTransaction()

			assert.EqualError(t, err, "unexpected state: expected an active connection bound to this runner")
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenCommitFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			runner.connection = connection

			connection.EXPECT().Commit().Return(commitHandle, failure)

			result, err := runner.commitTransaction()

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenFlushFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			runner.connection = connection

			gomock.InOrder(
				connection.EXPECT().Commit().Return(commitHandle, nil),
				connection.EXPECT().Flush().Return(failure),
			)

			result, err := runner.commitTransaction()

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldInvokeCommitAndFlush", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			runner.connection = connection

			gomock.InOrder(
				connection.EXPECT().Commit().Return(commitHandle, nil),
				connection.EXPECT().Flush(),
			)

			result, err := runner.commitTransaction()

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, result.resultHandle, commitHandle)
		})
	})

	t.Run("rollbackTransaction", func(t *testing.T) {
		rollbackHandle := gobolt.RequestHandle(1)
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldFailWhenThereIsNoActiveConnection", func(t *testing.T) {
			ctrl, _, runner := createMocks(t)
			defer ctrl.Finish()

			result, err := runner.rollbackTransaction()

			assert.EqualError(t, err, "unexpected state: expected an active connection bound to this runner")
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenRollbackFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			runner.connection = connection

			connection.EXPECT().Rollback().Return(rollbackHandle, failure)

			result, err := runner.rollbackTransaction()

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenFlushFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			runner.connection = connection

			gomock.InOrder(
				connection.EXPECT().Rollback().Return(rollbackHandle, nil),
				connection.EXPECT().Flush().Return(failure),
			)

			result, err := runner.rollbackTransaction()

			assert.Equal(t, err, failure)
			assert.Nil(t, result)
		})

		t.Run("shouldInvokeRollbackAndFlush", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			runner.connection = connection

			gomock.InOrder(
				connection.EXPECT().Rollback().Return(rollbackHandle, nil),
				connection.EXPECT().Flush(),
			)

			result, err := runner.rollbackTransaction()

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, result.resultHandle, rollbackHandle)
		})
	})

	t.Run("transformError", func(t *testing.T) {
		notALeaderError := newDatabaseError("ClientError", "Neo.ClientError.Cluster.NotALeader", "a message")
		forbiddenOnReadOnlyDatabaseError := newDatabaseError("ClientError", "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", "a message")

		t.Run("shouldTransformWriteErrors", func(t *testing.T) {
			var errors = []struct {
				name string
				err  error
			}{
				{"notALeader", notALeaderError},
				{"forbiddenOnReadOnlyDatabase", forbiddenOnReadOnlyDatabaseError},
			}

			t.Run("readAccessMode", func(t *testing.T) {
				for _, testCase := range errors {
					t.Run(testCase.name, func(t *testing.T) {
						ctrl, _, runner := createMocksWithParams(t, AccessModeRead, true)
						defer ctrl.Finish()

						err := transformError(runner, testCase.err)

						assert.EqualError(t, err, "write queries cannot be performed in read access mode")
					})
				}
			})

			t.Run("writeAccessMode", func(t *testing.T) {
				t.Run("withConnection", func(t *testing.T) {
					for _, testCase := range errors {
						t.Run(testCase.name, func(t *testing.T) {
							ctrl, connection, runner := createMocksWithParams(t, AccessModeWrite, true)
							defer ctrl.Finish()

							runner.connection = connection

							err := transformError(runner, testCase.err)

							assert.EqualError(t, err, "server at localhost:7687 no longer accepts writes")
						})
					}
				})

				t.Run("withoutConnection", func(t *testing.T) {
					for _, testCase := range errors {
						t.Run(testCase.name, func(t *testing.T) {
							ctrl, _, runner := createMocksWithParams(t, AccessModeWrite, true)
							defer ctrl.Finish()

							err := transformError(runner, testCase.err)

							assert.EqualError(t, err, "server at unknown no longer accepts writes")
						})
					}
				})
			})
		})

		t.Run("shouldReturnOriginal", func(t *testing.T) {
			var errors = []struct {
				name string
				err  error
			}{
				{"an ordinary error", fmt.Errorf("an unknown error")},
				{"a driver error", newDriverError("a driver error")},
				{"a session expired error", newSessionExpiredError("a session expired error")},
				{"a connector error", newConnectorError(1, 1, "text", "context", "description")},
			}

			for _, testCase := range errors {
				t.Run(testCase.name, func(t *testing.T) {
					ctrl, _, runner := createMocksWithParams(t, AccessModeWrite, true)
					defer ctrl.Finish()

					err := transformError(runner, testCase.err)

					assert.Equal(t, err, testCase.err)
				})
			}

		})
	})

	createResult := func(runner *statementRunner) *neoResult {
		return &neoResult{
			keys:            []string{},
			records:         []Record{},
			current:         nil,
			summary:         &neoResultSummary{},
			runner:          runner,
			err:             nil,
			runHandle:       1,
			runCompleted:    false,
			resultHandle:    2,
			resultCompleted: false,
		}
	}

	createResultWithConn := func(runner *statementRunner) *neoResult {
		_ = runner.ensureConnection()

		return createResult(runner)
	}

	t.Run("handleRunPhase", func(t *testing.T) {
		fields := []string{"Field 1", "Field 2"}
		metadata := map[string]interface{}{"type": "r"}
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldSkipIfRunIsCompleted", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)
			result.runCompleted = true

			connection.EXPECT().Fetch(gomock.Any()).Times(0)
			connection.EXPECT().Fields().Times(0)
			connection.EXPECT().Metadata().Times(0)

			assert.NoError(t, runner.handleRunPhase(result))
		})

		t.Run("shouldFailWhenFetchFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.runHandle).Return(gobolt.FetchType(0), failure)
			connection.EXPECT().Fields().Times(0)
			connection.EXPECT().Metadata().Times(0)

			assert.Equal(t, runner.handleRunPhase(result), failure)
		})

		t.Run("shouldFailWhenFetchReturnsRecord", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.runHandle).Return(gobolt.FetchTypeRecord, nil)
			connection.EXPECT().Fields().Times(0)
			connection.EXPECT().Metadata().Times(0)

			assert.EqualError(t, runner.handleRunPhase(result), "unexpected response received while waiting for a METADATA")
		})

		t.Run("shouldFailWhenFetchReturnsError", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.runHandle).Return(gobolt.FetchTypeError, nil)
			connection.EXPECT().Fields().Times(0)
			connection.EXPECT().Metadata().Times(0)

			assert.EqualError(t, runner.handleRunPhase(result), "unexpected response received while waiting for a METADATA")
		})

		t.Run("shouldFailWhenFieldsReturnsError", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.runHandle).Return(gobolt.FetchTypeMetadata, nil)
			connection.EXPECT().Fields().Return(nil, failure)
			connection.EXPECT().Metadata().Times(0)

			assert.Equal(t, runner.handleRunPhase(result), failure)
		})

		t.Run("shouldFailWhenMetadataReturnsError", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.runHandle).Return(gobolt.FetchTypeMetadata, nil)
			connection.EXPECT().Fields().Return([]string{"a"}, nil)
			connection.EXPECT().Metadata().Return(nil, failure)

			assert.Equal(t, runner.handleRunPhase(result), failure)
		})

		t.Run("shouldExecuteRunPhase", func(t *testing.T) {
			var collectedResult *neoResult
			var collectedMetadata map[string]interface{}
			collectMetadata = func(result *neoResult, metadata map[string]interface{}) {
				collectedResult = result
				collectedMetadata = metadata
			}

			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.runHandle).Return(gobolt.FetchTypeMetadata, nil)
			connection.EXPECT().Fields().Return(fields, nil)
			connection.EXPECT().Metadata().Return(metadata, nil)

			assert.NoError(t, runner.handleRunPhase(result))

			assert.Equal(t, result.keys, fields)
			assert.Equal(t, collectedResult, result)
			assert.Equal(t, collectedMetadata, metadata)
			assert.True(t, result.runCompleted)
		})
	})

	t.Run("handleRecordsPhase", func(t *testing.T) {
		record := []interface{}{true, 1, "data"}
		metadata := map[string]interface{}{"result_consumed_after": 1}
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldSkipIfRunIsCompleted", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)
			result.resultCompleted = true

			connection.EXPECT().Fetch(gomock.Any()).Times(0)
			connection.EXPECT().Metadata().Times(0)
			connection.EXPECT().Data().Times(0)

			assert.NoError(t, runner.handleRecordsPhase(result))
		})

		t.Run("shouldFailWhenFetchFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.resultHandle).Return(gobolt.FetchType(0), failure)
			connection.EXPECT().Metadata().Times(0)
			connection.EXPECT().Data().Times(0)

			assert.Equal(t, runner.handleRecordsPhase(result), failure)
		})

		t.Run("shouldFailOnFetchError", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.resultHandle).Return(gobolt.FetchTypeError, nil)
			connection.EXPECT().Metadata().Times(0)
			connection.EXPECT().Data().Times(0)

			assert.EqualError(t, runner.handleRecordsPhase(result), "unable to fetch from connection")
		})

		t.Run("shouldFailWhenMetadataFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.resultHandle).Return(gobolt.FetchTypeMetadata, nil)
			connection.EXPECT().Metadata().Return(nil, failure)
			connection.EXPECT().Data().Times(0)

			assert.Equal(t, runner.handleRecordsPhase(result), failure)
		})

		t.Run("shouldCompleteRecordPhaseOnMetadata", func(t *testing.T) {
			var cmResult *neoResult
			var cmMetadata map[string]interface{}
			collectMetadata = func(result *neoResult, metadata map[string]interface{}) {
				cmResult = result
				cmMetadata = metadata
			}

			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.resultHandle).Return(gobolt.FetchTypeMetadata, nil)
			connection.EXPECT().Metadata().Return(metadata, nil)

			assert.NoError(t, runner.handleRecordsPhase(result))

			assert.Equal(t, cmResult, result)
			assert.Equal(t, cmMetadata, metadata)
			assert.True(t, result.resultCompleted)
		})

		t.Run("shouldFailWhenDataFails", func(t *testing.T) {
			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.resultHandle).Return(gobolt.FetchTypeRecord, nil)
			connection.EXPECT().Data().Return(nil, failure)

			assert.Equal(t, runner.handleRecordsPhase(result), failure)
		})

		t.Run("shouldCollectRecord", func(t *testing.T) {
			var collectedResult *neoResult
			var collectedFields []interface{}
			collectRecord = func(result *neoResult, fields []interface{}) {
				collectedResult = result
				collectedFields = fields
			}

			ctrl, connection, runner := createMocks(t)
			defer ctrl.Finish()

			result := createResultWithConn(runner)

			connection.EXPECT().Fetch(result.resultHandle).Return(gobolt.FetchTypeRecord, nil)
			connection.EXPECT().Data().Return(record, nil)

			assert.NoError(t, runner.handleRecordsPhase(result))
			assert.Equal(t, collectedResult, result)
			assert.Equal(t, collectedFields, record)
		})
	})

	t.Run("receive", func(t *testing.T) {
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldFailWhenNoPendingResults", func(t *testing.T) {
			ctrl, _, runner := createMocks(t)
			defer ctrl.Finish()

			result, err := runner.receive()

			assert.EqualError(t, err, "unexpected state: no pending results registered on session")
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenNoActiveConnection", func(t *testing.T) {
			ctrl, _, runner := createMocks(t)
			defer ctrl.Finish()

			runner.pendingResults = append(runner.pendingResults, createResult(runner))

			result, err := runner.receive()

			assert.EqualError(t, err, "unexpected state: no open connection to perform receive")
			assert.Nil(t, result)
		})

		t.Run("shouldFailWhenRunPhaseFails", func(t *testing.T) {
			runPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				return failure
			}
			recordsPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				assert.Fail(t, "handleRecordPhase call is not expected")
				return nil
			}

			ctrl, _, runner := createMocks(t)
			runner.runPhaseHandler = runPhaseOverride
			runner.recordsPhaseHandler = recordsPhaseOverride
			defer ctrl.Finish()

			result := createResultWithConn(runner)
			runner.pendingResults = append(runner.pendingResults, result)

			recvdResult, err := runner.receive()

			assert.Equal(t, err, failure)
			assert.Nil(t, recvdResult)
			assert.True(t, result.runCompleted)
			assert.Equal(t, result.err, failure)
		})

		t.Run("shouldFailWhenRecordPhaseFails", func(t *testing.T) {
			runPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.runCompleted = true
				return nil
			}
			recordsPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				return failure
			}

			var testCases = []struct {
				name      string
				autoClose bool
			}{
				{"autoClose=true", true},
				{"autoClose=false", false},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					ctrl, connection, runner := createMocksWithParams(t, AccessModeWrite, testCase.autoClose)
					runner.runPhaseHandler = runPhaseOverride
					runner.recordsPhaseHandler = recordsPhaseOverride
					defer ctrl.Finish()

					if testCase.autoClose {
						// we expect auto-close logic to kick-in
						connection.EXPECT().LastBookmark()
						connection.EXPECT().Close()
					}

					result := createResultWithConn(runner)
					runner.pendingResults = append(runner.pendingResults, result)

					recvdResult, err := runner.receive()

					// returned error should be the one from handleRecordsPhase
					assert.Equal(t, err, failure)
					assert.Nil(t, recvdResult)

					// we should mark the result complete and store the error
					assert.True(t, result.resultCompleted)
					assert.Equal(t, result.err, failure)

					// we should remove active result from the result queue
					assert.Len(t, runner.pendingResults, 0)
				})
			}
		})

		t.Run("shouldReturnResult", func(t *testing.T) {
			runPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.runCompleted = true
				return nil
			}
			recordsPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.resultCompleted = true
				return nil
			}

			ctrl, _, runner := createMocksWithParams(t, AccessModeRead, false)
			runner.runPhaseHandler = runPhaseOverride
			runner.recordsPhaseHandler = recordsPhaseOverride
			defer ctrl.Finish()

			result := createResultWithConn(runner)
			runner.pendingResults = append(runner.pendingResults, result)

			recvdResult, err := runner.receive()

			// we should receive the actual record and no errors
			assert.NoError(t, err)
			assert.Equal(t, recvdResult, result)

			// we should mark the result complete and store the error
			assert.True(t, result.runCompleted)
			assert.True(t, result.resultCompleted)
			assert.NoError(t, result.err)

			// we should remove active result from the result queue
			assert.Len(t, runner.pendingResults, 0)
		})
	})

	t.Run("receiveAll", func(t *testing.T) {
		failure := fmt.Errorf("an unexpected error")

		t.Run("shouldNotReceiveIfNoPendingResults", func(t *testing.T) {
			receiveOverride := func(runner *statementRunner) (*neoResult, error) {
				assert.Fail(t, "receive call unexpected here")
				return nil, nil
			}

			ctrl, _, runner := createMocksWithParams(t, AccessModeRead, false)
			runner.receiveHandler = receiveOverride
			defer ctrl.Finish()

			assert.NoError(t, runner.receiveAll())
		})

		t.Run("shouldReceiveAllResults", func(t *testing.T) {
			runPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.runCompleted = true
				return nil
			}
			recordsPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.resultCompleted = true
				return nil
			}

			ctrl, _, runner := createMocksWithParams(t, AccessModeRead, false)
			runner.runPhaseHandler = runPhaseOverride
			runner.recordsPhaseHandler = recordsPhaseOverride
			defer ctrl.Finish()

			resultsPushed := []*neoResult{}
			for i := 0; i < 10; i++ {
				resultsPushed = append(resultsPushed, createResultWithConn(runner))
			}

			runner.pendingResults = append(runner.pendingResults, resultsPushed...)

			err := runner.receiveAll()

			assert.NoError(t, err)
			assert.Empty(t, runner.pendingResults)
		})

		t.Run("shouldReceiveAllResultsEvenSomeResultsFailInRunPhase", func(t *testing.T) {
			runs := 0
			runPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				runs++
				if runs%4 == 0 {
					return failure
				}
				result.runCompleted = true
				return nil
			}
			recordsPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.resultCompleted = true
				return nil
			}

			ctrl, _, runner := createMocksWithParams(t, AccessModeRead, false)
			runner.runPhaseHandler = runPhaseOverride
			runner.recordsPhaseHandler = recordsPhaseOverride
			defer ctrl.Finish()

			resultsPushed := []*neoResult{}
			for i := 0; i < 10; i++ {
				resultsPushed = append(resultsPushed, createResultWithConn(runner))
			}

			runner.pendingResults = append(runner.pendingResults, resultsPushed...)

			err := runner.receiveAll()

			assert.Equal(t, err, failure)
			assert.Empty(t, runner.pendingResults)
		})

		t.Run("shouldReceiveAllResultsEvenSomeResultsFailInRecordsPhase", func(t *testing.T) {
			runPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				result.runCompleted = true
				return nil
			}
			records := 0
			recordsPhaseOverride := func(runner *statementRunner, result *neoResult) error {
				records++
				if records%4 == 0 {
					return failure
				}
				result.resultCompleted = true
				return nil
			}

			ctrl, _, runner := createMocksWithParams(t, AccessModeRead, false)
			runner.runPhaseHandler = runPhaseOverride
			runner.recordsPhaseHandler = recordsPhaseOverride
			defer ctrl.Finish()

			resultsPushed := []*neoResult{}
			for i := 0; i < 10; i++ {
				resultsPushed = append(resultsPushed, createResultWithConn(runner))
			}

			runner.pendingResults = append(runner.pendingResults, resultsPushed...)

			err := runner.receiveAll()

			assert.Equal(t, err, failure)
			assert.Empty(t, runner.pendingResults)
		})
	})
}
