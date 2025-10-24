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

package neo4j

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/router"
	. "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
)

func assertNoRouter(t *testing.T, d Driver) {
	t.Helper()
	_, isDirectRouter := d.(*driver).router.(*directRouter)
	if !isDirectRouter {
		t.Error("Expected no router")
	}
}

func assertNoRouterAddress(t *testing.T, d Driver, address string) {
	t.Helper()
	direct := d.(*driver).router.(*directRouter)
	if direct.address != address {
		t.Errorf("Address mismatch %s vs %s", address, direct.address)
	}
}

func assertRouter(t *testing.T, d Driver) {
	t.Helper()
	_, isRouter := d.(*driver).router.(*router.Router)
	if !isRouter {
		t.Error("Expected router")
	}
}

func assertRouterContext(t *testing.T, d Driver, context map[string]string) {
	t.Helper()
	r := d.(*driver).router.(*router.Router)
	c := r.Context()
	if !reflect.DeepEqual(c, context) {
		t.Errorf("Router contexts differ: %#v vs %#v", c, context)
	}
}

func assertSkipEncryption(t *testing.T, d Driver, skipEncryption bool) {
	t.Helper()
	c := d.(*driver).connector
	if c.SkipEncryption != skipEncryption {
		t.Errorf("SkipEncryption mismatch, %t vs %t", skipEncryption, c.SkipEncryption)
	}
}

func assertSkipVerify(t *testing.T, d Driver, skipVerify bool) {
	t.Helper()
	c := d.(*driver).connector
	if c.SkipVerify != skipVerify {
		t.Errorf("SkipVerify mismatch, %t vs %t", skipVerify, c.SkipVerify)
	}
}

func assertNetwork(t *testing.T, d Driver, network string) {
	t.Helper()
	c := d.(*driver).connector
	if c.Network != network {
		t.Errorf("Network mismatch, %s vs %s", network, c.Network)
	}
}

func TestDriverURISchemes(t *testing.T) {
	uriSchemeTests := []struct {
		scheme         string
		testing        string
		router         bool
		skipEncryption bool
		skipVerify     bool
		network        string
		address        string
	}{
		{"bolt", "bolt://localhost:7687", false, true, false, "tcp", "localhost:7687"},
		{"bolt+s", "bolt+s://localhost:7687", false, false, false, "tcp", "localhost:7687"},
		{"bolt+ssc", "bolt+ssc://localhost:7687", false, false, true, "tcp", "localhost:7687"},
		{"bolt+unix", "bolt+unix:///tmp/a.socket", false, true, false, "unix", "/tmp/a.socket"},
		{"neo4j", "neo4j://localhost:7687", true, true, false, "tcp", ""},
		{"neo4j+s", "neo4j+s://localhost:7687", true, false, false, "tcp", ""},
		{"neo4j+ssc", "neo4j+ssc://localhost:7687", true, false, true, "tcp", ""},
	}

	for _, tt := range uriSchemeTests {
		t.Run(tt.scheme, func(t *testing.T) {
			driver, err := NewDriver(tt.testing, NoAuth())

			AssertNoError(t, err)
			AssertStringEqual(t, driver.Target().Scheme, tt.scheme)
			if !tt.router {
				assertNoRouter(t, driver)
				assertNoRouterAddress(t, driver, tt.address)
			} else {
				assertRouter(t, driver)
			}
			assertSkipEncryption(t, driver, tt.skipEncryption)
			if !tt.skipEncryption {
				assertSkipVerify(t, driver, tt.skipVerify)
			}
			assertNetwork(t, driver, tt.network)
		})
	}
}

func TestDriverInvalidURISchemes(t *testing.T) {
	invalidURISchemeTests := []struct {
		name    string
		scheme  string
		testing string
	}{
		{"bolt+routing://", "bolt+routing", "bolt+routing://localhost:7687"},
		{"invalid://", "invalid", "invalid://localhost:7687"},
	}

	for _, tt := range invalidURISchemeTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDriver(tt.testing, NoAuth())

			AssertError(t, err)
			assertUsageError(t, err)
			AssertStringContain(t, err.Error(), "scheme")
		})
	}
}

func TestDriverURIRoutingContext(t *testing.T) {
	t.Run("Extracts keys", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost:7687?x=y&a=b", NoAuth())

		AssertNoError(t1, err)
		assertRouterContext(t1, driver, map[string]string{"x": "y", "a": "b", "address": "localhost:7687"})
	})

	t.Run("Duplicate keys should error", func(t1 *testing.T) {
		_, err := NewDriver("neo4j://localhost:7687?x=y&x=b", NoAuth())

		AssertError(t, err)
		assertUsageError(t, err)
	})

	t.Run("Reserved key 'address' should error", func(t *testing.T) {
		_, err := NewDriver("neo4j://localhost:7687?x=y&address=b", NoAuth())

		AssertError(t, err)
		assertUsageError(t, err)
	})
}

func TestDriverDefaultPort(t *testing.T) {
	t.Run("neo4j://localhost should default to port 7687", func(t1 *testing.T) {
		driver, err := NewDriver("neo4j://localhost", NoAuth())
		driverTarget := driver.Target()

		AssertNoError(t1, err)
		AssertStringEqual(t1, driverTarget.Port(), "7687")
		assertRouterContext(t1, driver, map[string]string{"address": "localhost:7687"})
	})
}

func TestNewDriverAndClose(t *testing.T) {
	ctx := context.Background()
	driver, err := NewDriver("bolt://localhost:7687", NoAuth())
	AssertNoError(t, err)

	driverTarget := driver.Target()

	if driverTarget.Scheme != "bolt" {
		t.Errorf("the URI scheme was not properly set %v", driverTarget.Scheme)
	}

	if driverTarget.Hostname() != "localhost" {
		t.Errorf("the hostname is not the expected %v", driverTarget.Hostname())
	}

	if driverTarget.Port() != "7687" {
		t.Errorf("the port is not the expected %v", driverTarget.Port())
	}

	err = driver.Close(ctx)
	AssertNoError(t, err)

	session := driver.NewSession(ctx, SessionConfig{})
	_, err = session.Run(ctx, "cypher", nil)
	if !IsUsageError(err) {
		t.Errorf("should not allow new session after driver being closed")
	}

	err = driver.Close(ctx)
	if err != nil {
		t.Errorf("should allow the close call on a closed driver")
	}
}

func TestDriverMultipleCloseCalls(t *testing.T) {
	ctx := context.Background()
	driver, err := NewDriver("bolt://localhost:7687", NoAuth())
	AssertNoError(t, err)

	// Test multiple Close calls - should not deadlock
	for i := range 10 {
		err = driver.Close(ctx)
		if err != nil {
			t.Errorf("Close() call %d unexpected error = %v", i+1, err)
		}
	}
}

func TestDriverSessionCreation(t *testing.T) {
	driverSessionCreationTests := []struct {
		name      string
		testing   string
		mode      AccessMode
		bookmarks Bookmarks
	}{
		{"Write", "bolt://localhost:7687", AccessModeWrite, []string(nil)},
		{"Read", "bolt://localhost:7687", AccessModeRead, []string(nil)},
		{"Write+bookmarks", "bolt://localhost:7687", AccessModeWrite, []string{"B1", "B2", "B3"}},
		{"Read+bookmarks", "bolt://localhost:7687", AccessModeRead, []string{"B1", "B2", "B3", "B4"}},
	}

	for _, tt := range driverSessionCreationTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			driver, err := NewDriver(tt.testing, NoAuth())
			AssertNoError(t, err)

			sessi := driver.NewSession(ctx, SessionConfig{AccessMode: tt.mode, Bookmarks: tt.bookmarks})
			sess := sessi.(*session)

			if AccessMode(sess.defaultMode) != tt.mode {
				t.Errorf("the defaultMode was not correctly set %v", AccessMode(sess.defaultMode))
			}

			if len(sess.bookmarks.currentBookmarks()) != len(tt.bookmarks) {
				t.Errorf("the bookmarks was not correctly set %v", sess.bookmarks)
			}
		})
	}
}

func TestDriverExecuteQuery(outer *testing.T) {
	outer.Parallel()

	ctx := context.Background()
	keys := []string{"42"}
	records := []*Record{{
		Values: []any{42},
	}}
	summary := &fakeSummary{resultAvailableAfter: 42 * time.Millisecond}
	defaultBookmarkManager := &fakeBookmarkManager{}
	customBookmarkManager := &fakeBookmarkManager{}
	defaultSessionConfig := SessionConfig{BookmarkManager: defaultBookmarkManager}

	outer.Run("nil driver is not allowed", func(t *testing.T) {
		_, err := ExecuteQuery(ctx, nil, "RETURN 42", nil, EagerResultTransformer)

		AssertErrorMessageContains(t, err, "nil is not a valid Driver argument.")
	})

	type testCase[T any] struct {
		description           string
		resultTransformer     func() ResultTransformer[T]
		configurers           []ExecuteQueryConfigurationOption
		createSession         *fakeSession
		expectedSessionConfig SessionConfig
		expectedResult        T
		expectedErr           error
	}
	testCases := []testCase[*EagerResult]{
		{
			description:       "returns expected result of assumed write query",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns expected result of assumed write query impersonating user",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithImpersonatedUser("jane")},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: SessionConfig{ImpersonatedUser: "jane", BookmarkManager: defaultBookmarkManager},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns expected result of assumed write query targeting database",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithDatabase("imdb")},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: SessionConfig{DatabaseName: "imdb", BookmarkManager: defaultBookmarkManager},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns expected result of assumed write query with custom bookmark manager",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithBookmarkManager(customBookmarkManager)},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: SessionConfig{BookmarkManager: customBookmarkManager},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns expected result of assumed write query with nil bookmark manager",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithoutBookmarkManager()},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: SessionConfig{BookmarkManager: nil},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns expected result of explicit write query",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithWritersRouting()},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns expected result of explicit read query",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns error when routing mode is invalid",
			resultTransformer: EagerResultTransformer,
			configurers: []ExecuteQueryConfigurationOption{func(config *ExecuteQueryConfiguration) {
				config.Routing = 42
			}},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("unsupported routing control, expected 0 (Write) or 1 (Read) but got: 42"),
		},
		{
			description:       "returns error when result transformer function is nil",
			resultTransformer: nil,
			expectedErr: &UsageError{Message: "nil is not a valid ResultTransformer function argument. " +
				"Consider passing EagerResultTransformer or a function that returns an instance of your own " +
				"ResultTransformer implementation"},
		},
		{
			description:       "returns error when result transformer function returns a nil transformer",
			resultTransformer: func() ResultTransformer[*EagerResult] { return nil },
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr: &UsageError{Message: "expected the result transformer function to return a valid ResultTransformer" +
				" instance, but got nil"},
		},
		{
			description:       "returns error when write result keys cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex: -1,
					keysErr:   fmt.Errorf("dude, where are my keys"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("dude, where are my keys"),
		},
		{
			description:       "returns error when write result records cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex: -1,
					keys:      keys,
					nextErr:   fmt.Errorf("one does not simply get the next record"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("one does not simply get the next record"),
		},
		{
			description:       "returns error when write result summary cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summaryErr:  fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description:       "returns error when write result summary cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summaryErr:  fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description:       "returns error when write execution fails",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteErr: fmt.Errorf("oopsie"),
			},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("oopsie"),
		},
		{
			description:       "returns error when session close fails",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				},
				closeErr: fmt.Errorf("looking closer: it seems close erred 🥁"),
			},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("looking closer: it seems close erred 🥁"),
			// TODO: make the result nil in that case since people should check the error first?
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description:       "returns combined error when a previous error occurred before the session close's'",
			resultTransformer: EagerResultTransformer,
			createSession: &fakeSession{
				executeWriteErr: fmt.Errorf("he's a pirate writerrrr"),
				closeErr:        fmt.Errorf("looking closer: it seems close erred 🥁"),
			},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr: fmt.Errorf("error %v occurred after previous error %w",
				errors.New("looking closer: it seems close erred 🥁"),
				errors.New("he's a pirate writerrrr")),
		},
		{
			description:       "returns error when read result keys cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex: -1,
					keysErr:   fmt.Errorf("dude, where are my keys"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("dude, where are my keys"),
		},
		{
			description:       "returns error when read result records cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex: -1,
					keys:      keys,
					nextErr:   fmt.Errorf("one does not simply get the next record"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("one does not simply get the next record"),
		},
		{
			description:       "returns error when read result summary cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summaryErr:  fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description:       "returns error when read result summary cannot be retrieved",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summaryErr:  fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description:       "returns error when read execution fails",
			resultTransformer: EagerResultTransformer,
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadErr: fmt.Errorf("oopsie"),
			},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("oopsie"),
		},
		{
			description:       "returns error when custom transformer accept fails",
			resultTransformer: newFailingAcceptTransformer(fmt.Errorf("accept... except not")),
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("accept... except not"),
		},
		{
			description:       "returns error when custom transformer completion fails",
			resultTransformer: newFailingCompleteTransformer(fmt.Errorf("complete...ly not")),
			configurers:       []ExecuteQueryConfigurationOption{ExecuteQueryWithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					nextIndex:   -1,
					keys:        keys,
					nextRecords: records,
					summary:     summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("complete...ly not"),
		},
	}

	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			driver := &driverDelegate{
				newSession: func(_ context.Context, config SessionConfig) Session {
					AssertDeepEquals(t, testCase.expectedSessionConfig, config)
					return testCase.createSession
				},
				delegate: &driver{
					executeQueryBookmarkManager: defaultBookmarkManager,
					mut:                         sync.Mutex{},
				},
			}

			eagerResult, err := ExecuteQuery[*EagerResult](
				ctx, driver, "RETURN 42", nil, testCase.resultTransformer, testCase.configurers...)

			AssertDeepEquals(t, testCase.expectedErr, err)
			AssertDeepEquals(t, testCase.expectedResult, eagerResult)
		})
	}

	outer.Run("default bookmark manager is thread-safe", func(t *testing.T) {
		driver := &driverDelegate{
			newSession: func(_ context.Context, config SessionConfig) Session {
				return &fakeSession{
					executeWriteErr: fmt.Errorf("oopsie, write failed"),
				}
			},
			delegate: &driver{
				mut: sync.Mutex{},
			},
		}

		var wait sync.WaitGroup
		var bookmarkManagerAddresses sync.Map
		goroutineCount := 50
		wait.Add(goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			go func(i int) {
				callExecuteQueryOrBookmarkManagerGetter(driver, i)
				storeBookmarkManagerAddress(
					&bookmarkManagerAddresses,
					driver.delegate.executeQueryBookmarkManager.(*bookmarkManager))
				wait.Done()
			}(i)
		}
		wait.Wait()

		addressCounts := make(map[uintptr]int32, goroutineCount)
		bookmarkManagerAddresses.Range(func(key, value any) bool {
			addressCounts[key.(uintptr)] = *(value.(*int32))
			return true
		})
		if len(addressCounts) != 1 {
			t.Errorf("expected exactly 1 bookmark manager pointer to have been created, got %v", addressCounts)
		}
		address := uintptr(unsafe.Pointer(driver.delegate.executeQueryBookmarkManager.(*bookmarkManager)))
		if count, found := addressCounts[address]; !found || count != int32(goroutineCount) {
			t.Errorf("expected pointer address %v to be seen %d time(s), got these instead %v", address, count, addressCounts)
		}
	})
}

func callExecuteQueryOrBookmarkManagerGetter(driver Driver, i int) {
	if i%2 == 0 {
		// this lazily initializes the default bookmark manager
		_ = driver.ExecuteQueryBookmarkManager()
	} else {
		// this as well
		_, _ = ExecuteQuery[*EagerResult](context.Background(), driver, "RETURN 42", nil, EagerResultTransformer)
	}
}

func storeBookmarkManagerAddress(bookmarkManagerAddresses *sync.Map, bookmarkMgr *bookmarkManager) {
	address := uintptr(unsafe.Pointer(bookmarkMgr))
	defaultCount := int32(1)
	if count, loaded := bookmarkManagerAddresses.LoadOrStore(address, &defaultCount); loaded {
		atomic.AddInt32(count.(*int32), 1)
	}
}

type failingResultTransformer struct {
	acceptErr   error
	completeErr error
}

func newFailingAcceptTransformer(err error) func() ResultTransformer[*EagerResult] {
	return func() ResultTransformer[*EagerResult] {
		return &failingResultTransformer{acceptErr: err}
	}
}

func newFailingCompleteTransformer(err error) func() ResultTransformer[*EagerResult] {
	return func() ResultTransformer[*EagerResult] {
		return &failingResultTransformer{completeErr: err}
	}
}

func (f *failingResultTransformer) Accept(*Record) error {
	return f.acceptErr
}

func (f *failingResultTransformer) Complete([]string, ResultSummary) (*EagerResult, error) {
	return nil, f.completeErr
}

type driverDelegate struct {
	delegate   *driver
	newSession func(context.Context, SessionConfig) Session
}

func (d *driverDelegate) ExecuteQueryBookmarkManager() BookmarkManager {
	return d.delegate.ExecuteQueryBookmarkManager()
}

func (d *driverDelegate) Target() url.URL {
	return d.delegate.Target()
}

func (d *driverDelegate) NewSession(ctx context.Context, config SessionConfig) Session {
	return d.newSession(ctx, config)
}

func (d *driverDelegate) VerifyConnectivity(ctx context.Context) error {
	return d.delegate.VerifyConnectivity(ctx)
}

func (d *driverDelegate) VerifyAuthentication(ctx context.Context, auth *AuthToken) error {
	return d.delegate.VerifyAuthentication(ctx, auth)
}

func (d *driverDelegate) Close(ctx context.Context) error {
	return d.delegate.Close(ctx)
}

func (d *driverDelegate) IsEncrypted() bool {
	return d.delegate.IsEncrypted()
}

func (d *driverDelegate) GetServerInfo(ctx context.Context) (ServerInfo, error) {
	return d.delegate.GetServerInfo(ctx)
}

type fakeSession struct {
	executeReadTransactionResult   *fakeResult
	executeReadErr                 error
	executeWriteTransactionResult  *fakeResult
	executeWriteTransactionResults []*fakeResult
	executeWriteErr                error
	executeWriteErrs               []error
	executeWriteIndex              int
	closeErr                       error
}

func (s *fakeSession) LastBookmarks() Bookmarks {
	panic("implement me")
}

func (s *fakeSession) BeginTransaction(context.Context, ...func(*TransactionConfig)) (ExplicitTransaction, error) {
	panic("implement me")
}

func (s *fakeSession) ExecuteRead(_ context.Context, callback ManagedTransactionWork, _ ...func(*TransactionConfig)) (any, error) {
	return callback(&fakeManagedTransaction{
		result: s.executeReadTransactionResult,
		err:    s.executeReadErr,
	})
}

func (s *fakeSession) ExecuteWrite(_ context.Context, callback ManagedTransactionWork, _ ...func(*TransactionConfig)) (any, error) {
	result := s.executeWriteTransactionResult
	err := s.executeWriteErr
	if s.executeWriteErrs != nil {
		result = s.executeWriteTransactionResults[s.executeWriteIndex]
		err = s.executeWriteErrs[s.executeWriteIndex]
		s.executeWriteIndex++
	}
	return callback(&fakeManagedTransaction{result: result, err: err})
}
func (s *fakeSession) executeQueryRead(_ context.Context, callback ManagedTransactionWork, _ ...func(*TransactionConfig)) (any, error) {
	return callback(&fakeManagedTransaction{
		result: s.executeReadTransactionResult,
		err:    s.executeReadErr,
	})
}

func (s *fakeSession) executeQueryWrite(_ context.Context, callback ManagedTransactionWork, _ ...func(*TransactionConfig)) (any, error) {
	result := s.executeWriteTransactionResult
	err := s.executeWriteErr
	if s.executeWriteErrs != nil {
		result = s.executeWriteTransactionResults[s.executeWriteIndex]
		err = s.executeWriteErrs[s.executeWriteIndex]
		s.executeWriteIndex++
	}
	return callback(&fakeManagedTransaction{result: result, err: err})
}
func (s *fakeSession) Run(context.Context, string, map[string]any, ...func(*TransactionConfig)) (Result, error) {
	panic("implement me")
}

func (s *fakeSession) Close(context.Context) error {
	return s.closeErr
}

func (s *fakeSession) getServerInfo(context.Context) (ServerInfo, error) {
	panic("implement me")
}

func (s *fakeSession) verifyAuthentication(context.Context) error {
	panic("implement me")
}

type fakeManagedTransaction struct {
	result *fakeResult
	err    error
}

func (tx *fakeManagedTransaction) Run(context.Context, string, map[string]any) (Result, error) {
	return tx.result, tx.err
}

type fakeResult struct {
	keys        []string
	keysErr     error
	nextRecords []*Record
	nextIndex   int
	nextErr     error
	summary     ResultSummary
	summaryErr  error
}

func (f *fakeResult) Keys() ([]string, error) {
	return f.keys, f.keysErr
}

func (f *fakeResult) NextRecord(context.Context, **Record) bool {
	panic("implement me")
}

func (f *fakeResult) Next(context.Context) bool {
	if f.nextErr != nil {
		return false
	}
	if f.nextIndex == len(f.nextRecords)-1 {
		return false
	}
	f.nextIndex++
	return true
}

func (f *fakeResult) PeekRecord(context.Context, **Record) bool {
	panic("implement me")
}

func (f *fakeResult) Peek(context.Context) bool {
	panic("implement me")
}

func (f *fakeResult) Err() error {
	return f.nextErr
}

func (f *fakeResult) Record() *Record {
	return f.nextRecords[f.nextIndex]
}

func (f *fakeResult) Records(context.Context) func(yield func(*Record, error) bool) {
	panic("implement me")
}

func (f *fakeResult) Collect(context.Context) ([]*Record, error) {
	panic("implement me")
}

func (f *fakeResult) Single(context.Context) (*Record, error) {
	panic("implement me")
}

func (f *fakeResult) Consume(context.Context) (ResultSummary, error) {
	return f.summary, f.summaryErr
}

func (f *fakeResult) IsOpen() bool {
	panic("implement me")
}

func (f *fakeResult) buffer(context.Context) {
	panic("implement me")
}

func (f *fakeResult) errorHandler(error) {
	panic("implement me")
}

type fakeSummary struct {
	resultAvailableAfter time.Duration
}

func (sum *fakeSummary) Server() ServerInfo {
	panic("implement me")
}

func (sum *fakeSummary) Query() Query {
	panic("implement me")
}

func (sum *fakeSummary) StatementType() StatementType {
	panic("implement me")
}

func (sum *fakeSummary) QueryType() QueryType {
	panic("implement me")
}

func (sum *fakeSummary) Counters() Counters {
	panic("implement me")
}

func (sum *fakeSummary) Plan() Plan {
	panic("implement me")
}

func (sum *fakeSummary) Profile() ProfiledPlan {
	panic("implement me")
}

func (sum *fakeSummary) Notifications() []Notification {
	panic("implement me")
}

func (sum *fakeSummary) GqlStatusObjects() []GqlStatusObject {
	panic("implement me")
}

func (sum *fakeSummary) ResultAvailableAfter() time.Duration {
	return sum.resultAvailableAfter
}

func (sum *fakeSummary) ResultConsumedAfter() time.Duration {
	panic("implement me")
}

func (sum *fakeSummary) Database() DatabaseInfo {
	panic("implement me")
}
