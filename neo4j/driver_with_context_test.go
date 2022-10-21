package neo4j

import (
	"context"
	"fmt"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
	"time"
)

// TODO:
// - test session.Close error
// - define API at interface level
// - implement TestKit support

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

	type testCase struct {
		description           string
		configurers           []ExecuteQueryConfigurationOption
		createSession         *fakeSession
		expectedSessionConfig SessionConfig
		expectedResult        *EagerResult
		expectedErr           error
	}
	testCases := []testCase{
		{
			description: "returns expected result of assumed write query",
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:    keys,
					collect: records,
					summary: summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description: "returns expected result of assumed write query impersonating user",
			configurers: []ExecuteQueryConfigurationOption{WithImpersonatedUser("jane")},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:    keys,
					collect: records,
					summary: summary,
				}},
			expectedSessionConfig: SessionConfig{ImpersonatedUser: "jane", BookmarkManager: defaultBookmarkManager},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description: "returns expected result of assumed write query targeting database",
			configurers: []ExecuteQueryConfigurationOption{WithDatabase("imdb")},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:    keys,
					collect: records,
					summary: summary,
				}},
			expectedSessionConfig: SessionConfig{DatabaseName: "imdb", BookmarkManager: defaultBookmarkManager},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description: "returns expected result of assumed write query with custom bookmark manager",
			configurers: []ExecuteQueryConfigurationOption{WithBookmarkManager(customBookmarkManager)},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:    keys,
					collect: records,
					summary: summary,
				}},
			expectedSessionConfig: SessionConfig{BookmarkManager: customBookmarkManager},
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description: "returns expected result of explicit write query",
			configurers: []ExecuteQueryConfigurationOption{WithWritersRouting()},
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:    keys,
					collect: records,
					summary: summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description: "returns expected result of explicit read query",
			configurers: []ExecuteQueryConfigurationOption{WithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					keys:    keys,
					collect: records,
					summary: summary,
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedResult: &EagerResult{
				Keys:    keys,
				Records: records,
				Summary: summary,
			},
		},
		{
			description: "returns error when write result keys cannot be retrieved",
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keysErr: fmt.Errorf("dude, where are my keys"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("dude, where are my keys"),
		},
		{
			description: "returns error when write result records cannot be collected",
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:       keys,
					collectErr: fmt.Errorf("one does not simply collect"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("one does not simply collect"),
		},
		{
			description: "returns error when write result summary cannot be retrieved",
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:       keys,
					collect:    records,
					summaryErr: fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description: "returns error when write result summary cannot be retrieved",
			createSession: &fakeSession{
				executeWriteTransactionResult: &fakeResult{
					keys:       keys,
					collect:    records,
					summaryErr: fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description: "returns error when write execution fails",
			createSession: &fakeSession{
				executeWriteErr: fmt.Errorf("oopsie"),
			},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("oopsie"),
		},
		{
			description: "returns error when read result keys cannot be retrieved",
			configurers: []ExecuteQueryConfigurationOption{WithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					keysErr: fmt.Errorf("dude, where are my keys"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("dude, where are my keys"),
		},
		{
			description: "returns error when read result records cannot be collected",
			configurers: []ExecuteQueryConfigurationOption{WithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					keys:       keys,
					collectErr: fmt.Errorf("one does not simply collect"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("one does not simply collect"),
		},
		{
			description: "returns error when read result summary cannot be retrieved",
			configurers: []ExecuteQueryConfigurationOption{WithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					keys:       keys,
					collect:    records,
					summaryErr: fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description: "returns error when read result summary cannot be retrieved",
			configurers: []ExecuteQueryConfigurationOption{WithReadersRouting()},
			createSession: &fakeSession{
				executeReadTransactionResult: &fakeResult{
					keys:       keys,
					collect:    records,
					summaryErr: fmt.Errorf("in summary: nope"),
				}},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("in summary: nope"),
		},
		{
			description: "returns error when read execution fails",
			configurers: []ExecuteQueryConfigurationOption{WithReadersRouting()},
			createSession: &fakeSession{
				executeReadErr: fmt.Errorf("oopsie"),
			},
			expectedSessionConfig: defaultSessionConfig,
			expectedErr:           fmt.Errorf("oopsie"),
		},
	}

	for _, testCase := range testCases {
		outer.Run(testCase.description, func(t *testing.T) {
			driver := &driverWithContext{
				newSession: func(_ context.Context, config SessionConfig) SessionWithContext {
					AssertDeepEquals(t, testCase.expectedSessionConfig, config)
					return testCase.createSession
				},
				defaultManagedSessionBookmarkManager: defaultBookmarkManager,
			}

			eagerResult, err := driver.ExecuteQuery(ctx, "RETURN 42", nil, testCase.configurers...)

			AssertDeepEquals(t, testCase.expectedErr, err)
			AssertDeepEquals(t, testCase.expectedResult, eagerResult)
		})
	}
}

type fakeSession struct {
	executeReadTransactionResult  *fakeResult
	executeReadErr                error
	executeWriteTransactionResult *fakeResult
	executeWriteErr               error
	closeErr                      error
}

func (s *fakeSession) LastBookmarks() Bookmarks {
	panic("implement me")
}

func (s *fakeSession) lastBookmark() string {
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
	return callback(&fakeManagedTransaction{
		result: s.executeWriteTransactionResult,
		err:    s.executeWriteErr,
	})
}

func (s *fakeSession) Run(context.Context, string, map[string]any, ...func(*TransactionConfig)) (ResultWithContext, error) {
	panic("implement me")
}

func (s *fakeSession) Close(context.Context) error {
	return s.closeErr
}

func (s *fakeSession) legacy() Session {
	panic("implement me")
}

func (s *fakeSession) getServerInfo(context.Context) (ServerInfo, error) {
	panic("implement me")
}

type fakeManagedTransaction struct {
	result *fakeResult
	err    error
}

func (tx *fakeManagedTransaction) Run(context.Context, string, map[string]any) (ResultWithContext, error) {
	return tx.result, tx.err
}

func (tx *fakeManagedTransaction) legacy() Transaction {
	panic("implement me")
}

type fakeResult struct {
	keys       []string
	keysErr    error
	collect    []*Record
	collectErr error
	summary    ResultSummary
	summaryErr error
}

func (f *fakeResult) Keys() ([]string, error) {
	return f.keys, f.keysErr
}

func (f *fakeResult) NextRecord(context.Context, **Record) bool {
	panic("implement me")
}

func (f *fakeResult) Next(context.Context) bool {
	panic("implement me")
}

func (f *fakeResult) PeekRecord(context.Context, **Record) bool {
	panic("implement me")
}

func (f *fakeResult) Peek(context.Context) bool {
	panic("implement me")
}

func (f *fakeResult) Err() error {
	panic("implement me")
}

func (f *fakeResult) Record() *Record {
	panic("implement me")
}

func (f *fakeResult) Collect(context.Context) ([]*Record, error) {
	return f.collect, f.collectErr
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

func (f *fakeResult) legacy() Result {
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

func (sum *fakeSummary) ResultAvailableAfter() time.Duration {
	return sum.resultAvailableAfter
}

func (sum *fakeSummary) ResultConsumedAfter() time.Duration {
	panic("implement me")
}

func (sum *fakeSummary) Database() DatabaseInfo {
	panic("implement me")
}
