package issue_451_test

import (
	"context"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/issue_451"
	"sync"
	"testing"
)

type DriverTestSuite struct {
	driver *issue_451.WrappedDriver
	ctx    context.Context
	t      *testing.T
}

func TestIssue451(t *testing.T) {
	uri := "neo4j://localhost"
	user := "neo4j"
	password := "letmein!"
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, password, ""))
	if err != nil {
		t.Fatal(err)
	}
	suite := &DriverTestSuite{
		driver: &issue_451.WrappedDriver{
			DbUri:    uri,
			User:     user,
			Password: password,
			Driver:   driver,
		},
		ctx: context.Background(),
		t:   t,
	}
	suite.TestMultithreadedQueryRequestsWithConnectionRecovery()
}

func (s *DriverTestSuite) TestMultithreadedQueryRequestsWithConnectionRecovery() {
	count := 100
	wg := &sync.WaitGroup{}
	wg.Add(count)

	for i := 0; i < count; i++ {

		go func(wg *sync.WaitGroup, i int, s *DriverTestSuite) {
			defer wg.Done()
			err := s.executeSimpleQuery()
			testutil.AssertNoError(s.t, err)

		}(wg, i, s)
	}
	wg.Wait()

}

func executeSimpleQuery(ctx context.Context, driver *issue_451.WrappedDriver) error {
	return driver.ExecuteQuery(ctx, "CREATE (test:Test) return true", map[string]interface{}{}, func(result neo4j.ResultWithContext) error {
		var record *neo4j.Record
		result.NextRecord(ctx, &record)
		if record == nil || len(record.Values) == 0 {
			return errors.New("no records")
		}
		_, ok := record.Values[0].(bool)
		if !ok {
			return errors.New("expected value to be bool")
		}
		return nil
	})
}

func (s *DriverTestSuite) executeSimpleQuery() error {
	return executeSimpleQuery(s.ctx, s.driver)
}
