package issue_451

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"os"
	"strings"
	"sync"
)

var accessLock sync.RWMutex
var recoveryLock sync.Mutex

type WrappedDriver struct {
	DbUri    string
	User     string
	Password string
	Driver   neo4j.DriverWithContext
}

// ResultsHookFn allows the caller to parse the query results safely
type ResultsHookFn func(result neo4j.ResultWithContext) error

// ExecuteQuery runs a query an ensured connected Driver via Bolt. it it used with a hook of the original neo4j.Result object for a convenient usage
func (d *WrappedDriver) ExecuteQuery(ctx context.Context, query string, params map[string]interface{}, onResults ResultsHookFn) (err error) {
	accessLock.RLock()
	defer accessLock.RUnlock()
	return d.nonblockExecuteQuery(ctx, query, params, onResults)

}

// nonblockExecuteQuery makes sure that a recursive retry to execute a query doesn't create a more mutexes and thus a deadlock
// example is when a query executed, Rlock acquired, than Close function called, trying to aquire Lock, blocked, and then
// the function calls itself again for retry, trying to acquire Rlock, but is blocked by Lock that is blocked by previous Rlock
func (d *WrappedDriver) nonblockExecuteQuery(ctx context.Context, query string, params map[string]interface{}, onResults ResultsHookFn) (err error) {

	session, err := d.NewSession(ctx)
	if err != nil {
		return err
	}
	defer d.CloseSession(ctx, session)

	result, err := session.Run(ctx, query, params)
	if err != nil {
		if err.Error() == "Trying to create session on closed Driver" || strings.HasPrefix(err.Error(), "ConnectivityError") {
			err = d.reconnect(ctx)
			if err != nil {
				return err
			}
			return d.nonblockExecuteQuery(ctx, query, params, onResults)
		}
		return err
	}
	err = onResults(result) //<-- reporting metrics inside
	if err != nil {
		return err
	}
	return nil
}

// NewSession returns a new *connected* session only after ensuring the underlying connection is alive.
// it ensures liveliness by re-creating a new Driver in case of connectivity issues.
// it returns an error in case any connectivity issue could not be resolved even after re-creating the Driver.
func (d *WrappedDriver) NewSession(ctx context.Context) (neo4j.SessionWithContext, error) {
	return d.Driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}), nil
}

// CloseSession closes any open resources and marks this session as unusable.
// it wraps the original neo4j.Session.Close() func with af metrics and logs
func (d *WrappedDriver) CloseSession(ctx context.Context, session neo4j.SessionWithContext) {
	err := session.Close(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to close existing session: %s", err)
	}
}

// reconnect will create a new Driver if current one is not connected
// it uses double verification, as two queries might both get an error and try to reconnect, one will fix the connection
// the other doesn't need to reconnect
func (d *WrappedDriver) reconnect(ctx context.Context) error {
	recoveryLock.Lock()
	defer recoveryLock.Unlock()
	if err := d.Driver.VerifyConnectivity(ctx); err == nil {
		return nil

	}

	driver, err := neo4j.NewDriverWithContext(d.DbUri, neo4j.BasicAuth(d.User, d.Password, ""))
	if err != nil {
		return err
	}
	d.nonblockClose(ctx) //close old Driver
	d.Driver = driver
	return nil
}

// Close safely closes the underlying open connections to the DB.
func (d *WrappedDriver) Close(ctx context.Context) {
	accessLock.Lock()
	defer accessLock.Unlock()
	d.nonblockClose(ctx)
}

func (d *WrappedDriver) nonblockClose(ctx context.Context) {
	if d.Driver == nil {
		return
	}
	if err := d.Driver.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close existing Driver: %s", err)
	}
}
