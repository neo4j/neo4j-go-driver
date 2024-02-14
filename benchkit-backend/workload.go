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

package main

import (
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"strconv"
	"sync"
)

type workloads struct {
	id        int
	workloads map[string]*workload
}

type workload struct {
	Method     *string          `json:"method,omitempty"`
	Queries    []workloadQuery  `json:"queries,omitempty"`
	Database   *string          `json:"database,omitempty"`
	Routing    *string          `json:"routing,omitempty"` // Temporarily store AccessMode as string
	Mode       *string          `json:"mode,omitempty"`
	AccessMode neo4j.AccessMode `json:"-"` // Ignore during JSON decoding

}

type workloadQuery struct {
	Text       string                 `json:"text"`
	Parameters map[string]interface{} `json:"parameters"`
}

const (
	executeQueryParallelSessions       = "executeQuery_parallelSessions"
	executeQuerySequentialSessions     = "executeQuery_sequentialSessions"
	sessionRunParallelSessions         = "sessionRun_parallelSessions"
	sessionRunSequentialSessions       = "sessionRun_sequentialSessions"
	sessionRunSequentialTransactions   = "sessionRun_sequentialTransactions"
	executeReadParallelSessions        = "executeRead_parallelSessions"
	executeReadSequentialSessions      = "executeRead_sequentialSessions"
	executeReadSequentialTransactions  = "executeRead_sequentialTransactions"
	executeReadSequentialQueries       = "executeRead_sequentialQueries"
	executeWriteParallelSessions       = "executeWrite_parallelSessions"
	executeWriteSequentialSessions     = "executeWrite_sequentialSessions"
	executeWriteSequentialTransactions = "executeWrite_sequentialTransactions"
	executeWriteSequentialQueries      = "executeWrite_sequentialQueries"
)

// newWorkload creates a new workload instance with proper initialization and validation.
func newWorkload(method *string, queries []workloadQuery, database *string, routing *string, mode *string) (*workload, error) {
	wl := &workload{
		Method:   method,
		Queries:  queries,
		Database: database,
		Routing:  routing,
		Mode:     mode,
	}

	// Default database to an empty string if nil
	if wl.Database == nil {
		defaultDB := ""
		wl.Database = &defaultDB
	}

	// Convert routing to AccessMode
	if err := wl.convertRoutingToAccessMode(); err != nil {
		return nil, err
	}
	return wl, nil
}

func (w *workloads) store(wl *workload) string {
	if w.workloads == nil {
		w.id = 0
		w.workloads = make(map[string]*workload)
	}
	w.id++
	workloadId := strconv.Itoa(w.id)
	w.workloads[workloadId] = wl
	return workloadId
}

func (w *workloads) fetch(workloadId string) (*workload, bool) {
	value, ok := w.workloads[workloadId]
	return value, ok
}

func (w *workloads) delete(workloadId string) bool {
	_, ok := w.workloads[workloadId]
	delete(w.workloads, workloadId)
	return ok
}

func (w *workload) patch(update *workload) error {
	if update.Method != nil {
		w.Method = update.Method
	}
	if len(update.Queries) > 0 {
		w.Queries = update.Queries
	}
	if update.Database != nil {
		w.Database = update.Database
	}
	if update.Routing != nil {
		w.Routing = update.Routing
	}
	if update.Mode != nil {
		w.Mode = update.Mode
	}
	return w.convertRoutingToAccessMode()
}

func (w *workload) convertRoutingToAccessMode() error {
	if w.Routing == nil {
		w.AccessMode = neo4j.AccessModeWrite // Default to write if not specified
		return nil
	}

	switch *w.Routing {
	case "write":
		w.AccessMode = neo4j.AccessModeWrite
	case "read":
		w.AccessMode = neo4j.AccessModeRead
	default:
		return fmt.Errorf("invalid routing value '%s'", *w.Routing)
	}
	return nil
}

func (w *workload) execute(driver neo4j.DriverWithContext) error {
	if driver == nil {
		return errors.New("uninitialized driver: call /ready first")
	}
	if w.Method == nil || w.Mode == nil {
		return errors.New("method or mode is nil")
	}

	executionStrategy := fmt.Sprintf("%s_%s", *w.Method, *w.Mode)

	switch executionStrategy {
	case executeQueryParallelSessions:
		return w.executeQueryParallelSessions(driver)
	case executeQuerySequentialSessions:
		return w.executeQuerySequentialSessions(driver)
	case sessionRunParallelSessions:
		return w.sessionRunParallelSessions(driver)
	case sessionRunSequentialSessions:
		return w.sessionRunSequentialSessions(driver)
	case sessionRunSequentialTransactions:
		return w.sessionRunSequentialTransactions(driver)
	case executeReadParallelSessions:
		return w.executeReadParallelSessions(driver)
	case executeReadSequentialSessions:
		return w.executeReadSequentialSessions(driver)
	case executeReadSequentialTransactions:
		return w.executeReadSequentialTransactions(driver)
	case executeReadSequentialQueries:
		return w.executeReadSequentialQueries(driver)
	case executeWriteParallelSessions:
		return w.executeWriteParallelSessions(driver)
	case executeWriteSequentialSessions:
		return w.executeWriteSequentialSessions(driver)
	case executeWriteSequentialTransactions:
		return w.executeWriteSequentialTransactions(driver)
	case executeWriteSequentialQueries:
		return w.executeWriteSequentialQueries(driver)
	default:
		return fmt.Errorf("unsupported combination of method and mode: %s", executionStrategy)
	}
}

// executeParallel abstracts the common pattern of executing tasks in parallel.
// It takes a slice of workloadQuery and a function that defines how to execute each query.
func (w *workload) executeParallel(
	driver neo4j.DriverWithContext,
	queries []workloadQuery,
	executeFunc func(neo4j.DriverWithContext, workloadQuery) error) error {

	// A wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// An error channel to capture errors from goroutines
	errChan := make(chan error, len(queries))

	// Execute each query in its own goroutine
	for _, query := range queries {
		wg.Add(1)
		go func(q workloadQuery) {
			defer wg.Done() // Ensure the wait group is marked as done after the goroutine completes
			// Execute the query using the supplied executeFunc
			if err := executeFunc(driver, q); err != nil {
				errChan <- err
			}
		}(query)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan) // Close the error channel after all goroutines have finished

	// Check for errors sent to the error channel
	for err := range errChan {
		if err != nil {
			return err // Return the first error encountered
		}
	}
	return nil
}

func (w *workload) executeQuery(driver neo4j.DriverWithContext, query workloadQuery) error {
	// Prepare a slice to hold the configuration options
	var opts []neo4j.ExecuteQueryConfigurationOption

	// Append the database configuration option
	opts = append(opts, neo4j.ExecuteQueryWithDatabase(*w.Database))

	// Conditionally append the routing configuration option based on AccessMode
	if w.AccessMode == neo4j.AccessModeRead {
		opts = append(opts, neo4j.ExecuteQueryWithReadersRouting())
	} else if w.AccessMode == neo4j.AccessModeWrite {
		opts = append(opts, neo4j.ExecuteQueryWithWritersRouting())
	}

	_, err := neo4j.ExecuteQuery(ctx, driver, query.Text, query.Parameters, neo4j.EagerResultTransformer, opts...)
	if err != nil {
		return err
	}
	return nil
}

func (w *workload) executeQueryParallelSessions(driver neo4j.DriverWithContext) error {
	return w.executeParallel(driver, w.Queries, w.executeQuery)
}

func (w *workload) executeQuerySequentialSessions(driver neo4j.DriverWithContext) error {
	for _, query := range w.Queries {
		err := w.executeQuery(driver, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) createSession(driver neo4j.DriverWithContext) neo4j.SessionWithContext {
	sessionConfig := neo4j.SessionConfig{
		AccessMode:   w.AccessMode,
		DatabaseName: *w.Database,
	}
	return driver.NewSession(ctx, sessionConfig)
}

func (w *workload) sessionRun(session neo4j.SessionWithContext, query workloadQuery) error {
	result, err := session.Run(ctx, query.Text, query.Parameters)
	if err != nil {
		return err
	}

	// Consume the results to ensure they are fetched and processed
	_, err = result.Consume(ctx)
	return err
}

func (w *workload) sessionRunParallelSessions(driver neo4j.DriverWithContext) error {
	// Define how to execute a session run in parallel
	sessionRunFunc := func(driver neo4j.DriverWithContext, query workloadQuery) error {
		session := w.createSession(driver)
		defer session.Close(ctx)
		return w.sessionRun(session, query)
	}
	return w.executeParallel(driver, w.Queries, sessionRunFunc)
}

func (w *workload) sessionRunSequentialSessions(driver neo4j.DriverWithContext) error {
	for _, query := range w.Queries {
		session := w.createSession(driver)
		err := w.sessionRun(session, query)
		session.Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) sessionRunSequentialTransactions(driver neo4j.DriverWithContext) error {
	session := w.createSession(driver)
	defer session.Close(ctx)

	for _, query := range w.Queries {
		err := w.sessionRun(session, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) executeRead(session neo4j.SessionWithContext, query workloadQuery) error {
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query.Text, query.Parameters)
		if err != nil {
			return nil, err
		}
		// Consume the results to ensure they are fetched and processed
		return result.Consume(ctx)
	})
	return err
}

func (w *workload) executeReadParallelSessions(driver neo4j.DriverWithContext) error {
	// Define how to execute an execute read in parallel
	executeReadFunc := func(driver neo4j.DriverWithContext, query workloadQuery) error {
		session := w.createSession(driver)
		defer session.Close(ctx)
		return w.executeRead(session, query)
	}
	return w.executeParallel(driver, w.Queries, executeReadFunc)
}

func (w *workload) executeReadSequentialSessions(driver neo4j.DriverWithContext) error {
	for _, query := range w.Queries {
		session := w.createSession(driver)
		err := w.executeRead(session, query)
		session.Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) executeReadSequentialTransactions(driver neo4j.DriverWithContext) error {
	session := w.createSession(driver)
	defer session.Close(ctx)

	for _, query := range w.Queries {
		err := w.executeRead(session, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) executeReadSequentialQueries(driver neo4j.DriverWithContext) error {
	session := w.createSession(driver)
	defer session.Close(ctx)

	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var err error
		for _, query := range w.Queries {
			result, err := tx.Run(ctx, query.Text, query.Parameters)
			if err != nil {
				return nil, err
			}
			// Consume the results to ensure they are fetched and processed
			_, err = result.Consume(ctx)
			if err != nil {
				return nil, err
			}
		}
		return nil, err
	})
	return err
}

func (w *workload) executeWrite(session neo4j.SessionWithContext, query workloadQuery) error {
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query.Text, query.Parameters)
		if err != nil {
			return nil, err
		}
		// Consume the results to ensure they are fetched and processed
		return result.Consume(ctx)
	})
	return err
}

func (w *workload) executeWriteParallelSessions(driver neo4j.DriverWithContext) error {
	// Define how to execute an execute write in parallel
	executeWriteFunc := func(driver neo4j.DriverWithContext, query workloadQuery) error {
		session := w.createSession(driver)
		defer session.Close(ctx)
		return w.executeWrite(session, query)
	}
	return w.executeParallel(driver, w.Queries, executeWriteFunc)
}

func (w *workload) executeWriteSequentialSessions(driver neo4j.DriverWithContext) error {
	for _, query := range w.Queries {
		session := w.createSession(driver)
		err := w.executeWrite(session, query)
		session.Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) executeWriteSequentialTransactions(driver neo4j.DriverWithContext) error {
	session := w.createSession(driver)
	defer session.Close(ctx)

	for _, query := range w.Queries {
		err := w.executeRead(session, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *workload) executeWriteSequentialQueries(driver neo4j.DriverWithContext) error {
	session := w.createSession(driver)
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var err error
		for _, query := range w.Queries {
			result, err := tx.Run(ctx, query.Text, query.Parameters)
			if err != nil {
				return nil, err
			}
			// Consume the results to ensure they are fetched and processed
			_, err = result.Consume(ctx)
			if err != nil {
				return nil, err
			}
		}
		return nil, err
	})
	return err
}
