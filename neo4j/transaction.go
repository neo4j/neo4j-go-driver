/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// Transaction represents a transaction in the Neo4j database
type Transaction interface {
	// Run executes a statement on this transaction and returns a result
	Run(cypher string, params map[string]interface{}) (Result, error)
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
	// Close rolls back the actual transaction if it's not already committed/rolled back
	// and closes all resources associated with this transaction
	Close() error
}

type transaction struct {
	done     bool
	run      func(cypher string, params map[string]interface{}) (Result, error)
	commit   func() error
	rollback func() error
}

func (t *transaction) Run(cypher string, params map[string]interface{}) (Result, error) {
	return t.run(cypher, params)
}

func (t *transaction) Commit() error {
	err := t.commit()
	t.done = err == nil
	return err
}

func (t *transaction) Rollback() error {
	err := t.rollback()
	t.done = err == nil
	return err
}

func (t *transaction) Close() error {
	if t.done {
		return nil
	}
	return t.rollback()
}
