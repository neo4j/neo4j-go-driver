/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
	"errors"
)

// Transaction represents a transaction in the Neo4j database
type Transaction struct {
	session        *Session
	outcomeApplied bool
	beginResult    *Result
}

// TransactionWork represents a unit of work that will be executed against the provided
// transaction
type TransactionWork func(transaction *Transaction) (interface{}, error)

func (transaction *Transaction) ensureState() error {
	if transaction.outcomeApplied {
		return errors.New("transaction is already committed or rolled back")
	}

	if transaction.session.runner == nil {
		return errors.New("transaction is closed")
	}

	return nil
}

// Commit commits the transaction
func (transaction *Transaction) Commit() error {
	if err := transaction.ensureState(); err != nil {
		return err
	}

	commit, err := transaction.session.runner.commitTransaction()
	if err != nil {
		return err
	}

	_, err = commit.Consume()
	if err != nil {
		return err
	}

	transaction.outcomeApplied = true

	return nil
}

// Rollback rolls back the transaction
func (transaction *Transaction) Rollback() error {
	if err := transaction.ensureState(); err != nil {
		return err
	}

	rollback, err := transaction.session.runner.rollbackTransaction()
	if err != nil {
		return err
	}

	_, err = rollback.Consume()
	if err != nil {
		return err
	}

	transaction.outcomeApplied = true

	return nil
}

// Close rolls back the actual transaction if it's not already committed/rolled back
// and closes all resources associated with this transaction
func (transaction *Transaction) Close() error {
	if !transaction.outcomeApplied {
		if err := transaction.Rollback(); err != nil {
			return err
		}
	}

	if err := closeRunner(transaction.session); err != nil {
		return err
	}

	transaction.session.tx = nil

	return nil
}

// Run executes a statement on this transaction and returns a result
func (transaction *Transaction) Run(cypher string, params *map[string]interface{}) (*Result, error) {
	return transaction.runStatement(&Statement{cypher: cypher, params: params})
}

func (transaction *Transaction) runStatement(statement *Statement) (*Result, error) {
	if err := statement.validate(); err != nil {
		return nil, err
	}

	if err := transaction.ensureState(); err != nil {
		return nil, err
	}

	result, err := transaction.session.runner.runStatement(*statement)
	if err != nil {
		return nil, err
	}

	return result, nil
}
