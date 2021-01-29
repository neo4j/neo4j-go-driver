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

type neoTransaction struct {
	session        *neoSession
	outcomeApplied bool
	beginResult    Result
}

// TransactionWork represents a unit of work that will be executed against the provided
// transaction
type TransactionWork func(tx Transaction) (interface{}, error)

func ensureTxState(transaction *neoTransaction) error {
	if transaction.outcomeApplied {
		return newDriverError("transaction is already committed or rolled back")
	}

	if transaction.session.runner == nil {
		return newDriverError("transaction is closed")
	}

	return nil
}

func (transaction *neoTransaction) Commit() error {
	if err := ensureTxState(transaction); err != nil {
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

func (transaction *neoTransaction) Rollback() error {
	if err := ensureTxState(transaction); err != nil {
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

func (transaction *neoTransaction) Close() error {
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

func (transaction *neoTransaction) Run(cypher string, params map[string]interface{}) (Result, error) {
	return runStatementOnTransaction(transaction, &neoStatement{text: cypher, params: params})
}

func runStatementOnTransaction(transaction *neoTransaction, statement *neoStatement) (Result, error) {
	if err := statement.validate(); err != nil {
		return nil, err
	}

	if err := ensureTxState(transaction); err != nil {
		return nil, err
	}

	result, err := transaction.session.runner.runStatement(statement, nil, TransactionConfig{})
	if err != nil {
		return nil, err
	}

	return result, nil
}
