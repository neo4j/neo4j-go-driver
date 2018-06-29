package neo4j_go_driver

import (
	"errors"
)

type Transaction struct {
	session *Session
	outcomeApplied bool
	beginResult *Result
}

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

func (transaction *Transaction) Close() error {
	if !transaction.outcomeApplied {
		if err := transaction.Rollback(); err != nil {
			return err
		}
	}

	if err := transaction.session.closeRunner(); err != nil {
		return err
	}

	return nil
}

func (transaction *Transaction) Run(cypher string) (*Result, error) {
	return transaction.runStatement(NewStatement(cypher))
}

func (transaction *Transaction) RunWithParams(cypher string, params *map[string]interface{}) (*Result, error) {
	return transaction.runStatement(NewStatementWithParams(cypher, params))
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