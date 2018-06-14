package neo4j_go_driver

type Transaction struct {
	session *Session
}

type TransactionWork func(transaction *Transaction) (interface{}, error)

func (transaction *Transaction) Success() error {
	return nil
}

func (transaction *Transaction) Failure() error {
	return nil
}

func (transaction *Transaction) Close() error {
	return nil
}

func (transaction *Transaction) Run(cypher string) (*Result, error) {
	return transaction.RunWithParams(cypher, &map[string]interface{}{})
}

func (transaction *Transaction) RunWithParams(cypher string, params *map[string]interface{}) (*Result, error) {
	return nil, nil
}
