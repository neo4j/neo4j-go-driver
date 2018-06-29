package neo4j_go_driver

import "errors"

type Statement struct {
	cypher string
	params *map[string]interface{}
}

func (statement *Statement) validate() error {
	if len(statement.cypher) == 0 {
		return errors.New("cypher statement should not be empty")
	}

	return nil
}

func NewStatement(cypher string) *Statement {
	return &Statement{cypher: cypher}
}

func NewStatementWithParams(cypher string, params *map[string]interface{}) *Statement {
	return &Statement{cypher: cypher, params: params}
}
