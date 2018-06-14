package neo4j_go_driver

type Statement struct {
	cypher string
	params *map[string]interface{}
}

func NewStatement(cypher string) *Statement {
	return &Statement{cypher: cypher}
}

func NewStatementWithParams(cypher string, params *map[string]interface{}) *Statement {
	return &Statement{cypher: cypher, params: params}
}
