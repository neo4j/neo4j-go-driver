module main

go 1.24

replace github.com/neo4j/neo4j-go-driver/v6 => ../

require (
	github.com/neo4j/neo4j-go-driver v1.8.3
	github.com/neo4j/neo4j-go-driver/v6 v6.0.0-alpha.1
)
