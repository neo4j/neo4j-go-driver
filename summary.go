package neo4j_go_driver

type StatementType int

const (
	ReadOnly    StatementType = 0
	ReadWrite                 = 1
	WriteOnly                 = 2
	SchemaWrite               = 3
)

type ServerInfo struct {
	address string
	version string
}

type InputPosition struct {
	offset int
	line   int
	column int
}

type Notification struct {
	code        string
	title       string
	description string
	position    InputPosition
	severity    string
}

type Counters struct {
}

type Plan struct {
}

type Profile struct {
}

type ResultSummary struct {
	statement             Statement
	counters              *Counters
	statementType         StatementType
	plan                  *Plan
	profile               *Profile
	notifications         *[]Notification
	resultsAvailableAfter int64
	resultsConsumedAfter  int64
	server                ServerInfo
}
