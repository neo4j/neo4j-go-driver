package neo4j_go_driver

type StatementType int

const (
	StatementTypeReadOnly    StatementType = 0
	StatementTypeReadWrite                 = 1
	StatementTypeWriteOnly                 = 2
	StatementTypeSchemaWrite               = 3
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
