package neo4j_go_driver

import (
	"neo4j-go-connector/pkg"
	"errors"
)

type Session struct {
	driver         Driver
	accessMode     AccessMode
	bookmarks      []string
	connection     neo4j.Connection
	pendingResults []*Result
}

func newSession(driver Driver, accessMode AccessMode, bookmarks []string) *Session {
	return &Session{
		driver:         driver,
		accessMode:     accessMode,
		bookmarks:      bookmarks,
		pendingResults: nil,
	}
}

func (session *Session) BeginTransaction() (*Transaction, error) {
	return nil, nil
}

func (session *Session) ReadTransaction(work TransactionWork) (interface{}, error) {
	return nil, nil
}

func (session *Session) WriteTransaction(work TransactionWork) (interface{}, error) {
	return nil, nil
}

func (session *Session) Run(cypher string) (*Result, error) {
	return session.RunStatement(NewStatement(cypher))
}

func (session *Session) RunWithParams(cypher string, params *map[string]interface{}) (*Result, error) {
	return session.RunStatement(NewStatementWithParams(cypher, params))
}

func (session *Session) RunStatement(statement *Statement) (*Result, error) {
	if len(statement.cypher) == 0 {
		return nil, errors.New("cypher statement should not be empty")
	}

	if session.connection == nil {
		connection, err := session.driver.acquire(session.accessMode)
		if err != nil {
			return nil, err
		}

		session.connection = connection
	}

	runHandle, err := session.connection.Run(statement.cypher, statement.params)
	if err != nil {
		defer cleanSession(session)

		return nil, err
	}
	pullAllHandle, err := session.connection.PullAll()
	if err != nil {
		defer cleanSession(session)

		return nil, err
	}

	err = session.connection.Flush()
	if err != nil {
		defer cleanSession(session)

		return nil, err
	}

	result := &Result{
		session:       session,
		runHandle:     runHandle,
		pullAllHandle: pullAllHandle,
	}

	session.pendingResults = append(session.pendingResults, result)

	return result, nil
}

func (session *Session) Close() error {
	return cleanSession(session)
}

func cleanSession(session *Session) error {
	if session != nil && session.connection != nil {
		err := session.connection.Close()
		session.connection = nil
		return err
	}

	return nil
}
