package neo4j_go_driver

import (
	"neo4j-go-connector/pkg"
	"errors"
)

type Result struct {
	keys             []string
	records          []Record
	session          *Session
	err              error
	runHandle        neo4j.RequestHandle
	runCompleted     bool
	pullAllHandle    neo4j.RequestHandle
	pullAllCompleted bool
}

func receive(session *Session) (*Result, error) {
	if len(session.pendingResults) <= 0 {
		return nil, errors.New("unexpected state: no pending results registered on session")
	}

	activeResult := session.pendingResults[0]
	if !activeResult.runCompleted {
		received, err := session.connection.Fetch(activeResult.runHandle)
		if err != nil {
			return nil, err
		}

		if received != neo4j.METADATA {
			return nil, errors.New("unexpected response received while waiting for a METADATA")
		}

		err = collectMetadata(session, activeResult)
		if err != nil {
			return nil, err
		}
	} else if !activeResult.pullAllCompleted {
		received, err := session.connection.Fetch(activeResult.pullAllHandle)
		if err != nil {
			return nil, err
		}

		switch received {
		case neo4j.METADATA:
			err := collectMetadata(session, activeResult)
			if err != nil {
				return nil, err
			}
		case neo4j.RECORD:
			err := collectRecord(session, activeResult)
			if err != nil {
				return nil, err
			}
		case neo4j.ERROR:
			return nil, errors.New("unable to fetch from connection")
		}
	}

	return activeResult, nil
}

func collectMetadata(session *Session, result *Result) error {
	metadata, err := session.connection.Metadata()
	if err != nil {
		return err
	}

	if fields, ok := metadata["fields"]; ok {
		result.keys = fields.([]string)
		result.records = make([]Record, 0)

		result.runCompleted = true
	}

	return nil
}

func collectRecord(session *Session, result *Result) error {
	data, err := session.connection.Data()
	if err != nil {
		return err
	}

	result.records = append(result.records, Record{keys: result.keys, values: data})

	return nil
}

func (result *Result) Keys() ([]string, error) {
	for result.keys == nil {
		_, err := receive(result.session)
		if err != nil {
			return nil, err
		}
	}

	return result.keys, nil
}

func (result *Result) Next() bool {
	for result.records == nil {
		_, err := receive(result.session)
		if err != nil {
			result.err = err

			return false
		}
	}

	if !result.pullAllCompleted && len(result.records) == 0 {
		_, err := receive(result.session)
		if err != nil {
			result.err = err

			return false
		}
	}

	return len(result.records) > 0
}

func (result *Result) Record() *Record {
	if len(result.records) > 0 {
		record := result.records[0]
		result.records = result.records[1:]
		return &record
	}

	return nil
}

func (result *Result) Peek() *Record {
	return nil
}

func (result *Result) Summary() (*ResultSummary, error) {
	return nil, nil
}

func (result *Result) Consume() (*ResultSummary, error) {
	return nil, nil
}

func (result *Result) Err() error {
	return result.err
}
