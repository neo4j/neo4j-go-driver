package neo4j_go_driver

import (
    "neo4j-go-connector/pkg"
)

type Result struct {
    keys            []string
    records         []Record
    current         *Record
    summary         ResultSummary
    runner          *statementRunner
    err             error
    runHandle       neo4j.RequestHandle
    runCompleted    bool
    resultHandle    neo4j.RequestHandle
    resultCompleted bool
}

func (result *Result) collectMetadata(metadata map[string]interface{}) {
    if metadata != nil {
        if fields, ok := metadata["fields"]; ok {
            result.keys = fields.([]string)
        }
    }
}

func (result *Result) collectRecord(fields []interface{}) {
    if fields != nil {
        result.records = append(result.records, Record{keys: result.keys, values: fields})
    }
}

func (result *Result) Keys() ([]string, error) {
    for !result.runCompleted {
        _, err := result.runner.receive()
        if err != nil {
            return nil, err
        }
    }

    return result.keys, nil
}

func (result *Result) Next() bool {
    for !result.runCompleted {
        _, err := result.runner.receive()
        if err != nil {
            result.err = err

            return false
        }
    }

    if !result.resultCompleted && len(result.records) == 0 {
        _, err := result.runner.receive()
        if err != nil {
            result.err = err

            return false
        }
    }

    if len(result.records) > 0 {
        result.current = &result.records[0]
        result.records = result.records[1:]
    } else {
        result.current = nil
    }

    return result.current != nil
}

func (result *Result) Err() error {
    return result.err
}

func (result *Result) Record() *Record {
    return result.current
}

func (result *Result) Summary() (*ResultSummary, error) {
    for !result.resultCompleted {
        _, err := result.runner.receive()
        if err != nil {
            return nil, err
        }
    }

    return &result.summary, nil
}

func (result *Result) Consume() (*ResultSummary, error) {
    for !result.resultCompleted {
        _, err := result.runner.receive()
        if err != nil {
            return nil, err
        }
    }

    return &result.summary, nil
}
