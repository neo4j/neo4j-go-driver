/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j_go_driver

import (
    neo4j "neo4j-go-connector"
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

func extractIntValue(dict *map[string]interface{}, key string) int {
    if value, ok := (*dict)[key]; ok {
        return int(value.(int64))
    }

    return 0
}

func (result *Result) collectMetadata(metadata map[string]interface{}) {
    if metadata != nil {
        if fields, ok := metadata["fields"]; ok {
            result.keys = fields.([]string)
        }

        if resultAvailabilityTimer, ok := metadata["result_available_after"]; ok {
            result.summary.resultsAvailableAfter = resultAvailabilityTimer.(int64)
        }

        if resultConsumptionTimer, ok := metadata["result_consumed_after"]; ok {
            result.summary.resultsConsumedAfter = resultConsumptionTimer.(int64)
        }

        if typeString, ok := metadata["type"]; ok {
            switch typeString.(string) {
            case "r":
                result.summary.statementType = StatementTypeReadOnly
            case "rw":
                result.summary.statementType = StatementTypeReadWrite
            case "w":
                result.summary.statementType = StatementTypeWriteOnly
            case "s":
                result.summary.statementType = StatementTypeSchemaWrite
            default:
                // TODO: Shall we expose this via Result.err?
            }
        }

        if stats, ok := metadata["stats"]; ok {
            if statsDict, ok := stats.(map[string]interface{}); ok {
                result.summary.counters.nodesCreated = extractIntValue(&statsDict, "nodes-created")
                result.summary.counters.nodesDeleted = extractIntValue(&statsDict, "nodes-deleted")
                result.summary.counters.relationshipsCreated = extractIntValue(&statsDict, "relationships-created")
                result.summary.counters.relationshipsDeleted = extractIntValue(&statsDict, "relationships-deleted")
                result.summary.counters.propertiesSet = extractIntValue(&statsDict, "properties-set")
                result.summary.counters.labelsAdded = extractIntValue(&statsDict, "labels-added")
                result.summary.counters.labelsRemoved = extractIntValue(&statsDict, "labels-removed")
                result.summary.counters.indexesAdded = extractIntValue(&statsDict, "indexes-added")
                result.summary.counters.indexesRemoved = extractIntValue(&statsDict, "indexes-removed")
                result.summary.counters.constraintsAdded = extractIntValue(&statsDict, "constraints-added")
                result.summary.counters.constraintsRemoved = extractIntValue(&statsDict, "constraints-removed")
            }
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
