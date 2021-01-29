/*
 * Copyright (c) "Neo4j"
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

package neo4j

import (
	"time"

	"github.com/neo4j-drivers/gobolt"
)

type neoResult struct {
	keys            []string
	records         []Record
	current         Record
	summary         *neoResultSummary
	runner          *statementRunner
	err             error
	runHandle       gobolt.RequestHandle
	runCompleted    bool
	resultHandle    gobolt.RequestHandle
	resultCompleted bool
}

var collectMetadata = func(result *neoResult, metadata map[string]interface{}) {
	if metadata != nil {
		if resultAvailabilityTimer, ok := metadata["result_available_after"]; ok {
			result.summary.resultAvailableAfter = time.Duration(resultAvailabilityTimer.(int64)) / time.Millisecond
		}

		if resultAvailabilityTimer, ok := metadata["t_first"]; ok {
			result.summary.resultAvailableAfter = time.Duration(resultAvailabilityTimer.(int64)) / time.Millisecond
		}

		if resultConsumptionTimer, ok := metadata["result_consumed_after"]; ok {
			result.summary.resultConsumedAfter = time.Duration(resultConsumptionTimer.(int64)) / time.Millisecond
		}

		if resultConsumptionTimer, ok := metadata["t_last"]; ok {
			result.summary.resultConsumedAfter = time.Duration(resultConsumptionTimer.(int64)) / time.Millisecond
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
				result.summary.statementType = StatementTypeUnknown
			}
		}

		if stats, ok := metadata["stats"]; ok {
			if statsDict, ok := stats.(map[string]interface{}); ok {
				result.summary.counters = collectCounters(&statsDict)
			}
		}

		if plan, ok := metadata["plan"]; ok {
			if plansDict, ok := plan.(map[string]interface{}); ok {
				result.summary.plan = collectPlan(&plansDict)
			}
		}

		if profile, ok := metadata["profile"]; ok {
			if profileDict, ok := profile.(map[string]interface{}); ok {
				result.summary.profile = collectProfile(&profileDict)
			}
		}

		if notifications, ok := metadata["notifications"]; ok {
			if notificationsList, ok := notifications.([]interface{}); ok {
				collectNotification(&notificationsList, &result.summary.notifications)
			}
		}
	}
}

var collectRecord = func(result *neoResult, fields []interface{}) {
	if fields != nil {
		result.records = append(result.records, &neoRecord{keys: result.keys, values: fields})
	}
}

func (result *neoResult) Keys() ([]string, error) {
	for !result.runCompleted {
		if currentResult, err := result.runner.receive(); currentResult == result && err != nil {
			return nil, err
		}
	}

	return result.keys, nil
}

func (result *neoResult) Next() bool {
	if result.err != nil {
		return false
	}

	for !result.runCompleted {
		if currentResult, err := result.runner.receive(); currentResult == result && err != nil {
			return false
		}
	}

	for !result.resultCompleted && len(result.records) == 0 {
		if currentResult, err := result.runner.receive(); currentResult == result && err != nil {
			return false
		}
	}

	if len(result.records) > 0 {
		result.current = result.records[0]
		result.records = result.records[1:]
	} else {
		result.current = nil
	}

	return result.current != nil
}

func (result *neoResult) Err() error {
	return result.err
}

func (result *neoResult) Record() Record {
	return result.current
}

func (result *neoResult) Summary() (ResultSummary, error) {
	for result.err == nil && !result.resultCompleted {
		if _, err := result.runner.receive(); err != nil {
			result.err = err

			break
		}
	}

	if result.err != nil {
		return nil, result.err
	}

	return result.summary, nil
}

func (result *neoResult) Consume() (ResultSummary, error) {
	for result.Next() {

	}

	if result.err != nil {
		return nil, result.err
	}

	return result.summary, nil
}
