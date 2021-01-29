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
	"fmt"
	"strconv"
)

func extractIntValue(dict *map[string]interface{}, key string, defaultValue int64) int64 {
	if dict == nil {
		return defaultValue
	}

	if value, ok := (*dict)[key]; ok {
		if valueAsLong, ok := value.(int64); ok {
			return valueAsLong
		}

		valueAsLong, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
		if err != nil {
			return defaultValue
		}

		return valueAsLong
	}

	return defaultValue
}

func extractStringValue(dict *map[string]interface{}, key string, defaultValue string) string {
	if dict == nil {
		return defaultValue
	}

	if value, ok := (*dict)[key]; ok {
		if valueAsStr, ok := value.(string); ok {
			return valueAsStr
		}

		if isNil(value) {
			return defaultValue
		}

		return fmt.Sprintf("%v", value)
	}

	return defaultValue
}

func collectCounters(dict *map[string]interface{}) Counters {
	counters := &neoCounters{}

	if dict != nil {
		counters.nodesCreated = int(extractIntValue(dict, "nodes-created", 0))
		counters.nodesDeleted = int(extractIntValue(dict, "nodes-deleted", 0))
		counters.relationshipsCreated = int(extractIntValue(dict, "relationships-created", 0))
		counters.relationshipsDeleted = int(extractIntValue(dict, "relationships-deleted", 0))
		counters.propertiesSet = int(extractIntValue(dict, "properties-set", 0))
		counters.labelsAdded = int(extractIntValue(dict, "labels-added", 0))
		counters.labelsRemoved = int(extractIntValue(dict, "labels-removed", 0))
		counters.indexesAdded = int(extractIntValue(dict, "indexes-added", 0))
		counters.indexesRemoved = int(extractIntValue(dict, "indexes-removed", 0))
		counters.constraintsAdded = int(extractIntValue(dict, "constraints-added", 0))
		counters.constraintsRemoved = int(extractIntValue(dict, "constraints-removed", 0))
	}

	return counters
}

func collectNotification(list *[]interface{}, target *[]Notification) {
	if list == nil {
		return
	}

	notificationsList := *list

	notifications := make([]Notification, len(notificationsList))
	for i, notification := range notificationsList {
		if notificationDict, ok := notification.(map[string]interface{}); ok {
			code := extractStringValue(&notificationDict, "code", "")
			title := extractStringValue(&notificationDict, "title", "")
			description := extractStringValue(&notificationDict, "description", "")
			severity := extractStringValue(&notificationDict, "severity", "")

			position := &neoInputPosition{}
			if positionData, ok := notificationDict["position"]; ok {
				if positionDict, ok := positionData.(map[string]interface{}); ok {
					position.offset = int(extractIntValue(&positionDict, "offset", 0))
					position.line = int(extractIntValue(&positionDict, "line", 0))
					position.column = int(extractIntValue(&positionDict, "column", 0))
				}
			}

			notifications[i] = &neoNotification{
				code:        code,
				title:       title,
				description: description,
				position:    position,
				severity:    severity,
			}
		}
	}

	*target = notifications
}

func collectPlan(dict *map[string]interface{}) Plan {
	var (
		operationType string
		args          map[string]interface{}
		identifiers   []string
		children      []Plan
	)

	if dict == nil {
		return nil
	}

	plansDict := *dict
	operationType = extractStringValue(dict, "operatorType", "")

	if argsValue, ok := plansDict["args"]; ok {
		args = argsValue.(map[string]interface{})
	}

	if identifiersValue, ok := plansDict["identifiers"]; ok {
		identifiersList := identifiersValue.([]interface{})
		identifiers = make([]string, len(identifiersList))
		for i, identifier := range identifiersList {
			identifiers[i] = identifier.(string)
		}
	}

	if childrenValue, ok := plansDict["children"]; ok {
		childrenList := childrenValue.([]interface{})
		children = make([]Plan, len(childrenList))
		for i, child := range childrenList {
			childMap := child.(map[string]interface{})
			children[i] = collectPlan(&childMap)
		}
	}

	return &neoPlan{
		operator:    operationType,
		arguments:   args,
		identifiers: identifiers,
		children:    children,
	}
}

func collectProfile(dict *map[string]interface{}) ProfiledPlan {
	var (
		operationType string
		args          map[string]interface{}
		identifiers   []string
		dbHits        int64
		records       int64
		children      []ProfiledPlan
	)

	if dict == nil {
		return nil
	}

	plansDict := *dict
	operationType = extractStringValue(dict, "operatorType", "")
	dbHits = extractIntValue(dict, "dbHits", 0)
	records = extractIntValue(dict, "rows", 0)

	if argsValue, ok := plansDict["args"]; ok {
		args = argsValue.(map[string]interface{})
	}

	if identifiersValue, ok := plansDict["identifiers"]; ok {
		identifiersList := identifiersValue.([]interface{})
		identifiers = make([]string, len(identifiersList))
		for i, identifier := range identifiersList {
			identifiers[i] = identifier.(string)
		}
	}

	if childrenValue, ok := plansDict["children"]; ok {
		childrenList := childrenValue.([]interface{})
		children = make([]ProfiledPlan, len(childrenList))
		for i, child := range childrenList {
			childMap := child.(map[string]interface{})
			children[i] = collectProfile(&childMap)
		}
	}

	return &neoProfiledPlan{
		operator:    operationType,
		arguments:   args,
		identifiers: identifiers,
		dbHits:      dbHits,
		records:     records,
		children:    children,
	}
}
