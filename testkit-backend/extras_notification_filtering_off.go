//go:build internal_neo4j_testkit_no_notification_filtering

/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

const extrasNotificationFiltering = "notificationFiltering"

func init() {
	registerExtra(
		extrasNotificationFiltering,
		ExtrasRegisterEntry{
			extraBlockedTestKitFeatures: []string{
				"Feature:API:Driver:NotificationsConfig",
				"Feature:API:Session:NotificationsConfig",
				"Feature:Bolt:5.1",
				"Feature:Bolt:5.2",
			},
		},
	)
}

func serializeNotification(notification neo4j.Notification) map[string]any {
	res := map[string]any{
		"code":             notification.Code(),
		"title":            notification.Title(),
		"description":      notification.Description(),
		"severity":         notification.Severity(),
		"severityLevel":    "",
		"rawSeverityLevel": "",
		"category":         "",
		"rawCategory":      "",
	}
	if notification.Position() != nil {
		res["position"] = map[string]any{
			"offset": notification.Position().Offset(),
			"line":   notification.Position().Line(),
			"column": notification.Position().Column(),
		}
	}
	return res
}
