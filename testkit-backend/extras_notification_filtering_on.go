//go:build !internal_neo4j_testkit_no_notification_filtering

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

import (
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

const extrasNotificationFiltering = "notificationFiltering"

func init() {
	registerExtra(
		extrasNotificationFiltering,
		ExtrasRegisterEntry{
			extraDriverConfigurer:  extrasNotificationFilteringDriverConfig,
			extraSessionConfigurer: extrasNotificationFilteringSessionConfig,
		},
	)
}

func serializeNotification(notification neo4j.Notification) map[string]any {
	res := map[string]any{
		"code":        notification.Code(),
		"title":       notification.Title(),
		"description": notification.Description(),
		//lint:ignore SA1019 Severity is supported at least until 6.0
		"severity":         notification.Severity(),
		"severityLevel":    string(notification.SeverityLevel()),
		"rawSeverityLevel": notification.RawSeverityLevel(),
		"category":         string(notification.Category()),
		"rawCategory":      notification.RawCategory(),
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

func extrasNotificationFilteringDriverConfig(backend *backend, data map[string]any, config *Config) error {
	if data["notificationsMinSeverity"] != nil {
		minSeverity, err := mapNotificationMinSeverityLevel(data["notificationsMinSeverity"].(string))
		if err != nil {
			return err
		}
		config.NotificationsMinSeverity = minSeverity
	}
	if data["notificationsDisabledCategories"] != nil {
		notiDisCats := data["notificationsDisabledCategories"].([]any)
		if len(notiDisCats) == 0 {
			config.NotificationsDisabledCategories = notifications.DisableNoCategories()
		} else {
			cats := convertSlice(notiDisCats, anyToNotificationCategory)
			config.NotificationsDisabledCategories = notifications.DisableCategories(cats...)
		}
	}
	return nil
}

func extrasNotificationFilteringSessionConfig(backend *backend, data map[string]any, config *neo4j.SessionConfig) error {
	if data["notificationsMinSeverity"] != nil {
		minSeverity, err := mapNotificationMinSeverityLevel(data["notificationsMinSeverity"].(string))
		if err != nil {
			return err
		}
		config.NotificationsMinSeverity = minSeverity
	}
	if data["notificationsDisabledCategories"] != nil {
		notiDisCats := data["notificationsDisabledCategories"].([]any)
		if len(notiDisCats) == 0 {
			config.NotificationsDisabledCategories = notifications.DisableNoCategories()
		} else {
			cats := convertSlice(notiDisCats, anyToNotificationCategory)
			config.NotificationsDisabledCategories = notifications.DisableCategories(cats...)
		}
	}
	return nil
}

func anyToNotificationCategory(v any) notifications.NotificationCategory {
	return notifications.NotificationCategory(v.(string))
}

func mapNotificationMinSeverityLevel(rawMinSeverityLevel string) (notifications.NotificationMinimumSeverityLevel, error) {
	switch rawMinSeverityLevel {
	case "OFF":
		return notifications.DisabledLevel, nil
	case "WARNING":
		return notifications.WarningLevel, nil
	case "INFORMATION":
		return notifications.InformationLevel, nil
	}
	return "", fmt.Errorf("unknown min severity level %s", rawMinSeverityLevel)
}
