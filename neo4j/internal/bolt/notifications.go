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

package bolt

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

func checkNotificationFiltering(
	notificationConfig idb.NotificationConfig,
	bolt idb.Connection,
) error {
	if notificationConfig.MinSev == notifications.DefaultLevel &&
		!notificationConfig.DisCats.DisablesNone() && len(notificationConfig.DisCats.DisabledCategories()) == 0 {
		return nil
	}
	version := bolt.Version()
	if version.Major < 5 || version.Major == 5 && version.Minor < 2 {
		return &db.FeatureNotSupportedError{
			Server:  bolt.ServerName(),
			Feature: "notification filtering",
			Reason:  "requires least server v5.7",
		}
	}
	return nil
}
