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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// Notification contains information about notifications generated by the server
type Notification interface {
	Code() string
	Title() string
	Description() string
	Position() InputPosition
	Severity() string
}

type neoNotification struct {
	code        string
	title       string
	description string
	position    *neoInputPosition
	severity    string
}

func (notification *neoNotification) Code() string {
	return notification.code
}

func (notification *neoNotification) Title() string {
	return notification.title
}

func (notification *neoNotification) Description() string {
	return notification.description
}

func (notification *neoNotification) Position() InputPosition {
	return notification.position
}

func (notification *neoNotification) Severity() string {
	return notification.severity
}
