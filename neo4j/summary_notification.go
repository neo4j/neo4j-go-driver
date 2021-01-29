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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

// Notification represents notifications generated when executing a statement.
// A notification can be visualized in a client pinpointing problems or other information about the statement.
type Notification interface {
	// Code returns a notification code for the discovered issue of this notification.
	Code() string
	// Title returns a short summary of this notification.
	Title() string
	// Description returns a longer description of this notification.
	Description() string
	// Position returns the position in the statement where this notification points to.
	// Not all notifications have a unique position to point to and in that case the position would be set to nil.
	Position() InputPosition
	// Severity returns the severity level of this notification.
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
