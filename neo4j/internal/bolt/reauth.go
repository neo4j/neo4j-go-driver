/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
)

func checkReAuth(auth *db.ReAuthToken, connection db.Connection) error {
	fromSession := auth.FromSession
	version := connection.Version()
	if !fromSession {
		return nil
	}
	if version.Major < 5 || (version.Major == 5 && version.Minor == 0) {
		serverName := connection.ServerName()
		return &idb.FeatureNotSupportedError{
			Server:  serverName,
			Feature: "session auth",
			Reason:  "requires least server v5.5",
		}
	}
	return nil
}
