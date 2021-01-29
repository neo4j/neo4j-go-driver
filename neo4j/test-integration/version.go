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

package test_integration

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/utils"
)

var (
	// V340 identifies server version 3.4.0
	V340 = utils.VersionOf("3.4.0")
	// V350 identifies server version 3.5.0
	V350 = utils.VersionOf("3.5.0")
)

func versionOfDriver(driver neo4j.Driver) utils.Version {
	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return utils.VersionOf("0.0.0")
	}
	defer session.Close()

	result, err := session.Run("RETURN 1", nil)
	if err != nil {
		return utils.VersionOf("0.0.0")
	}

	summary, err := result.Consume()
	if err != nil {
		return utils.VersionOf("0.0.0")
	}

	return utils.VersionOf(summary.Server().Version())
}
