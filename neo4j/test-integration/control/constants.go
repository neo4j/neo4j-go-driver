/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package control

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j"
)

const (
	enterpriseEdition = "enterprise"

	defaultVersionAndEdition = "-e 3.5"
	defaultLogLevel          = neo4j.ERROR

	username = "neo4j"
	password = "password"

	clusterCoreCount        = 3
	clusterReadReplicaCount = 2
	clusterPort             = 20000
	clusterStartupTimeout   = 120 * time.Second
	clusterCheckInterval    = 500 * time.Millisecond
)
