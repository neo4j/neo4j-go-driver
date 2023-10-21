/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
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

package boltagent

import (
	. "github.com/DaChartreux/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func init() {
	os = "darwin"
	arch = "amd64"
	goVersion = "go1.20.3"
	driverVersion = "5.9.0"
}

func TestNew(t *testing.T) {
	actual := New()

	AssertStringEqual(t, actual.Product(), "neo4j-go/5.9.0")
	AssertStringEqual(t, actual.Platform(), "darwin; amd64")
	AssertStringEqual(t, actual.Language(), "Go/go1.20.3")
}
