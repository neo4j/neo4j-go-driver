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

package boltagent

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/metadata"
	"runtime"
)

var os = runtime.GOOS
var arch = runtime.GOARCH
var goVersion = runtime.Version()
var driverVersion = metadata.DriverVersion

type BoltAgent struct {
	product  string
	platform string
	language string
}

// New returns a BoltAgent containing immutable, preformatted driver information.
func New() *BoltAgent {
	return &BoltAgent{
		product:  fmt.Sprintf("neo4j-go/%s", driverVersion),
		platform: fmt.Sprintf("%s; %s", os, arch),
		language: fmt.Sprintf("Go/%s", goVersion),
	}
}

func (b *BoltAgent) Product() string {
	return b.product
}

func (b *BoltAgent) Platform() string {
	return b.platform
}

func (b *BoltAgent) Language() string {
	return b.language
}
