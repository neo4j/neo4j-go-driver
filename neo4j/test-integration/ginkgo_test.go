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
	"os"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	"github.com/onsi/gomega"
)

func TestIntegrationTests(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)

	customReporters := []ginkgo.Reporter(nil)
	if os.Getenv("TEAMCITY_VERSION") != "" {
		customReporters = append(customReporters, reporters.NewTeamCityReporter(os.Stdout))
	}

	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "Neo4j Go Driver Integration Tests", customReporters)
}
