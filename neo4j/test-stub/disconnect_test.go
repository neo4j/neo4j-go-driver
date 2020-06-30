/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

package test_stub

import (
	"path"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-stub/control"
)

func Test_Disconnect(t *testing.T) {
	var cases = []struct {
		name     string
		protocol string
	}{
		//{"V1", "v1"},
		{"V3", "v3"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var verifyServiceUnavailable = func(t *testing.T, script string) {
				stub := control.NewStubServer(t, 9001, path.Join(testCase.protocol, script))
				defer stub.Finished(t)

				driver := newDriver(t, "bolt://localhost:9001")
				defer driver.Close()

				session := createWriteSession(t, driver)
				defer session.Close()

				result, err := session.Run("RETURN $x", map[string]interface{}{"x": 1})
				assertNoError(t, err)

				summary, err := result.Consume()
				assertError(t, err)
				// assert IsServiceUnavailable
				assertNil(t, summary)
			}

			t.Run("shouldReturnErrorWhenServerDisconnectsAfterInit", func(t *testing.T) {
				verifyServiceUnavailable(t, "disconnect_after_login.script")
			})

			t.Run("shouldReturnErrorWhenServerDisconnectsAfterRun", func(t *testing.T) {
				t.SkipNow()
				verifyServiceUnavailable(t, "disconnect_on_run.script")
			})

			t.Run("shouldReturnErrorWhenServerDisconnectsAfterPullAll", func(t *testing.T) {
				t.SkipNow()
				verifyServiceUnavailable(t, "disconnect_on_pull_all.script")
			})
		})
	}

}
