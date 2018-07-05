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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/mockseabolt"
	"testing"

	"github.com/golang/mock/gomock"
)

func TestRunner(t *testing.T) {
	t.Run("runStatement should invoke run on connection", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()

		mockConnection := mockseabolt.NewMockConnection(mockCtrl)
		mockConnector := mockseabolt.MockedConnector(mockConnection)
		mockDriver := newDirectWithConnector("bolt://localhost", mockConnector)
		runner := newRunner(mockDriver, AccessModeWrite, true)

		gomock.InOrder(
			mockConnection.EXPECT().Run("RETURN 1", nil).Times(1),
			mockConnection.EXPECT().PullAll().Times(1),
			mockConnection.EXPECT().Flush().Times(1),
		)

		_, err := runner.runStatement(Statement{cypher: "RETURN 1"})

		assertNil(t, err)
	})
}
