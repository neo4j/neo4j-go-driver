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
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Runner", func() {
	var (
		mockCtrl *gomock.Controller
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("runStatement", func() {
		It("should invoke run on connection", func() {
			mockConnection := NewMockConnection(mockCtrl)
			mockConnector := MockedConnector(mockConnection)
			mockDriver := newGoboltWithConnector("bolt://localhost", mockConnector)
			runner := newRunner(mockDriver, AccessModeWrite, true)

			emptyMap := map[string]interface{}(nil)

			gomock.InOrder(
				mockConnection.EXPECT().Run("RETURN 1", emptyMap, nil, 0*time.Second, nil).Times(1),
				mockConnection.EXPECT().PullAll().Times(1),
				mockConnection.EXPECT().Flush().Times(1),
				mockConnection.EXPECT().RemoteAddress(),
				mockConnection.EXPECT().Server(),
			)

			_, err := runner.runStatement(&neoStatement{text: "RETURN 1"}, nil, TransactionConfig{})

			Expect(err).To(BeNil())
		})
	})
})
