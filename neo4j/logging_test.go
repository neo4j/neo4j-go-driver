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

import (
	"github.com/golang/mock/gomock"
	"github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = Describe("Logging", func() {
	var (
		mockCtrl *gomock.Controller
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("errorf", func() {
		When("Error level is not enabled", func() {
			It("should not invoke Errorf on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().ErrorEnabled().Times(1).Return(false)
				logging.EXPECT().Errorf(gomock.Any(), gomock.Any()).Times(0)

				errorf(logging, "some error: %s, %d", "str1", 1)
			})
		})

		When("Error level is enabled", func() {
			It("should invoke Errorf on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().ErrorEnabled().Times(1).Return(true)
				logging.EXPECT().Errorf(test.WrapMatcher(gomega.ContainSubstring("some error")), "str1", 1).Times(1)

				errorf(logging, "some error", "str1", 1)
			})
		})
	})

	Context("warningf", func() {
		When("Warning level is not enabled", func() {
			It("should not invoke Warningf on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().WarningEnabled().Times(1).Return(false)
				logging.EXPECT().Warningf(gomock.Any(), gomock.Any()).Times(0)

				warningf(logging, "some warning: %s, %d", "str1", 1)
			})
		})

		When("Warning level is enabled", func() {
			It("should invoke Warningf on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().WarningEnabled().Times(1).Return(true)
				logging.EXPECT().Warningf(test.WrapMatcher(gomega.ContainSubstring("some warning")), "str1", 1).Times(1)

				warningf(logging, "some warning", "str1", 1)
			})
		})
	})

	Context("infof", func() {
		When("Info level is not enabled", func() {
			It("should not invoke Infof on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().InfoEnabled().Times(1).Return(false)
				logging.EXPECT().Infof(gomock.Any(), gomock.Any()).Times(0)

				infof(logging, "some info: %s, %d", "str1", 1)
			})
		})

		When("Info level is enabled", func() {
			It("should invoke Infof on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().InfoEnabled().Times(1).Return(true)
				logging.EXPECT().Infof(test.WrapMatcher(gomega.ContainSubstring("some info")), "str1", 1).Times(1)

				infof(logging, "some info", "str1", 1)
			})
		})
	})

	Context("debugf", func() {
		When("Debug level is not enabled", func() {
			It("should not invoke Debugf on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().DebugEnabled().Times(1).Return(false)
				logging.EXPECT().Debugf(gomock.Any(), gomock.Any()).Times(0)

				debugf(logging, "some debug: %s, %d", "str1", 1)
			})
		})

		When("Debug level is enabled", func() {
			It("should invoke Debugf on logger", func() {
				logging := NewMockLogging(mockCtrl)

				logging.EXPECT().DebugEnabled().Times(1).Return(true)
				logging.EXPECT().Debugf(test.WrapMatcher(gomega.ContainSubstring("some debug")), "str1", 1).Times(1)

				debugf(logging, "some debug", "str1", 1)
			})
		})
	})
})
