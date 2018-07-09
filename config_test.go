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
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
    "time"
)

var _ = Describe("Config", func() {
    Context("DefaultConfig", func() {
        config := defaultConfig()

        It("should have encryption turned on", func() {
            Expect(config.Encrypted).To(BeTrue())
        })

        It("should have max transaction retry duration as 30s", func() {
            Expect(config.MaxTransactionRetryDuration).To(BeIdenticalTo(30 * time.Second))
        })

        It("should have no-op logger", func() {
            Expect(config.Log).NotTo(BeNil())

            logger, ok := config.Log.(*internalLogger)
            Expect(ok).To(BeTrue())

            Expect(logger.level).To(BeZero())
        })
    })
})
