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

var _ = Describe("Summary", func() {

	Context("extractIntValue", func() {
		dict := map[string]interface{}{
			"key1": 123,
			"key2": "456",
            "key3": "456.123",
			"key4": "some non-number value",
		}

        It("should return default value if map is nil", func() {
            extracted := extractIntValue(nil, "key", -1)

            Expect(extracted).To(BeNumerically("==", -1))
        })

		It("should return default value if key not found", func() {
            extracted := extractIntValue(&dict, "key", -1)

            Expect(extracted).To(BeNumerically("==", -1))
		})

        It("should return the int value", func() {
            extracted := extractIntValue(&dict, "key1", -1)

            Expect(extracted).To(BeNumerically("==", 123))
        })

        It("should return the non-int value if parsable", func() {
            extracted := extractIntValue(&dict, "key2", -1)

            Expect(extracted).To(BeNumerically("==", 456))
        })

        It("should return default value if value is a float", func() {
            extracted := extractIntValue(&dict, "key3", -1)

            Expect(extracted).To(BeNumerically("==", -1))
        })

		It("should return default value if value doesn't represent a numeric value", func() {
            extracted := extractIntValue(&dict, "key4", -1)

            Expect(extracted).To(BeNumerically("==", -1))
		})
	})

    Context("extractStringValue", func() {
        dict := map[string]interface{}{
            "key1": "123",
            "key2": 456,
            "key3": 3 * time.Second,
            "key4": 123.568,
            "key5": nil,
        }

        It("should return default value if map is nil", func() {
            extracted := extractStringValue(nil, "key", "default")

            Expect(extracted).To(Equal("default"))
        })

        It("should return default value if key not found", func() {
            extracted := extractStringValue(&dict, "key", "default")

            Expect(extracted).To(Equal("default"))
        })

        It("should return the string value", func() {
            extracted := extractStringValue(&dict, "key1", "default")

            Expect(extracted).To(Equal("123"))
        })

        It("should return the converted string value of int", func() {
            extracted := extractStringValue(&dict, "key2", "default")

            Expect(extracted).To(Equal("456"))
        })

        It("should return the converted string value of duration", func() {
            extracted := extractStringValue(&dict, "key3", "default")

            Expect(extracted).To(Equal("3s"))
        })

        It("should return the converted string value of float", func() {
            extracted := extractStringValue(&dict, "key4", "default")

            Expect(extracted).To(Equal("123.568"))
        })

        It("should return the default value when nil", func() {
            extracted := extractStringValue(&dict, "key5", "default")

            Expect(extracted).To(Equal("default"))
        })
    })

})
