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

package neo4j

import (
	"net/url"

	. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/neo4j-drivers/gobolt"
)

var _ = Describe("Driver", func() {
	Context("Creation", func() {
		Context("Authentication Token", func() {
			driverUrl, _ := url.Parse("bolt://localhost")

			It("should fail when an invalid type is passed", func() {
				type unsupportedType struct {
				}

				var token = AuthToken{
					tokens: map[string]interface{}{
						"unsupported_type": unsupportedType{},
					},
				}

				driver, err := newGoboltDriver(driverUrl, token, nil)
				Expect(err).To(BeGenericError(ContainSubstring("unable to convert authentication token:")))
				Expect(driver).To(BeNil())
			})
		})

		Context("Routing Context", func() {
			DescribeTable("should not fail",
				func(uri string) {
					driverUrl, _ := url.Parse(uri)

					driver, err := newGoboltDriver(driverUrl, NoAuth(), nil)
					Expect(err).To(BeNil())
					Expect(driver).NotTo(BeNil())
				},
				Entry("empty query string", "bolt://localhost"),
				Entry("query string with empty value", "bolt://localhost?abc"),
				Entry("query string with value", "bolt://localhost?abc=def"),
				Entry("query string with values", "bolt://localhost?abc=def&def=gef"),
			)

			DescribeTable("should fail",
				func(uri string) {
					driverUrl, _ := url.Parse(uri)

					driver, err := newGoboltDriver(driverUrl, NoAuth(), nil)
					Expect(err).To(BeGenericError(ContainSubstring("unable to extract routing context")))
					Expect(driver).To(BeNil())
				},
				Entry("incorrect query string 1", "bolt://localhost?a%b"),
				Entry("incorrect query string 2", "bolt://localhost?abc%d=ef"),
				Entry("duplicate values", "bolt://localhost?abc=def&def=gef&abc=hij"),
			)
		})
	})
})

func newGoboltWithConnector(target string, connector gobolt.Connector) *goboltDriver {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil
	}

	return &goboltDriver{
		target:    *targetURL,
		connector: connector,
		open:      1,
	}
}
