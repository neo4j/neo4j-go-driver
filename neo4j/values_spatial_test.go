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
	"math"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Spatial Types", func() {

	Context("Point", func() {
		When("constructed with 2 coordinates", func() {
			point := NewPoint2D(1, 1.0, 2.0)

			It("should have a dimension of 2", func() {
				Expect(point.dimension).To(Equal(2))
			})

			It("should report provided srId", func() {
				Expect(point.SrId()).To(Equal(1))
			})

			It("should report provided x", func() {
				Expect(point.X()).To(Equal(1.0))
			})

			It("should report provided y", func() {
				Expect(point.Y()).To(Equal(2.0))
			})

			It("should report z as NaN", func() {
				Expect(math.IsNaN(point.Z())).To(BeTrue())
			})

			It("should generate correct string", func() {
				Expect(point.String()).To(Equal("Point{srId=1, x=1.000000, y=2.000000}"))
			})
		})

		When("constructed with 3 coordinates", func() {
			point := NewPoint3D(1, 1.0, 2.0, 3.0)

			It("should have a dimension of 3", func() {
				Expect(point.dimension).To(Equal(3))
			})

			It("should report provided srId", func() {
				Expect(point.SrId()).To(Equal(1))
			})

			It("should report provided x", func() {
				Expect(point.X()).To(Equal(1.0))
			})

			It("should report provided y", func() {
				Expect(point.Y()).To(Equal(2.0))
			})

			It("should report provided z", func() {
				Expect(point.Z()).To(Equal(3.0))
			})

			It("should generate correct string", func() {
				Expect(point.String()).To(Equal("Point{srId=1, x=1.000000, y=2.000000, z=3.000000}"))
			})
		})
	})

})
