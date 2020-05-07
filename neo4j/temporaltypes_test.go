/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Temporal Types", func() {
	Context("LocalDate", func() {
		When("initialized from a Time value", func() {
			source := time.Date(2006, time.December, 16, 13, 59, 59, 999999999, time.Local)
			value := DateOf(source)

			It("should report year as 2006", func() {
				Expect(value.Year()).To(Equal(2006))
			})

			It("should report month as December", func() {
				Expect(value.Month()).To(Equal(time.December))
			})

			It("should report day as 16", func() {
				Expect(value.Day()).To(Equal(16))
			})

			It("should return an equivalent Time value when converted", func() {
				year, month, day := value.Time().Date()

				Expect(year).To(Equal(source.Year()))
				Expect(month).To(Equal(source.Month()))
				Expect(day).To(Equal(source.Day()))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("2006-12-16"))
			})
		})
	})

	Context("LocalTime", func() {
		When("initialized from a Time value", func() {
			source := time.Date(0, 0, 0, 13, 59, 59, 999999999, time.Local)
			value := LocalTimeOf(source)

			It("should report hour as 13", func() {
				Expect(value.Hour()).To(Equal(13))
			})

			It("should report minute as 59", func() {
				Expect(value.Minute()).To(Equal(59))
			})

			It("should report second as 59", func() {
				Expect(value.Second()).To(Equal(59))
			})

			It("should report nanosecond as 999999999", func() {
				Expect(value.Nanosecond()).To(Equal(999999999))
			})

			It("should return an equivalent Time value when converted", func() {
				converted := value.Time()

				Expect(converted.Hour()).To(Equal(source.Hour()))
				Expect(converted.Minute()).To(Equal(source.Minute()))
				Expect(converted.Second()).To(Equal(source.Second()))
				Expect(converted.Nanosecond()).To(Equal(source.Nanosecond()))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("13:59:59.999999999"))
			})
		})
	})

	Context("OffsetTime", func() {
		When("initialized from a Time value", func() {
			year, month, day := time.Now().Date()
			location, _ := time.LoadLocation("Europe/Istanbul")
			source := time.Date(year, month, day, 13, 59, 59, 999999999, location)
			value := OffsetTimeOf(source)

			It("should report hour as 13", func() {
				Expect(value.Hour()).To(Equal(13))
			})

			It("should report minute as 59", func() {
				Expect(value.Minute()).To(Equal(59))
			})

			It("should report second as 59", func() {
				Expect(value.Second()).To(Equal(59))
			})

			It("should report nanosecond as 999999999", func() {
				Expect(value.Nanosecond()).To(Equal(999999999))
			})

			It("should report offset as 3*60*60 (3 hours)", func() {
				Expect(value.Offset()).To(Equal(10800))
			})

			It("should return an equivalent Time value when converted", func() {
				converted := value.Time()

				Expect(converted.Hour()).To(Equal(source.Hour()))
				Expect(converted.Minute()).To(Equal(source.Minute()))
				Expect(converted.Second()).To(Equal(source.Second()))
				Expect(converted.Nanosecond()).To(Equal(source.Nanosecond()))

				_, convertedOffset := converted.Zone()
				_, offset := source.Zone()
				Expect(convertedOffset).To(Equal(offset))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("13:59:59.999999999+03:00"))
			})
		})
	})

	Context("LocalDateTime", func() {
		When("initialized from a Time value with local time zone", func() {
			source := time.Date(1947, 12, 17, 23, 49, 54, 999999999, time.Local)
			value := LocalDateTimeOf(source)

			It("should report year as 2006", func() {
				Expect(value.Year()).To(Equal(1947))
			})

			It("should report month as December", func() {
				Expect(value.Month()).To(Equal(time.December))
			})

			It("should report day as 17", func() {
				Expect(value.Day()).To(Equal(17))
			})

			It("should report hour as 23", func() {
				Expect(value.Hour()).To(Equal(23))
			})

			It("should report minute as 49", func() {
				Expect(value.Minute()).To(Equal(49))
			})

			It("should report second as 54", func() {
				Expect(value.Second()).To(Equal(54))
			})

			It("should report nanosecond as 999999999", func() {
				Expect(value.Nanosecond()).To(Equal(999999999))
			})

			It("should return an equivalent Time value when converted", func() {
				converted := value.Time()

				Expect(converted.Year()).To(Equal(source.Year()))
				Expect(converted.Month()).To(Equal(source.Month()))
				Expect(converted.Day()).To(Equal(source.Day()))
				Expect(converted.Hour()).To(Equal(source.Hour()))
				Expect(converted.Minute()).To(Equal(source.Minute()))
				Expect(converted.Second()).To(Equal(source.Second()))
				Expect(converted.Nanosecond()).To(Equal(source.Nanosecond()))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("1947-12-17T23:49:54.999999999"))
			})
		})

		When("initialized from a Time value with UTC time zone", func() {
			source := time.Date(1947, 12, 17, 23, 49, 54, 999999999, time.UTC)
			value := LocalDateTimeOf(source)

			It("should report year as 2006", func() {
				Expect(value.Year()).To(Equal(1947))
			})

			It("should report month as December", func() {
				Expect(value.Month()).To(Equal(time.December))
			})

			It("should report day as 17", func() {
				Expect(value.Day()).To(Equal(17))
			})

			It("should report hour as 23", func() {
				Expect(value.Hour()).To(Equal(23))
			})

			It("should report minute as 49", func() {
				Expect(value.Minute()).To(Equal(49))
			})

			It("should report second as 54", func() {
				Expect(value.Second()).To(Equal(54))
			})

			It("should report nanosecond as 999999999", func() {
				Expect(value.Nanosecond()).To(Equal(999999999))
			})

			It("should return an equivalent Time value when converted", func() {
				converted := value.Time()

				Expect(converted.Year()).To(Equal(source.Year()))
				Expect(converted.Month()).To(Equal(source.Month()))
				Expect(converted.Day()).To(Equal(source.Day()))
				Expect(converted.Hour()).To(Equal(source.Hour()))
				Expect(converted.Minute()).To(Equal(source.Minute()))
				Expect(converted.Second()).To(Equal(source.Second()))
				Expect(converted.Nanosecond()).To(Equal(source.Nanosecond()))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("1947-12-17T23:49:54.999999999"))
			})
		})

		When("initialized from a Time value with a custom time zone", func() {
			location, _ := time.LoadLocation("Europe/Istanbul")
			source := time.Date(1947, 12, 17, 23, 49, 54, 999999999, location)
			value := LocalDateTimeOf(source)

			It("should report year as 2006", func() {
				Expect(value.Year()).To(Equal(1947))
			})

			It("should report month as December", func() {
				Expect(value.Month()).To(Equal(time.December))
			})

			It("should report day as 17", func() {
				Expect(value.Day()).To(Equal(17))
			})

			It("should report hour as 23", func() {
				Expect(value.Hour()).To(Equal(23))
			})

			It("should report minute as 49", func() {
				Expect(value.Minute()).To(Equal(49))
			})

			It("should report second as 54", func() {
				Expect(value.Second()).To(Equal(54))
			})

			It("should report nanosecond as 999999999", func() {
				Expect(value.Nanosecond()).To(Equal(999999999))
			})

			It("should return an equivalent Time value when converted", func() {
				converted := value.Time()

				Expect(converted.Year()).To(Equal(source.Year()))
				Expect(converted.Month()).To(Equal(source.Month()))
				Expect(converted.Day()).To(Equal(source.Day()))
				Expect(converted.Hour()).To(Equal(source.Hour()))
				Expect(converted.Minute()).To(Equal(source.Minute()))
				Expect(converted.Second()).To(Equal(source.Second()))
				Expect(converted.Nanosecond()).To(Equal(source.Nanosecond()))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("1947-12-17T23:49:54.999999999"))
			})
		})
	})

	Context("Duration", func() {
		When("constructed", func() {
			value := DurationOf(15, 32, 785, 789215800)

			It("should report months as 15", func() {
				Expect(value.Months()).To(BeNumerically("==", 15))
			})

			It("should report days as 32", func() {
				Expect(value.Days()).To(BeNumerically("==", 32))
			})

			It("should report second as 785", func() {
				Expect(value.Seconds()).To(BeNumerically("==", 785))
			})

			It("should report nanosecond as 789215800", func() {
				Expect(value.Nanos()).To(BeNumerically("==", 789215800))
			})

			It("should generate correct string", func() {
				Expect(value.String()).To(Equal("P15M32DT785.789215800S"))
			})
		})

		DescribeTable("should generate correct string", func(months, days, seconds, nanoseconds int, expected string) {
			duration := DurationOf(int64(months), int64(days), int64(seconds), nanoseconds)

			Expect(duration.String()).To(Equal(expected))
		},
			Entry("P15M32DT785.789215800S", 15, 32, 785, 789215800, "P15M32DT785.789215800S"),
			Entry("P0M32DT785.789215800S", 0, 32, 785, 789215800, "P0M32DT785.789215800S"),
			Entry("P0M0DT785.789215800S", 0, 0, 785, 789215800, "P0M0DT785.789215800S"),
			Entry("P0M0DT0.789215800S", 0, 0, 0, 789215800, "P0M0DT0.789215800S"),
			Entry("P0M0DT-1S", 0, 0, -1, 0, "P0M0DT-1S"),
			Entry("P0M0DT0.999999999S", 0, 0, 0, 999999999, "P0M0DT0.999999999S"),
			Entry("P0M0DT-0.999999995S", 0, 0, -1, 5, "P0M0DT-0.999999995S"),
			Entry("P0M0DT-0.000000001S", 0, 0, -1, 999999999, "P0M0DT-0.000000001S"),
			Entry("P500M0DT0S", 500, 0, 0, 0, "P500M0DT0S"),
			Entry("P0M0DT0.000000005S", 0, 0, 0, 5, "P0M0DT0.000000005S"),
			Entry("P0M0DT-499.999999999S", 0, 0, -500, 1, "P0M0DT-499.999999999S"),
			Entry("P0M0DT-500S", 0, 0, -500, 0, "P0M0DT-500S"),
			Entry("P-10M5DT-1.999999500S", -10, 5, -2, 500, "P-10M5DT-1.999999500S"),
			Entry("P-10M-5DT-1.999999500S", -10, -5, -2, 500, "P-10M-5DT-1.999999500S"))
	})
})
