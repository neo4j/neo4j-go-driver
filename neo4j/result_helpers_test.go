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
	. "github.com/neo4j/neo4j-go-driver/neo4j/utils/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("Result Helpers", func() {
	var (
		mockCtrl   *gomock.Controller
		mockResult *MockResult
		mockRecord *MockRecord
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Single", func() {
		var fixedError = errors.New("some error")

		It("should return error when error is passed", func() {
			record, err := Single(nil, fixedError)

			Expect(record).To(BeNil())
			Expect(err).To(Equal(fixedError))
		})

		It("should return error when from is not record", func() {
			record, err := Single("i'm not a record", nil)

			Expect(record).To(BeNil())
			Expect(err).To(BeGenericError(ContainSubstring("expected from to be a result but it was 'i'm not a record'")))
		})

		It("should return error if result returns error", func() {
			mockResult = NewMockResult(mockCtrl)
			gomock.InOrder(
				mockResult.EXPECT().Next().Return(false),
				mockResult.EXPECT().Err().Return(fixedError),
			)

			record, err := Single(mockResult, nil)

			Expect(record).To(BeNil())
			Expect(err).To(Equal(fixedError))
		})

		It("should return error if result returns error after first Next", func() {
			mockRecord = NewMockRecord(mockCtrl)
			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord),
				mockResult.EXPECT().Err().Return(fixedError),
			)

			record, err := Single(mockResult, nil)

			Expect(record).To(BeNil())
			Expect(err).To(Equal(fixedError))
		})

		It("should return nil when there's no elements", func() {
			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(false),
				mockResult.EXPECT().Err().Return(nil),
			)

			record, err := Single(mockResult, nil)

			Expect(record).To(BeNil())
			Expect(err).Should(BeGenericError(ContainSubstring("result contains no records")))
		})

		It("should return record when there's only one element", func() {
			mockRecord = NewMockRecord(mockCtrl)
			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord),
				mockResult.EXPECT().Err().Return(nil),
				mockResult.EXPECT().Next().Return(false),
			)

			record, err := Single(mockResult, nil)

			Expect(record).To(Equal(mockRecord))
			Expect(err).To(BeNil())
		})

		It("should return error when there's more than one element", func() {
			mockRecord = NewMockRecord(mockCtrl)
			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord),
				mockResult.EXPECT().Err().Return(nil),
				mockResult.EXPECT().Next().Return(true),
			)

			record, err := Single(mockResult, nil)

			Expect(record).To(BeNil())
			Expect(err).Should(BeGenericError(ContainSubstring("result contains more than one record")))
		})
	})

	Context("Collect", func() {
		var fixedError = errors.New("some error")

		It("should return error when error is passed", func() {
			records, err := Collect(nil, fixedError)

			Expect(records).To(BeNil())
			Expect(err).To(Equal(fixedError))
		})

		It("should return error when from is not record", func() {
			record, err := Collect("i'm not a record", nil)

			Expect(record).To(BeNil())
			Expect(err).To(BeGenericError(ContainSubstring("expected from to be a result but it was 'i'm not a record'")))
		})

		It("should return error if result returns error", func() {
			mockResult = NewMockResult(mockCtrl)
			gomock.InOrder(
				mockResult.EXPECT().Next().Return(false),
				mockResult.EXPECT().Err().Return(fixedError),
			)

			records, err := Collect(mockResult, nil)

			Expect(records).To(BeNil())
			Expect(err).To(Equal(fixedError))
		})

		It("should return error if result returns error after first Next", func() {
			mockRecord = NewMockRecord(mockCtrl)
			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord),
				mockResult.EXPECT().Next().Return(false),
				mockResult.EXPECT().Err().Return(fixedError),
			)

			records, err := Collect(mockResult, nil)

			Expect(records).To(BeNil())
			Expect(err).To(Equal(fixedError))
		})

		It("should return one record", func() {
			mockRecord = NewMockRecord(mockCtrl)
			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord),
				mockResult.EXPECT().Next().Return(false),
				mockResult.EXPECT().Err().Return(nil),
			)

			records, err := Collect(mockResult, nil)

			Expect(records).To(HaveLen(1))
			Expect(records[0]).To(Equal(mockRecord))
			Expect(err).To(BeNil())
		})

		It("should return five records", func() {
			mockRecord1 := NewMockRecord(mockCtrl)
			mockRecord2 := NewMockRecord(mockCtrl)
			mockRecord3 := NewMockRecord(mockCtrl)
			mockRecord4 := NewMockRecord(mockCtrl)
			mockRecord5 := NewMockRecord(mockCtrl)

			mockResult = NewMockResult(mockCtrl)

			gomock.InOrder(
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord1),
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord2),
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord3),
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord4),
				mockResult.EXPECT().Next().Return(true),
				mockResult.EXPECT().Record().Return(mockRecord5),
				mockResult.EXPECT().Next().Return(false),
				mockResult.EXPECT().Err().Return(nil),
			)

			records, err := Collect(mockResult, nil)

			Expect(records).To(HaveLen(5))
			Expect(records[0]).To(Equal(mockRecord1))
			Expect(records[1]).To(Equal(mockRecord2))
			Expect(records[2]).To(Equal(mockRecord3))
			Expect(records[3]).To(Equal(mockRecord4))
			Expect(records[4]).To(Equal(mockRecord5))
			Expect(err).To(BeNil())
		})
	})
})
