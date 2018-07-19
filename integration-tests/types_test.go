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

package integration_tests

import (
	. "github.com/neo4j/neo4j-go-driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Types", func() {
	var (
		err     error
		driver  Driver
		session *Session
		result  *Result
	)

	BeforeEach(func() {
		driver, err = NewDriver(singleInstanceUri, BasicAuth(username, password, ""))
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())

		session, err = driver.Session(AccessModeRead)
		Expect(err).To(BeNil())
		Expect(session).NotTo(BeNil())
	})

	AfterEach(func() {
		if session != nil {
			session.Close()
		}

		if driver != nil {
			driver.Close()
		}
	})

	It("should be able to send and receive boolean property", func() {
		value := true
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(Equal(value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive byte property", func() {
		value := byte(1)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(int64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int8 property", func() {
		value := int8(2)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(int64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int16 property", func() {
		value := int16(3)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(int64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int property", func() {
		value := int(4)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(int64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int32 property", func() {
		value := int32(5)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(int64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int64 property", func() {
		value := int64(6)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(int64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive float32 property", func() {
		value := float32(7.1)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(float64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive float64 property", func() {
		value := float64(81.9224)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(float64(0)))
			Expect(result.Record().GetByIndex(0)).To(BeNumerically("==", value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive string property", func() {
		value := "a string"
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(""))
			Expect(result.Record().GetByIndex(0)).To(Equal(value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive byte array property", func() {
		value := []byte{1, 2, 3, 4, 5}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf([]byte(nil)))
			Expect(result.Record().GetByIndex(0)).To(BeEquivalentTo(value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive boolean array property", func() {
		value := []bool{true, false, false, true}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf([]interface{}(nil)))
			Expect(result.Record().GetByIndex(0)).To(Equal([]interface{}{value[0], value[1], value[2], value[3]}))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int array property", func() {
		value := []int{1, 2, 3, 4, 5}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf([]interface{}(nil)))
			Expect(result.Record().GetByIndex(0)).To(Equal([]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive float64 array property", func() {
		value := []float64{1.11, 2.22, 3.33, 4.44, 5.55}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf([]interface{}(nil)))
			Expect(result.Record().GetByIndex(0)).To(Equal([]interface{}{1.11, 2.22, 3.33, 4.44, 5.55}))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive string array property", func() {
		value := []string{"a", "b", "c", "d", "e"}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf([]interface{}(nil)))
			Expect(result.Record().GetByIndex(0)).To(Equal([]interface{}{"a", "b", "c", "d", "e"}))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	//It("should be able to send and receive large string property", func() {
	//	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	//
	//	randSeq := func (n int) string {
	//		b := make([]rune, n)
	//		for i := range b {
	//		b[i] = letters[rand.Intn(len(letters))]
	//	}
	//		return string(b)
	//	}
	//
	//	value := randSeq(20 * 1024)
	//	fmt.Println(value)
	//	fmt.Println()
	//
	//	result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
	//	Expect(err).To(BeNil())
	//
	//	if result.Next() {
	//		fmt.Println(result.Record().GetByIndex(0))
	//		fmt.Println()
	//		Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(""))
	//		Expect(result.Record().GetByIndex(0)).To(Equal(value))
	//	}
	//	Expect(result.Next()).To(BeFalse())
	//	Expect(result.Err()).To(BeNil())
	//})

})
