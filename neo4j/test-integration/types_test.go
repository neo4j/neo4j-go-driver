/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

package test_integration

import (
	"math/rand"
	"reflect"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/test-integration/dbserver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Types", func() {
	server := dbserver.GetDbServer()
	var err error
	var driver neo4j.Driver
	var session neo4j.Session
	var result neo4j.Result

	BeforeEach(func() {
		driver = server.Driver()

		session, err = driver.Session(neo4j.AccessModeWrite)
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(Equal(value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive byte property", func() {
		value := byte(1)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(int64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int8 property", func() {
		value := int8(2)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(int64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int16 property", func() {
		value := int16(3)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(int64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int property", func() {
		value := int(4)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(int64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int32 property", func() {
		value := int32(5)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(int64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int64 property", func() {
		value := int64(6)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(int64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive float32 property", func() {
		value := float32(7.1)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(float64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive float64 property", func() {
		value := float64(81.9224)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(float64(0)))
		Expect(result.Record().Values[0]).To(BeNumerically("==", value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive string property", func() {
		value := "a string"
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(""))
		Expect(result.Record().Values[0]).To(Equal(value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive byte array property", func() {
		value := []byte{1, 2, 3, 4, 5}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf([]byte(nil)))
		Expect(result.Record().Values[0]).To(BeEquivalentTo(value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive boolean array property", func() {
		value := []bool{true, false, false, true}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf([]interface{}(nil)))
		Expect(result.Record().Values[0]).To(Equal([]interface{}{value[0], value[1], value[2], value[3]}))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive int array property", func() {
		value := []int{1, 2, 3, 4, 5}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf([]interface{}(nil)))
		Expect(result.Record().Values[0]).To(Equal([]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive float64 array property", func() {
		value := []float64{1.11, 2.22, 3.33, 4.44, 5.55}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf([]interface{}(nil)))
		Expect(result.Record().Values[0]).To(Equal([]interface{}{1.11, 2.22, 3.33, 4.44, 5.55}))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive string array property", func() {
		value := []string{"a", "b", "c", "d", "e"}
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf([]interface{}(nil)))
		Expect(result.Record().Values[0]).To(Equal([]interface{}{"a", "b", "c", "d", "e"}))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive large string property", func() {
		var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

		randSeq := func(n int) string {
			b := make([]rune, n)
			for i := range b {
				b[i] = letters[rand.Intn(len(letters))]
			}
			return string(b)
		}

		value := randSeq(20 * 1024)

		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		Expect(result.Record().Values[0]).To(BeAssignableToTypeOf(""))
		Expect(result.Record().Values[0]).To(Equal(value))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to receive a node with properties", func() {
		result, err = session.Run("CREATE (n:Person:Manager {id: 1, name: 'a name'}) RETURN n", nil)
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		node := result.Record().Values[0].(neo4j.Node)

		Expect(node).NotTo(BeNil())
		Expect(node.Id).NotTo(BeNil())
		Expect(node.Labels).To(HaveLen(2))
		Expect(node.Labels).To(ContainElement("Person"))
		Expect(node.Labels).To(ContainElement("Manager"))
		Expect(node.Props).To(HaveLen(2))
		Expect(node.Props).To(HaveKeyWithValue("id", int64(1)))
		Expect(node.Props).To(HaveKeyWithValue("name", "a name"))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to receive a relationship with properties", func() {
		result, err = session.Run("CREATE (e:Person:Employee {id: 1, name: 'employee 1'})-[w:WORKS_FOR { from: '2017-01-01' }]->(m:Person:Manager {id: 2, name: 'manager 1'}) RETURN e, w, m", nil)
		Expect(err).To(BeNil())

		Expect(result.Next()).To(BeTrue())
		employee := result.Record().Values[0].(neo4j.Node)
		worksFor := result.Record().Values[1].(neo4j.Relationship)
		manager := result.Record().Values[2].(neo4j.Node)

		Expect(employee).NotTo(BeNil())
		Expect(employee.Id).NotTo(BeNil())
		Expect(employee.Labels).To(HaveLen(2))
		Expect(employee.Labels).To(ContainElement("Person"))
		Expect(employee.Labels).To(ContainElement("Employee"))
		Expect(employee.Props).To(HaveLen(2))
		Expect(employee.Props).To(HaveKeyWithValue("id", int64(1)))
		Expect(employee.Props).To(HaveKeyWithValue("name", "employee 1"))

		Expect(worksFor).NotTo(BeNil())
		Expect(worksFor.Id).NotTo(BeNil())
		Expect(worksFor.StartId).To(Equal(employee.Id))
		Expect(worksFor.EndId).To(Equal(manager.Id))
		Expect(worksFor.Type).To(Equal("WORKS_FOR"))
		Expect(worksFor.Props).To(HaveLen(1))
		Expect(worksFor.Props).To(HaveKeyWithValue("from", "2017-01-01"))

		Expect(manager).NotTo(BeNil())
		Expect(manager.Id).NotTo(BeNil())
		Expect(manager.Labels).To(HaveLen(2))
		Expect(manager.Labels).To(ContainElement("Person"))
		Expect(manager.Labels).To(ContainElement("Manager"))
		Expect(manager.Props).To(HaveLen(2))
		Expect(manager.Props).To(HaveKeyWithValue("id", int64(2)))
		Expect(manager.Props).To(HaveKeyWithValue("name", "manager 1"))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to receive a path with two nodes and one relationship", func() {
		result, err = session.Run("CREATE p=(e:Person:Employee {id: 1, name: 'employee 1'})-[w:WORKS_FOR { from: '2017-01-01' }]->(m:Person:Manager {id: 2, name: 'manager 1'}) RETURN p, e, w, m", nil)
		Expect(err).To(BeNil())
		Expect(result.Next()).To(BeTrue())
		path := result.Record().Values[0].(neo4j.Path)
		employee := result.Record().Values[1].(neo4j.Node)
		worksFor := result.Record().Values[2].(neo4j.Relationship)
		manager := result.Record().Values[3].(neo4j.Node)

		Expect(path).NotTo(BeNil())
		Expect(path.Nodes).To(HaveLen(2))
		Expect(path.Nodes).To(ContainElement(employee))
		Expect(path.Nodes).To(ContainElement(manager))
		Expect(path.Relationships).To(HaveLen(1))
		Expect(path.Relationships).To(ContainElement(worksFor))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to receive a path with three nodes and two relationship", func() {
		result, err = session.Run(`CREATE (e1:Person:Employee $employee) 
				CREATE (l1:Person:Lead $lead) 
				CREATE (m1:Person:Manager $manager) 
				CREATE p = (e1)-[r1:LED_BY { from: '2017-01-01' }]->(l1)-[r2:REPORTS_TO { from: '2017-01-01' }]->(m1)
				RETURN p, e1, l1, m1, r1, r2`, map[string]interface{}{
			"employee": map[string]interface{}{
				"id":   1,
				"name": "employee 1",
			},
			"lead": map[string]interface{}{
				"id":   2,
				"name": "lead 1",
			},
			"manager": map[string]interface{}{
				"id":   3,
				"name": "manager 1",
			},
		})
		Expect(err).To(BeNil())
		Expect(result.Next()).To(BeTrue())

		path := result.Record().Values[0].(neo4j.Path)
		employee := result.Record().Values[1].(neo4j.Node)
		lead := result.Record().Values[2].(neo4j.Node)
		manager := result.Record().Values[3].(neo4j.Node)
		ledBy := result.Record().Values[4].(neo4j.Relationship)
		reportsTo := result.Record().Values[5].(neo4j.Relationship)

		Expect(path).NotTo(BeNil())
		Expect(path.Nodes).To(HaveLen(3))
		Expect(path.Nodes).To(ContainElement(employee))
		Expect(path.Nodes).To(ContainElement(lead))
		Expect(path.Nodes).To(ContainElement(manager))
		Expect(path.Relationships).To(HaveLen(2))
		Expect(path.Relationships).To(ContainElement(ledBy))
		Expect(path.Relationships).To(ContainElement(reportsTo))
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	DescribeTable("should be able to send and receive nil pointer property",
		func(value interface{}) {
			result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
			Expect(err).To(BeNil())

			Expect(result.Next()).To(BeTrue())
			Expect(result.Record().Values[0]).To(BeNil())
			Expect(result.Next()).To(BeFalse())
			Expect(result.Err()).To(BeNil())
		},
		Entry("boolean", (*bool)(nil)),
		Entry("byte", (*byte)(nil)),
		Entry("int8", (*int8)(nil)),
		Entry("int16", (*int16)(nil)),
		Entry("int32", (*int32)(nil)),
		Entry("int64", (*int64)(nil)),
		Entry("int", (*int)(nil)),
		Entry("uint8", (*uint8)(nil)),
		Entry("uint16", (*uint16)(nil)),
		Entry("uint32", (*uint32)(nil)),
		Entry("uint64", (*uint64)(nil)),
		Entry("uint", (*uint)(nil)),
		Entry("float32", (*float32)(nil)),
		Entry("float64", (*float64)(nil)),
		Entry("string", (*string)(nil)),
		Entry("boolean array", (*[]bool)(nil)),
		Entry("byte array", (*[]byte)(nil)),
		Entry("int8 array", (*[]int8)(nil)),
		Entry("int16 array", (*[]int16)(nil)),
		Entry("int32 array", (*[]int32)(nil)),
		Entry("int64 array", (*[]int64)(nil)),
		Entry("int array", (*[]int)(nil)),
		Entry("uint8 array", (*[]uint8)(nil)),
		Entry("uint16 array", (*[]uint16)(nil)),
		Entry("uint32 array", (*[]uint32)(nil)),
		Entry("uint64 array", (*[]uint64)(nil)),
		Entry("uint array", (*[]uint)(nil)),
		Entry("float32 array", (*[]float32)(nil)),
		Entry("float64 array", (*[]float64)(nil)),
		Entry("string array", (*[]string)(nil)),
	)

	Context("Un-convertible Go types", func() {
		type unsupportedType struct {
		}

		Context("Session.Run", func() {
			It("should fail when sending as parameter", func() {
				result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": unsupportedType{}})
				Expect(err).ToNot(BeNil())
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert parameter \"value\" to connector value for run message")))
				Expect(result).To(BeNil())
			})

			It("should fail when sending as tx metadata", func() {
				result, err = session.Run("CREATE (n)", nil, neo4j.WithTxMetadata(map[string]interface{}{"m1": unsupportedType{}}))
				Expect(err).ToNot(BeNil())
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert tx metadata to connector value for run message")))
				Expect(result).To(BeNil())
			})
		})

		/* Fails due to lazy transaction start
		Context("Session.BeginTransaction", func() {
			var tx neo4j.Transaction

			It("should fail when sending as tx metadata", func() {
				tx, err = session.BeginTransaction(neo4j.WithTxMetadata(map[string]interface{}{"m1": unsupportedType{}}))
				Expect(err).ToNot(BeNil())
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert tx metadata to connector value for begin message")))
				Expect(tx).To(BeNil())
			})
		})

		Context("Session.BeginTransaction", func() {
			var tx neo4j.Transaction

			It("should fail when sending as tx metadata", func() {
				tx, err = session.BeginTransaction(neo4j.WithTxMetadata(map[string]interface{}{"m1": unsupportedType{}}))
				Expect(err).ToNot(BeNil())
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert tx metadata to connector value for begin message")))
				Expect(tx).To(BeNil())
			})
		})

		Context("Session.ReadTransaction", func() {
			It("should fail when sending as tx metadata", func() {
				result, err := session.ReadTransaction(func(tx neo4j.Transaction) (i interface{}, e error) {
					return nil, nil
				}, neo4j.WithTxMetadata(map[string]interface{}{"m1": unsupportedType{}}))
				Expect(err).ToNot(BeNil())
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert tx metadata to connector value for begin message")))
				Expect(result).To(BeNil())
			})
		})

		Context("Session.WriteTransaction", func() {
			It("should fail when sending as tx metadata", func() {
				result, err := session.WriteTransaction(func(tx neo4j.Transaction) (i interface{}, e error) {
					return nil, nil
				}, neo4j.WithTxMetadata(map[string]interface{}{"m1": unsupportedType{}}))
				Expect(err).ToNot(BeNil())
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert tx metadata to connector value for begin message")))
				Expect(result).To(BeNil())
			})
		})
		*/

		Context("Transaction.Run", func() {
			var tx neo4j.Transaction

			It("should fail when sending as tx metadata", func() {
				tx, err = session.BeginTransaction()
				Expect(err).To(BeNil())
				Expect(tx).NotTo(BeNil())

				result, err = tx.Run("CREATE (n)", map[string]interface{}{"unsupported": unsupportedType{}})
				Expect(err).ToNot(BeNil())
				//				Expect(err).To(BeGenericError(ContainSubstring("unable to convert parameter \"unsupported\" to connector value for run messag")))
				Expect(result).To(BeNil())
			})
		})
	})

	Context("Aliased types", func() {
		type (
			booleanAlias      bool
			byteAlias         byte
			int8Alias         int8
			int16Alias        int16
			int32Alias        int32
			int64Alias        int64
			intAlias          int
			uint8Alias        uint8
			uint16Alias       uint16
			uint32Alias       uint32
			uint64Alias       uint64
			uintAlias         uint
			float32Alias      float32
			float64Alias      float64
			stringAlias       string
			booleanArrayAlias []bool
			byteArrayAlias    []byte
			int8ArrayAlias    []int8
			int16ArrayAlias   []int16
			int32ArrayAlias   []int32
			int64ArrayAlias   []int64
			intArrayAlias     []int
			uint8ArrayAlias   []uint8
			uint16ArrayAlias  []uint16
			uint32ArrayAlias  []uint32
			uint64ArrayAlias  []uint64
			uintArrayAlias    []uint
			float32ArrayAlias []float32
			float64ArrayAlias []float64
			stringArrayAlias  []string
		)

		var (
			boolAliasValue    = booleanAlias(true)
			byteAliasValue    = byteAlias('A')
			int8AliasValue    = int8Alias(127)
			int16AliasValue   = int16Alias(-512)
			int32AliasValue   = int32Alias(-1024)
			int64AliasValue   = int64Alias(-124798272)
			intAliasValue     = intAlias(-98937323493)
			uint8AliasValue   = uint8Alias(127)
			uint16AliasValue  = uint16Alias(512)
			uint32AliasValue  = uint32Alias(1024)
			uint64AliasValue  = uint64Alias(124798272)
			uintAliasValue    = uintAlias(98937323493)
			float32AliasValue = float32Alias(12.5863)
			float64AliasValue = float64Alias(873983.24239)
			stringAliasValue  = stringAlias("a string with alias type")
		)

		DescribeTable("should be able to send and receive nil pointers",
			func(value interface{}) {
				result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
				Expect(err).To(BeNil())

				Expect(result.Next()).To(BeTrue())
				Expect(result.Record().Values[0]).To(BeNil())
				Expect(result.Next()).To(BeFalse())
				Expect(result.Err()).To(BeNil())
			},
			Entry("boolean", (*booleanAlias)(nil)),
			Entry("byte", (*byteAlias)(nil)),
			Entry("int8", (*int8Alias)(nil)),
			Entry("int16", (*int16Alias)(nil)),
			Entry("int32", (*int32Alias)(nil)),
			Entry("int64", (*int64Alias)(nil)),
			Entry("int", (*intAlias)(nil)),
			Entry("uint8", (*uint8Alias)(nil)),
			Entry("uint16", (*uint16Alias)(nil)),
			Entry("uint32", (*uint32Alias)(nil)),
			Entry("uint64", (*uint64Alias)(nil)),
			Entry("uint", (*uintAlias)(nil)),
			Entry("float32", (*float32Alias)(nil)),
			Entry("float64", (*float64Alias)(nil)),
			Entry("string", (*stringAlias)(nil)),
			Entry("boolean array", (*[]booleanAlias)(nil)),
			Entry("byte array", (*[]byteAlias)(nil)),
			Entry("int8 array", (*[]int8Alias)(nil)),
			Entry("int16 array", (*[]int16Alias)(nil)),
			Entry("int32 array", (*[]int32Alias)(nil)),
			Entry("int64 array", (*[]int64Alias)(nil)),
			Entry("int array", (*[]intAlias)(nil)),
			Entry("uint8 array", (*[]uint8Alias)(nil)),
			Entry("uint16 array", (*[]uint16Alias)(nil)),
			Entry("uint32 array", (*[]uint32Alias)(nil)),
			Entry("uint64 array", (*[]uint64Alias)(nil)),
			Entry("uint array", (*[]uintAlias)(nil)),
			Entry("float32 array", (*[]float32Alias)(nil)),
			Entry("float64 array", (*[]float64Alias)(nil)),
			Entry("string array", (*[]stringAlias)(nil)),
			Entry("boolean array alias", (*booleanArrayAlias)(nil)),
			Entry("byte array alias", (*byteArrayAlias)(nil)),
			Entry("int8 array alias", (*int8ArrayAlias)(nil)),
			Entry("int16 array alias", (*int16ArrayAlias)(nil)),
			Entry("int32 array alias", (*int32ArrayAlias)(nil)),
			Entry("int64 array alias", (*int64ArrayAlias)(nil)),
			Entry("int array alias", (*intArrayAlias)(nil)),
			Entry("uint8 array alias", (*uint8ArrayAlias)(nil)),
			Entry("uint16 array alias", (*uint16ArrayAlias)(nil)),
			Entry("uint32 array alias", (*uint32ArrayAlias)(nil)),
			Entry("uint64 array alias", (*uint64ArrayAlias)(nil)),
			Entry("uint array alias", (*uintArrayAlias)(nil)),
			Entry("float32 array alias", (*float32ArrayAlias)(nil)),
			Entry("float64 array alias", (*float64ArrayAlias)(nil)),
			Entry("string array alias", (*stringArrayAlias)(nil)),
		)

		DescribeTable("should be able to send aliased types",
			func(value interface{}, expected interface{}) {
				assertEquals := func(actual interface{}) {
					actualValue := reflect.ValueOf(actual)

					if actualValue.Kind() == reflect.Slice {
						expectedItems := make([]interface{}, actualValue.Len())

						for i := 0; i < actualValue.Len(); i++ {
							expectedItems[i] = actualValue.Index(i).Interface()
						}

						Expect(actualValue.Interface()).Should(ConsistOf(expectedItems...))
					} else {
						Expect(actualValue.Interface()).Should(BeEquivalentTo(expected))
					}
				}

				result, err = session.Run("CREATE (n {value1: $value1, value2: $value2}) RETURN n.value1, n.value2", map[string]interface{}{"value1": value, "value2": &value})
				Expect(err).To(BeNil())

				Expect(result.Next()).To(BeTrue())
				assertEquals(result.Record().Values[0])
				assertEquals(result.Record().Values[1])
				Expect(result.Next()).To(BeFalse())
				Expect(result.Err()).To(BeNil())
			},
			Entry("boolean", boolAliasValue, bool(boolAliasValue)),
			Entry("byte", byteAliasValue, byte(byteAliasValue)),
			Entry("int8", int8AliasValue, int8(int8AliasValue)),
			Entry("int16", int16AliasValue, int16(int16AliasValue)),
			Entry("int32", int32AliasValue, int32(int32AliasValue)),
			Entry("int64", int64AliasValue, int64(int64AliasValue)),
			Entry("int", intAliasValue, int(intAliasValue)),
			Entry("uint8", uint8AliasValue, uint8(uint8AliasValue)),
			Entry("uint16", uint16AliasValue, uint16(uint16AliasValue)),
			Entry("uint32", uint32AliasValue, uint32(uint32AliasValue)),
			Entry("uint64", uint64AliasValue, uint64(uint64AliasValue)),
			Entry("uint", uintAliasValue, uint(uintAliasValue)),
			Entry("float32", float32AliasValue, float32(float32AliasValue)),
			Entry("float64", float64AliasValue, float64(float64AliasValue)),
			Entry("string", stringAliasValue, string(stringAliasValue)),
			Entry("boolean array", []booleanAlias{true, false}, []bool{true, false}),
			Entry("byte array", []byteAlias{'A', 'B', 'C'}, []byte{'A', 'B', 'C'}),
			Entry("int8 array", []int8Alias{-5, -10, 1, 2, 45}, []int8{-5, -10, 1, 2, 45}),
			Entry("int16 array", []int16Alias{-412, 9, 0, 124}, []int16{-412, 9, 0, 124}),
			Entry("int32 array", []int32Alias{-138923, 3123, 2120021312}, []int32{-138923, 3123, 2120021312}),
			Entry("int64 array", []int64Alias{-1322489234, 1239817239821, -1}, []int64{-1322489234, 1239817239821, -1}),
			Entry("int array", []intAlias{1123213, -23423442, 83282347423}, []int{1123213, -23423442, 83282347423}),
			Entry("uint8 array", []uint8Alias{0, 4, 128}, []uint8{0, 4, 128}),
			Entry("uint16 array", []uint16Alias{12, 5534, 21333}, []uint16{12, 5534, 21333}),
			Entry("uint32 array", []uint32Alias{21323, 12355343, 3545364}, []uint32{21323, 12355343, 3545364}),
			Entry("uint64 array", []uint64Alias{129389, 123, 0, 24294323}, []uint64{129389, 123, 0, 24294323}),
			Entry("uint array", []uintAlias{12309312, 120398213}, []uint{12309312, 120398213}),
			Entry("float32 array", []float32Alias{12.5863, 32424.43534}, []float32{12.5863, 32424.43534}),
			Entry("float64 array", []float64Alias{873983.24239, 249872384.9723}, []float64{873983.24239, 249872384.9723}),
			Entry("string array", []stringAlias{"string 1", "string 2"}, []string{"string 1", "string 2"}),
			Entry("boolean array alias", booleanArrayAlias{true, false}, []bool{true, false}),
			Entry("byte array alias", byteArrayAlias{'A', 'B', 'C'}, []byte{'A', 'B', 'C'}),
			Entry("int8 array alias", int8ArrayAlias{-5, -10, 1, 2, 45}, []int8{-5, -10, 1, 2, 45}),
			Entry("int16 array alias", int16ArrayAlias{-412, 9, 0, 124}, []int16{-412, 9, 0, 124}),
			Entry("int32 array alias", int32ArrayAlias{-138923, 3123, 2120021312}, []int32{-138923, 3123, 2120021312}),
			Entry("int64 array alias", int64ArrayAlias{-1322489234, 1239817239821, -1}, []int64{-1322489234, 1239817239821, -1}),
			Entry("int array alias", intArrayAlias{1123213, -23423442, 83282347423}, []int{1123213, -23423442, 83282347423}),
			Entry("uint8 array alias", uint8ArrayAlias{0, 4, 128}, []uint8{0, 4, 128}),
			Entry("uint16 array alias", uint16ArrayAlias{12, 5534, 21333}, []uint16{12, 5534, 21333}),
			Entry("uint32 array alias", uint32ArrayAlias{21323, 12355343, 3545364}, []uint32{21323, 12355343, 3545364}),
			Entry("uint64 array alias", uint64ArrayAlias{129389, 123, 0, 24294323}, []uint64{129389, 123, 0, 24294323}),
			Entry("uint array alias", uintArrayAlias{12309312, 120398213}, []uint{12309312, 120398213}),
			Entry("float32 array alias", float32ArrayAlias{12.5863, 32424.43534}, []float32{12.5863, 32424.43534}),
			Entry("float64 array alias", float64ArrayAlias{873983.24239, 249872384.9723}, []float64{873983.24239, 249872384.9723}),
			Entry("string array alias", stringArrayAlias{"string 1", "string 2"}, []string{"string 1", "string 2"}),
		)
	})
})
