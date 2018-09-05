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

package test_integration

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/neo4j/neo4j-go-driver/neo4j/test-integration/control"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Types", func() {
	var server *control.SingleInstance
	var err error
	var driver neo4j.Driver
	var session neo4j.Session
	var result neo4j.Result

	BeforeEach(func() {
		server, err = control.EnsureSingleInstance()
		Expect(err).To(BeNil())
		Expect(server).NotTo(BeNil())

		driver, err = server.Driver()
		Expect(err).To(BeNil())
		Expect(driver).NotTo(BeNil())

		session, err = driver.Session(neo4j.AccessModeRead)
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

		if result.Next() {
			Expect(result.Record().GetByIndex(0)).To(Equal(value))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to send and receive byte property", func() {
		value := byte(1)
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
		result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", map[string]interface{}{"value": value})
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
	//
	//	result, err = session.Run("CREATE (n {value: $value}) RETURN n.value", &map[string]interface{}{"value": value})
	//	Expect(err).To(BeNil())
	//
	//	if result.Next() {
	//		Expect(result.Record().GetByIndex(0)).To(BeAssignableToTypeOf(""))
	//		Expect(result.Record().GetByIndex(0)).To(Equal(value))
	//	}
	//	Expect(result.Next()).To(BeFalse())
	//	Expect(result.Err()).To(BeNil())
	//})

	It("should be able to receive a node with properties", func() {
		result, err = session.Run("CREATE (n:Person:Manager {id: 1, name: 'a name'}) RETURN n", nil)
		Expect(err).To(BeNil())

		if result.Next() {
			node := result.Record().GetByIndex(0).(neo4j.Node)

			Expect(node).NotTo(BeNil())
			Expect(node.Id()).NotTo(BeNil())
			Expect(node.Labels()).To(HaveLen(2))
			Expect(node.Labels()).To(ContainElement("Person"))
			Expect(node.Labels()).To(ContainElement("Manager"))
			Expect(node.Props()).To(HaveLen(2))
			Expect(node.Props()).To(HaveKeyWithValue("id", int64(1)))
			Expect(node.Props()).To(HaveKeyWithValue("name", "a name"))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to receive a relationship with properties", func() {
		result, err = session.Run("CREATE (e:Person:Employee {id: 1, name: 'employee 1'})-[w:WORKS_FOR { from: '2017-01-01' }]->(m:Person:Manager {id: 2, name: 'manager 1'}) RETURN e, w, m", nil)
		Expect(err).To(BeNil())

		if result.Next() {
			employee := result.Record().GetByIndex(0).(neo4j.Node)
			worksFor := result.Record().GetByIndex(1).(neo4j.Relationship)
			manager := result.Record().GetByIndex(2).(neo4j.Node)

			Expect(employee).NotTo(BeNil())
			Expect(employee.Id()).NotTo(BeNil())
			Expect(employee.Labels()).To(HaveLen(2))
			Expect(employee.Labels()).To(ContainElement("Person"))
			Expect(employee.Labels()).To(ContainElement("Employee"))
			Expect(employee.Props()).To(HaveLen(2))
			Expect(employee.Props()).To(HaveKeyWithValue("id", int64(1)))
			Expect(employee.Props()).To(HaveKeyWithValue("name", "employee 1"))

			Expect(worksFor).NotTo(BeNil())
			Expect(worksFor.Id()).NotTo(BeNil())
			Expect(worksFor.StartId()).To(Equal(employee.Id()))
			Expect(worksFor.EndId()).To(Equal(manager.Id()))
			Expect(worksFor.Type()).To(Equal("WORKS_FOR"))
			Expect(worksFor.Props()).To(HaveLen(1))
			Expect(worksFor.Props()).To(HaveKeyWithValue("from", "2017-01-01"))

			Expect(manager).NotTo(BeNil())
			Expect(manager.Id()).NotTo(BeNil())
			Expect(manager.Labels()).To(HaveLen(2))
			Expect(manager.Labels()).To(ContainElement("Person"))
			Expect(manager.Labels()).To(ContainElement("Manager"))
			Expect(manager.Props()).To(HaveLen(2))
			Expect(manager.Props()).To(HaveKeyWithValue("id", int64(2)))
			Expect(manager.Props()).To(HaveKeyWithValue("name", "manager 1"))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

	It("should be able to receive a path with two nodes and one relationship", func() {
		result, err = session.Run("CREATE p=(e:Person:Employee {id: 1, name: 'employee 1'})-[w:WORKS_FOR { from: '2017-01-01' }]->(m:Person:Manager {id: 2, name: 'manager 1'}) RETURN p, e, w, m", nil)
		Expect(err).To(BeNil())

		if result.Next() {
			path := result.Record().GetByIndex(0).(neo4j.Path)
			employee := result.Record().GetByIndex(1).(neo4j.Node)
			worksFor := result.Record().GetByIndex(2).(neo4j.Relationship)
			manager := result.Record().GetByIndex(3).(neo4j.Node)

			Expect(path).NotTo(BeNil())
			Expect(path.Nodes()).To(HaveLen(2))
			Expect(path.Nodes()).To(ContainElement(employee))
			Expect(path.Nodes()).To(ContainElement(manager))
			Expect(path.Relationships()).To(HaveLen(1))
			Expect(path.Relationships()).To(ContainElement(worksFor))
		}
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

		if result.Next() {
			path := result.Record().GetByIndex(0).(neo4j.Path)
			employee := result.Record().GetByIndex(1).(neo4j.Node)
			lead := result.Record().GetByIndex(2).(neo4j.Node)
			manager := result.Record().GetByIndex(3).(neo4j.Node)
			ledBy := result.Record().GetByIndex(4).(neo4j.Relationship)
			reportsTo := result.Record().GetByIndex(5).(neo4j.Relationship)

			Expect(path).NotTo(BeNil())
			Expect(path.Nodes()).To(HaveLen(3))
			Expect(path.Nodes()).To(ContainElement(employee))
			Expect(path.Nodes()).To(ContainElement(lead))
			Expect(path.Nodes()).To(ContainElement(manager))
			Expect(path.Relationships()).To(HaveLen(2))
			Expect(path.Relationships()).To(ContainElement(ledBy))
			Expect(path.Relationships()).To(ContainElement(reportsTo))
		}
		Expect(result.Next()).To(BeFalse())
		Expect(result.Err()).To(BeNil())
	})

})
