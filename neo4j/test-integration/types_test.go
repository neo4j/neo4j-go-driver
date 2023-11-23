/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test_integration

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/test-integration/dbserver"
)

func TestTypes(outer *testing.T) {
	if testing.Short() {
		outer.Skip()
	}

	ctx := context.Background()
	server := dbserver.GetDbServer(ctx)
	var err error
	var driver neo4j.DriverWithContext
	var session neo4j.SessionWithContext
	var result neo4j.ResultWithContext

	driver = server.Driver()
	session = driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	assertNotNil(outer, session)

	defer func() {
		if session != nil {
			session.Close(ctx)
		}

		if driver != nil {
			driver.Close(ctx)
		}
	}()

	outer.Run("should be able to send and receive boolean property", func(t *testing.T) {
		value := true
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertEquals(t, result.Record().Values[0], value)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive byte property", func(t *testing.T) {
		value := byte(1)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], int64(0))
		assertEquals(t, result.Record().Values[0], int64(value))
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive int8 property", func(t *testing.T) {
		value := int8(2)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], int64(0))
		assertEquals(t, result.Record().Values[0], int64(value))
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive int16 property", func(t *testing.T) {
		value := int16(3)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], int64(0))
		assertEquals(t, result.Record().Values[0], int64(value))
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive int property", func(t *testing.T) {
		value := int(4)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], int64(0))
		assertEquals(t, result.Record().Values[0], int64(value))
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive int32 property", func(t *testing.T) {
		value := int32(5)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], int64(0))
		assertEquals(t, result.Record().Values[0], int64(value))
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive int64 property", func(t *testing.T) {
		value := int64(6)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], int64(0))
		assertEquals(t, result.Record().Values[0], value)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive float32 property", func(t *testing.T) {
		value := float32(7.1)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], float64(0))
		assertEquals(t, result.Record().Values[0], float64(value))
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive float64 property", func(t *testing.T) {
		value := float64(81.9224)
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], float64(0))
		assertEquals(t, result.Record().Values[0], value)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive string property", func(t *testing.T) {
		value := "a string"
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], "")
		assertEquals(t, result.Record().Values[0], value)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive byte array property", func(t *testing.T) {
		value := []byte{1, 2, 3, 4, 5}
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], []byte(nil))
		assertEquals(t, result.Record().Values[0], value)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive boolean array property", func(t *testing.T) {
		value := []bool{true, false, false, true}
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], []any(nil))
		assertEquals(t, result.Record().Values[0], []any{value[0], value[1], value[2], value[3]})
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive int array property", func(t *testing.T) {
		value := []int{1, 2, 3, 4, 5}
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], []any(nil))
		assertEquals(t, result.Record().Values[0], []any{int64(1), int64(2), int64(3), int64(4), int64(5)})
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive float64 array property", func(t *testing.T) {
		value := []float64{1.11, 2.22, 3.33, 4.44, 5.55}
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], []any(nil))
		assertEquals(t, result.Record().Values[0], []any{1.11, 2.22, 3.33, 4.44, 5.55})
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive string array property", func(t *testing.T) {
		value := []string{"a", "b", "c", "d", "e"}
		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], []any(nil))
		assertEquals(t, result.Record().Values[0], []any{"a", "b", "c", "d", "e"})
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive large string property", func(t *testing.T) {
		var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

		randSeq := func(n int) string {
			b := make([]rune, n)
			for i := range b {
				b[i] = letters[rand.Intn(len(letters))]
			}
			return string(b)
		}

		value := randSeq(20 * 1024)

		result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": value})
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		assertAssignableToTypeOf(t, result.Record().Values[0], "")
		assertEquals(t, result.Record().Values[0], value)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to receive a node with properties", func(t *testing.T) {
		result, err = session.Run(ctx, "CREATE (n:Person:Manager {id: 1, name: 'a name'}) RETURN n", nil)
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		node := result.Record().Values[0].(neo4j.Node)

		assertNotNil(t, node)
		//lint:ignore SA1019 Id is supported at least until 6.0
		assertNotNil(t, node.Id)
		assertEquals(t, len(node.Labels), 2)
		assertStringsHas(t, node.Labels, "Person")
		assertStringsHas(t, node.Labels, "Manager")
		assertEquals(t, len(node.Props), 2)
		assertMapHas(t, node.Props, "id", int64(1))
		assertMapHas(t, node.Props, "name", "a name")
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to receive a relationship with properties", func(t *testing.T) {
		result, err = session.Run(ctx, "CREATE (e:Person:Employee {id: 1, name: 'employee 1'})-[w:WORKS_FOR { from: '2017-01-01' }]->(m:Person:Manager {id: 2, name: 'manager 1'}) RETURN e, w, m", nil)
		assertNil(t, err)

		assertTrue(t, result.Next(ctx))
		employee := result.Record().Values[0].(neo4j.Node)
		worksFor := result.Record().Values[1].(neo4j.Relationship)
		manager := result.Record().Values[2].(neo4j.Node)

		assertNotNil(t, employee)
		//lint:ignore SA1019 Id is supported at least until 6.0
		assertNotNil(t, employee.Id)
		assertEquals(t, len(employee.Labels), 2)
		assertStringsHas(t, employee.Labels, "Person")
		assertStringsHas(t, employee.Labels, "Employee")
		assertEquals(t, len(employee.Props), 2)
		assertMapHas(t, employee.Props, "id", int64(1))
		assertMapHas(t, employee.Props, "name", "employee 1")

		assertNotNil(t, worksFor)
		//lint:ignore SA1019 Id is supported at least until 6.0
		assertNotNil(t, worksFor.Id)
		//lint:ignore SA1019 StartId is supported at least until 6.0
		assertEquals(t, worksFor.StartId, employee.Id)
		//lint:ignore SA1019 EndId is supported at least until 6.0
		assertEquals(t, worksFor.EndId, manager.Id)
		assertEquals(t, worksFor.Type, "WORKS_FOR")
		assertEquals(t, len(worksFor.Props), 1)
		assertMapHas(t, worksFor.Props, "from", "2017-01-01")

		assertNotNil(t, manager)
		//lint:ignore SA1019 Id is supported at least until 6.0
		assertNotNil(t, manager.Id)
		assertEquals(t, len(manager.Labels), 2)
		assertStringsHas(t, manager.Labels, "Person")
		assertStringsHas(t, manager.Labels, "Manager")
		assertEquals(t, len(manager.Props), 2)
		assertMapHas(t, manager.Props, "id", int64(2))
		assertMapHas(t, manager.Props, "name", "manager 1")
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to receive a path with two nodes and one relationship", func(t *testing.T) {
		result, err = session.Run(ctx, "CREATE p=(e:Person:Employee {id: 1, name: 'employee 1'})-[w:WORKS_FOR { from: '2017-01-01' }]->(m:Person:Manager {id: 2, name: 'manager 1'}) RETURN p, e, w, m", nil)
		assertNil(t, err)
		assertTrue(t, result.Next(ctx))
		path := result.Record().Values[0].(neo4j.Path)
		employee := result.Record().Values[1].(neo4j.Node)
		worksFor := result.Record().Values[2].(neo4j.Relationship)
		manager := result.Record().Values[3].(neo4j.Node)

		assertNotNil(t, path)
		assertEquals(t, len(path.Nodes), 2)
		assertNodesHas(t, path.Nodes, employee)
		assertNodesHas(t, path.Nodes, manager)
		assertEquals(t, len(path.Relationships), 1)
		assertRelationshipsHas(t, path.Relationships, worksFor)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to receive a path with three nodes and two relationship", func(t *testing.T) {
		result, err = session.Run(ctx, `CREATE (e1:Person:Employee $employee) 
				CREATE (l1:Person:Lead $lead) 
				CREATE (m1:Person:Manager $manager) 
				CREATE p = (e1)-[r1:LED_BY { from: '2017-01-01' }]->(l1)-[r2:REPORTS_TO { from: '2017-01-01' }]->(m1)
				RETURN p, e1, l1, m1, r1, r2`, map[string]any{
			"employee": map[string]any{
				"id":   1,
				"name": "employee 1",
			},
			"lead": map[string]any{
				"id":   2,
				"name": "lead 1",
			},
			"manager": map[string]any{
				"id":   3,
				"name": "manager 1",
			},
		})
		assertNil(t, err)
		assertTrue(t, result.Next(ctx))

		path := result.Record().Values[0].(neo4j.Path)
		employee := result.Record().Values[1].(neo4j.Node)
		lead := result.Record().Values[2].(neo4j.Node)
		manager := result.Record().Values[3].(neo4j.Node)
		ledBy := result.Record().Values[4].(neo4j.Relationship)
		reportsTo := result.Record().Values[5].(neo4j.Relationship)

		assertNotNil(t, path)
		assertEquals(t, len(path.Nodes), 3)
		assertNodesHas(t, path.Nodes, employee)
		assertNodesHas(t, path.Nodes, lead)
		assertNodesHas(t, path.Nodes, manager)
		assertEquals(t, len(path.Relationships), 2)
		assertRelationshipsHas(t, path.Relationships, ledBy)
		assertRelationshipsHas(t, path.Relationships, reportsTo)
		assertFalse(t, result.Next(ctx))
		assertNil(t, result.Err())
	})

	outer.Run("should be able to send and receive nil pointer property", func(inner *testing.T) {
		nilPointers := map[string]any{
			"boolean":       (*bool)(nil),
			"byte":          (*byte)(nil),
			"int8":          (*int8)(nil),
			"int16":         (*int16)(nil),
			"int32":         (*int32)(nil),
			"int64":         (*int64)(nil),
			"int":           (*int)(nil),
			"uint8":         (*uint8)(nil),
			"uint16":        (*uint16)(nil),
			"uint32":        (*uint32)(nil),
			"uint64":        (*uint64)(nil),
			"uint":          (*uint)(nil),
			"float32":       (*float32)(nil),
			"float64":       (*float64)(nil),
			"string":        (*string)(nil),
			"boolean array": (*[]bool)(nil),
			"byte array":    (*[]byte)(nil),
			"int8 array":    (*[]int8)(nil),
			"int16 array":   (*[]int16)(nil),
			"int32 array":   (*[]int32)(nil),
			"int64 array":   (*[]int64)(nil),
			"int array":     (*[]int)(nil),
			"uint8 array":   (*[]uint8)(nil),
			"uint16 array":  (*[]uint16)(nil),
			"uint32 array":  (*[]uint32)(nil),
			"uint64 array":  (*[]uint64)(nil),
			"uint array":    (*[]uint)(nil),
			"float32 array": (*[]float32)(nil),
			"float64 array": (*[]float64)(nil),
			"string array":  (*[]string)(nil),
		}

		for _, k := range sortedKeys(nilPointers) {
			inner.Run(fmt.Sprintf("with %s type", k), func(t *testing.T) {
				result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": nilPointers[k]})
				assertNil(t, err)

				assertTrue(t, result.Next(ctx))
				assertNil(t, result.Record().Values[0])
				assertFalse(t, result.Next(ctx))
				assertNil(t, result.Err())
			})
		}

	})

	outer.Run("Un-convertible Go types", func(inner *testing.T) {
		type unsupportedType struct {
		}

		inner.Run("Session.Run", func(deepT *testing.T) {
			deepT.Run("should fail when sending as parameter", func(t *testing.T) {
				result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": unsupportedType{}})
				assertNotNil(t, err)
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert parameter \"value\" to connector value for run message")))
				assertNil(t, result)
			})

			deepT.Run("should fail when sending as tx metadata", func(t *testing.T) {
				result, err = session.Run(ctx, "CREATE (n)", nil, neo4j.WithTxMetadata(map[string]any{"m1": unsupportedType{}}))
				assertNotNil(t, err)
				//Expect(err).To(BeGenericError(ContainSubstring("unable to convert tx metadata to connector value for run message")))
				assertNil(t, result)
			})
		})

		inner.Run("Transaction.Run", func(deepT *testing.T) {
			var tx neo4j.ExplicitTransaction

			deepT.Run("should fail when sending as tx metadata", func(t *testing.T) {
				tx, err = session.BeginTransaction(ctx)
				defer assertCloses(ctx, deepT, tx)
				assertNil(t, err)
				assertNotNil(t, tx)

				result, err = tx.Run(ctx, "CREATE (n)", map[string]any{"unsupported": unsupportedType{}})
				assertNotNil(t, err)
				assertNil(t, result)
			})
		})
	})

	outer.Run("Aliased types", func(inner *testing.T) {
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

		inner.Run("should be able to send and receive nil pointers", func(deepT *testing.T) {
			nilPointers := map[string]any{
				"boolean":             (*booleanAlias)(nil),
				"byte":                (*byteAlias)(nil),
				"int8":                (*int8Alias)(nil),
				"int16":               (*int16Alias)(nil),
				"int32":               (*int32Alias)(nil),
				"int64":               (*int64Alias)(nil),
				"int":                 (*intAlias)(nil),
				"uint8":               (*uint8Alias)(nil),
				"uint16":              (*uint16Alias)(nil),
				"uint32":              (*uint32Alias)(nil),
				"uint64":              (*uint64Alias)(nil),
				"uint":                (*uintAlias)(nil),
				"float32":             (*float32Alias)(nil),
				"float64":             (*float64Alias)(nil),
				"string":              (*stringAlias)(nil),
				"boolean array":       (*[]booleanAlias)(nil),
				"byte array":          (*[]byteAlias)(nil),
				"int8 array":          (*[]int8Alias)(nil),
				"int16 array":         (*[]int16Alias)(nil),
				"int32 array":         (*[]int32Alias)(nil),
				"int64 array":         (*[]int64Alias)(nil),
				"int array":           (*[]intAlias)(nil),
				"uint8 array":         (*[]uint8Alias)(nil),
				"uint16 array":        (*[]uint16Alias)(nil),
				"uint32 array":        (*[]uint32Alias)(nil),
				"uint64 array":        (*[]uint64Alias)(nil),
				"uint array":          (*[]uintAlias)(nil),
				"float32 array":       (*[]float32Alias)(nil),
				"float64 array":       (*[]float64Alias)(nil),
				"string array":        (*[]stringAlias)(nil),
				"boolean array alias": (*booleanArrayAlias)(nil),
				"byte array alias":    (*byteArrayAlias)(nil),
				"int8 array alias":    (*int8ArrayAlias)(nil),
				"int16 array alias":   (*int16ArrayAlias)(nil),
				"int32 array alias":   (*int32ArrayAlias)(nil),
				"int64 array alias":   (*int64ArrayAlias)(nil),
				"int array alias":     (*intArrayAlias)(nil),
				"uint8 array alias":   (*uint8ArrayAlias)(nil),
				"uint16 array alias":  (*uint16ArrayAlias)(nil),
				"uint32 array alias":  (*uint32ArrayAlias)(nil),
				"uint64 array alias":  (*uint64ArrayAlias)(nil),
				"uint array alias":    (*uintArrayAlias)(nil),
				"float32 array alias": (*float32ArrayAlias)(nil),
				"float64 array alias": (*float64ArrayAlias)(nil),
				"string array alias":  (*stringArrayAlias)(nil),
			}

			for _, k := range sortedKeys(nilPointers) {
				deepT.Run(fmt.Sprintf("with %s type", k), func(t *testing.T) {
					result, err = session.Run(ctx, "CREATE (n {value: $value}) RETURN n.value", map[string]any{"value": nilPointers[k]})
					assertNil(t, err)

					assertTrue(t, result.Next(ctx))
					assertNil(t, result.Record().Values[0])
					assertFalse(t, result.Next(ctx))
					assertNil(t, result.Err())
				})
			}

		},
		)

		inner.Run("should be able to send aliased types", func(deepT *testing.T) {

			type aliasedValue struct {
				aliased any
				value   any
			}

			sortedKeys := func(m map[string]aliasedValue) []string {
				result := make([]string, len(m))
				i := 0
				for k := range m {
					result[i] = k
					i++
				}
				sort.Strings(result)
				return result
			}

			aliases := map[string]aliasedValue{
				"boolean":             {boolAliasValue, bool(boolAliasValue)},
				"byte":                {byteAliasValue, byte(byteAliasValue)},
				"int8":                {int8AliasValue, int8(int8AliasValue)},
				"int16":               {int16AliasValue, int16(int16AliasValue)},
				"int32":               {int32AliasValue, int32(int32AliasValue)},
				"int64":               {int64AliasValue, int64(int64AliasValue)},
				"int":                 {intAliasValue, int(intAliasValue)},
				"uint8":               {uint8AliasValue, uint8(uint8AliasValue)},
				"uint16":              {uint16AliasValue, uint16(uint16AliasValue)},
				"uint32":              {uint32AliasValue, uint32(uint32AliasValue)},
				"uint64":              {uint64AliasValue, uint64(uint64AliasValue)},
				"uint":                {uintAliasValue, uint(uintAliasValue)},
				"float32":             {float32AliasValue, float32(float32AliasValue)},
				"float64":             {float64AliasValue, float64(float64AliasValue)},
				"string":              {stringAliasValue, string(stringAliasValue)},
				"boolean array":       {[]booleanAlias{true, false}, []bool{true, false}},
				"byte array":          {[]byteAlias{'A', 'B', 'C'}, []byte{'A', 'B', 'C'}},
				"int8 array":          {[]int8Alias{-5, -10, 1, 2, 45}, []int8{-5, -10, 1, 2, 45}},
				"int16 array":         {[]int16Alias{-412, 9, 0, 124}, []int16{-412, 9, 0, 124}},
				"int32 array":         {[]int32Alias{-138923, 3123, 2120021312}, []int32{-138923, 3123, 2120021312}},
				"int64 array":         {[]int64Alias{-1322489234, 1239817239821, -1}, []int64{-1322489234, 1239817239821, -1}},
				"int array":           {[]intAlias{1123213, -23423442, 83282347423}, []int{1123213, -23423442, 83282347423}},
				"uint8 array":         {[]uint8Alias{0, 4, 128}, []uint8{0, 4, 128}},
				"uint16 array":        {[]uint16Alias{12, 5534, 21333}, []uint16{12, 5534, 21333}},
				"uint32 array":        {[]uint32Alias{21323, 12355343, 3545364}, []uint32{21323, 12355343, 3545364}},
				"uint64 array":        {[]uint64Alias{129389, 123, 0, 24294323}, []uint64{129389, 123, 0, 24294323}},
				"uint array":          {[]uintAlias{12309312, 120398213}, []uint{12309312, 120398213}},
				"float32 array":       {[]float32Alias{12.5863, 32424.43534}, []float32{12.5863, 32424.43534}},
				"float64 array":       {[]float64Alias{873983.24239, 249872384.9723}, []float64{873983.24239, 249872384.9723}},
				"string array":        {[]stringAlias{"string 1", "string 2"}, []string{"string 1", "string 2"}},
				"boolean array alias": {booleanArrayAlias{true, false}, []bool{true, false}},
				"byte array alias":    {byteArrayAlias{'A', 'B', 'C'}, []byte{'A', 'B', 'C'}},
				"int8 array alias":    {int8ArrayAlias{-5, -10, 1, 2, 45}, []int8{-5, -10, 1, 2, 45}},
				"int16 array alias":   {int16ArrayAlias{-412, 9, 0, 124}, []int16{-412, 9, 0, 124}},
				"int32 array alias":   {int32ArrayAlias{-138923, 3123, 2120021312}, []int32{-138923, 3123, 2120021312}},
				"int64 array alias":   {int64ArrayAlias{-1322489234, 1239817239821, -1}, []int64{-1322489234, 1239817239821, -1}},
				"int array alias":     {intArrayAlias{1123213, -23423442, 83282347423}, []int{1123213, -23423442, 83282347423}},
				"uint8 array alias":   {uint8ArrayAlias{0, 4, 128}, []uint8{0, 4, 128}},
				"uint16 array alias":  {uint16ArrayAlias{12, 5534, 21333}, []uint16{12, 5534, 21333}},
				"uint32 array alias":  {uint32ArrayAlias{21323, 12355343, 3545364}, []uint32{21323, 12355343, 3545364}},
				"uint64 array alias":  {uint64ArrayAlias{129389, 123, 0, 24294323}, []uint64{129389, 123, 0, 24294323}},
				"uint array alias":    {uintArrayAlias{12309312, 120398213}, []uint{12309312, 120398213}},
				"float32 array alias": {float32ArrayAlias{12.5863, 32424.43534}, []float32{12.5863, 32424.43534}},
				"float64 array alias": {float64ArrayAlias{873983.24239, 249872384.9723}, []float64{873983.24239, 249872384.9723}},
				"string array alias":  {stringArrayAlias{"string 1", "string 2"}, []string{"string 1", "string 2"}},
			}

			for _, k := range sortedKeys(aliases) {
				deepT.Run(k, func(t *testing.T) {
					alias := aliases[k]

					result, err = session.Run(ctx, "CREATE (n {value1: $value1, value2: $value2}) RETURN n.value1, n.value2", map[string]any{"value1": alias.aliased, "value2": alias.value})
					assertNil(t, err)

					assertTrue(t, result.Next(ctx))
					assertEquals(t, result.Record().Values[0], alias.aliased)
					assertEquals(t, result.Record().Values[1], alias.value)
					assertNil(t, result.Err())
				})
			}
		},
		)
	})
}
