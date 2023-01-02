/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ogm_test

import (
	"context"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/ogm"
	"testing"
)

func TestMapSingle(outer *testing.T) {
	outer.Parallel()

	ctx := context.Background()

	outer.Run("when getting a single node", func(inner *testing.T) {

		inner.Run("maps to simple struct", func(t *testing.T) {
			type Country struct {
				Labels   []string `neo4j:"mapping_type=labels"`
				Id       int64    `neo4j:"mapping_type=id"`
				BetterId string   `neo4j:"mapping_type=element_id"`
				Name     string   `neo4j:"mapping_type=property,name=country"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Id:        42,
						ElementId: "42",
						Labels:    []string{"Country"},
						Props:     map[string]any{"country": "France"},
					}}},
				},
			}

			country, err := ogm.MapSingle[Country](ctx, session, "MATCH (c:Country) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, Country{
				Labels:   []string{"Country"},
				Id:       42,
				BetterId: "42",
				Name:     "France",
			})
		})

		inner.Run("maps simple struct and ignores fields without the neo4j tag", func(t *testing.T) {
			type Country struct {
				Labels   []string `neo4j:"mapping_type=labels"`
				Id       int64    `neo4j:"mapping_type=id"`
				BetterId string   `neo4j:"mapping_type=element_id"`
				Name     string   `neo4j:"mapping_type=property,name=country"`
				Ignored1 bool     `json:"ignored"`
				Ignored2 int
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Id:        42,
						ElementId: "42",
						Labels:    []string{"Country"},
						Props:     map[string]any{"country": "France"},
					}}},
				},
			}

			country, err := ogm.MapSingle[Country](ctx, session, "MATCH (c:Country) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, Country{
				Labels:   []string{"Country"},
				Id:       42,
				BetterId: "42",
				Name:     "France",
			})
		})

		inner.Run("maps to simple struct with property bag", func(t *testing.T) {
			type Country struct {
				Props map[string]any `neo4j:"mapping_type=properties"`
			}
			nodeProps := map[string]any{"country": "France", "phone_prefix": 33, "true_is": true}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Id:        42,
						ElementId: "42",
						Labels:    []string{"Country"},
						Props:     nodeProps,
					}}},
				},
			}

			country, err := ogm.MapSingle[Country](ctx, session, "MATCH (c:Country) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, Country{Props: nodeProps})
		})

		inner.Run("maps to pointer of struct", func(t *testing.T) {
			type Company struct {
				Labels []string `neo4j:"mapping_type=labels"`
				Id     string   `neo4j:"mapping_type=element_id"`
				Domain string   `neo4j:"name=domain,mapping_type=property"`
				Name   string   `neo4j:"name=name,mapping_type=property"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						ElementId: "42",
						Labels:    []string{"Company"},
						Props:     map[string]any{"name": "Neo4j", "domain": "IT"},
					}}},
				},
			}

			country, err := ogm.MapSingle[*Company](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, &Company{
				Labels: []string{"Company"},
				Id:     "42",
				Domain: "IT",
				Name:   "Neo4j",
			})
		})

		inner.Run("maps to pointer of struct with property bag", func(t *testing.T) {
			type Country struct {
				Props map[string]any `neo4j:"mapping_type=properties"`
			}
			nodeProps := map[string]any{"country": "France", "phone_prefix": 33, "true_is": true}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Props: nodeProps,
					}}},
				},
			}

			country, err := ogm.MapSingle[*Country](ctx, session, "MATCH (c:Country) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, &Country{Props: nodeProps})
		})

		inner.Run("maps to dictionary's properties", func(t *testing.T) {
			nodeProperties := map[string]any{"name": "Neo4j", "domain": "IT", "answer": int64(42)}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Props: nodeProperties}}},
				},
			}

			dictionary, err := ogm.MapSingle[map[string]any](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, dictionary, nodeProperties)
		})

		inner.Run("maps a pointer to dictionary from entity properties", func(t *testing.T) {
			nodeProperties := map[string]any{"name": "Neo4j", "domain": "IT", "answer": int64(42)}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Props: nodeProperties}}},
				},
			}

			dictionary, err := ogm.MapSingle[*map[string]any](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, dictionary, &nodeProperties)
		})

		inner.Run("fails to map single result when session run fails", func(t *testing.T) {
			runErr := errors.New("oopsie")
			session := &fakeSession{RunErr: runErr}

			_, err := ogm.MapSingle[any](ctx, session, "SELECT * FROM oops", nil)

			AssertDeepEquals(t, err, runErr)
		})

		inner.Run("fails to map single result when single record extraction fails", func(t *testing.T) {
			singleErr := errors.New("oopsie")
			session := &fakeSession{RunResult: &fakeResult{SingleErr: singleErr}}

			_, err := ogm.MapSingle[any](ctx, session, "CALL point.ofNoReturn()", nil)

			AssertDeepEquals(t, err, singleErr)
		})

		inner.Run("fails to map if the property name is not specified when mapping a property", func(t *testing.T) {
			type Value struct {
				Value string `neo4j:"mapping_type=property"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Labels: []string{"Value"},
						Props:  map[string]any{"value": "relationships, we do"},
					}}},
				},
			}

			_, err := ogm.MapSingle[Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`the property name is missing for field "Value" of type ogm_test.Value`)
		})

		inner.Run("fails to map if the property name is specified when mapping an ID", func(t *testing.T) {
			type Value struct {
				Id int64 `neo4j:"mapping_type=id,name=id"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Id: int64(42),
					}}},
				},
			}

			_, err := ogm.MapSingle[Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`the property name "id" on the field "Id" of ogm_test.Value must be removed when mapping ID`)
		})

		inner.Run("fails to map if the property name is specified when mapping an element ID", func(t *testing.T) {
			type Value struct {
				Id string `neo4j:"mapping_type=element_id,name=element_id"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						ElementId: "42",
					}}},
				},
			}

			_, err := ogm.MapSingle[Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`the property name "element_id" on the field "Id" of ogm_test.Value must be removed when mapping element ID`)
		})

		inner.Run("fails to map if the property name is specified when mapping labels", func(t *testing.T) {
			type Value struct {
				Labels []string `neo4j:"mapping_type=labels,name=labels"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Labels: []string{"Labello"},
					}}},
				},
			}

			_, err := ogm.MapSingle[Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`the property name "labels" on the field "Labels" of ogm_test.Value must be removed when mapping labels`)
		})

		inner.Run("fails to map if the property name is specified when mapping a property bag", func(t *testing.T) {
			type Value struct {
				Props map[string]string `neo4j:"mapping_type=properties,name=properties"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Props: map[string]any{"prop": "ale"},
					}}},
				},
			}

			_, err := ogm.MapSingle[Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`the property name "properties" on the field "Props" of ogm_test.Value must be removed when mapping bag of properties`)
		})

		inner.Run("fails to map if missing value of node property is mapped to non-nilable type", func(t *testing.T) {
			type Value struct {
				Value string `neo4j:"mapping_type=property,name=value"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Labels: []string{"Value"},
						Props:  nil,
					}}},
				},
			}

			_, err := ogm.MapSingle[Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`the value of property "value" is nil, but the type of the field "Value" of type ogm_test.Value is not nilable`)
		})

		inner.Run("fails to map to pointer of pointer of struct", func(t *testing.T) {
			type Value struct {
				Value string `neo4j:"mapping_type=property,name=value"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Labels: []string{"Value"},
					}}},
				},
			}

			_, err := ogm.MapSingle[**Value](ctx, session, "MATCH (v:Value) RETURN v LIMIT 1", nil)

			AssertErrorMessageContains(t, err, "only struct, map, struct pointer and map pointer types are supported, given: **ogm_test.Value")
		})

		inner.Run("fails to map id to invalid field type", func(t *testing.T) {
			type Thing struct {
				Id string `neo4j:"mapping_type=id"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Labels: []string{"Thing"}}}},
				},
			}

			_, err := ogm.MapSingle[Thing](ctx, session, "MATCH (t:Thing) RETURN t LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`failed setting ID to field "Id" of type ogm_test.Thing, expected type int64 but this error occurred: reflect: call of reflect.Value.SetInt on string Value`)
		})

		inner.Run("fails to map element id to invalid field type", func(t *testing.T) {
			type Thing struct {
				Id bool `neo4j:"mapping_type=element_id"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Labels: []string{"Thing"}}}},
				},
			}

			_, err := ogm.MapSingle[Thing](ctx, session, "MATCH (t:Thing) RETURN t LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`failed setting element ID to field "Id" of type ogm_test.Thing, expected type string but this error occurred: reflect: call of reflect.Value.SetString on bool Value`)
		})

		inner.Run("fails to map labels to invalid field type", func(t *testing.T) {
			type Thing struct {
				Labels []int `neo4j:"mapping_type=labels"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Labels: []string{"Thing"}}}},
				},
			}

			_, err := ogm.MapSingle[Thing](ctx, session, "MATCH (t:Thing) RETURN t LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`failed setting labels to field "Labels" of type ogm_test.Thing, expected type []string but this error occurred: reflect.Set: value of type []string is not assignable to type []int`)
		})

		inner.Run("fails to map property to invalid field type", func(t *testing.T) {
			type Thing struct {
				Age chan string `neo4j:"mapping_type=property,name=age"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Props: map[string]any{"age": int64(42)}}}},
				},
			}

			_, err := ogm.MapSingle[Thing](ctx, session, "MATCH (t:Thing) RETURN t LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`failed setting property "age" to field "Age" of type ogm_test.Thing, expected type []string but this error occurred: reflect.Set: value of type int64 is not assignable to type chan string`)
		})

		inner.Run("fails to map property bag to invalid field type", func(t *testing.T) {
			type Thing struct {
				Props chan string `neo4j:"mapping_type=properties"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{Props: map[string]any{"age": int64(42)}}}},
				},
			}

			_, err := ogm.MapSingle[Thing](ctx, session, "MATCH (t:Thing) RETURN t LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`failed setting bag of properties to field "Props" of type ogm_test.Thing, expected type []string but this error occurred: reflect.Set: value of type map[string]interface {} is not assignable to type chan string`)
		})

		inner.Run("fails to map to invalid map type's properties", func(t *testing.T) {
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Props: map[string]any{"name": "Neo4j", "domain": "IT", "answer": int64(42)}}}},
				},
			}

			_, err := ogm.MapSingle[map[bool]any](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertErrorMessageContains(t, err, "failed setting map of type map[bool]interface {}, expected type map[string]interface {} (or pointer thereof) but this error occurred: reflect.Set: value of type map[string]interface {} is not assignable to type map[bool]interface {}")
		})

		inner.Run("fails to map with invalid mapping type", func(t *testing.T) {
			type Company struct {
				Nope string `neo4j:"mapping_type=zglorb,name=wat"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Node{
						Props: map[string]any{"name": "Neo4j", "domain": "IT", "answer": int64(42)}}}},
				},
			}

			_, err := ogm.MapSingle[Company](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertErrorMessageContains(t, err, `unsupported mapping type "zglorb", expected one of "element_id", "id", "labels", "properties", "property", "type"`)
		})

		inner.Run("fails to map labels from relationship", func(t *testing.T) {
			type Company struct {
				Labels []string `neo4j:"mapping_type=labels"`
			}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Relationship{}}},
				},
			}

			_, err := ogm.MapSingle[Company](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertErrorMessageContains(t, err, `expected query to return a dbtype.Node but it returned a dbtype.Relationship`)
		})
	})

	outer.Run("when getting a single relationship", func(inner *testing.T) {
		inner.Run("maps to simple struct", func(t *testing.T) {
			type Country struct {
				Type     string `neo4j:"mapping_type=type"`
				Id       int64  `neo4j:"mapping_type=id"`
				BetterId string `neo4j:"mapping_type=element_id"`
				Name     string `neo4j:"mapping_type=property,name=country"`
			}
			session := &fakeSession{RunResult: &fakeResult{SingleResult: &neo4j.Record{
				Values: []any{neo4j.Relationship{
					Id:        int64(8),
					ElementId: "8",
					Type:      "IN_COUNTRY",
					Props: map[string]any{
						"country": "Syldavia",
					}},
				},
			}}}

			country, err := ogm.MapSingle[Country](ctx, session, "MATCH (tintin)-[r:IN_COUNTRY]->() RETURN r LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, Country{
				Type:     "IN_COUNTRY",
				Id:       int64(8),
				BetterId: "8",
				Name:     "Syldavia",
			})
		})

		inner.Run("maps to pointer of struct", func(t *testing.T) {
			type Country struct {
				Type     string `neo4j:"mapping_type=type"`
				Id       int64  `neo4j:"mapping_type=id"`
				BetterId string `neo4j:"mapping_type=element_id"`
				Name     string `neo4j:"mapping_type=property,name=country"`
			}
			session := &fakeSession{RunResult: &fakeResult{SingleResult: &neo4j.Record{
				Values: []any{neo4j.Relationship{
					Id:        int64(8),
					ElementId: "8",
					Type:      "IN_COUNTRY",
					Props: map[string]any{
						"country": "Syldavia",
					}},
				},
			}}}

			country, err := ogm.MapSingle[*Country](ctx, session, "MATCH (tintin)-[r:IN_COUNTRY]->() RETURN r LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, &Country{
				Type:     "IN_COUNTRY",
				Id:       int64(8),
				BetterId: "8",
				Name:     "Syldavia",
			})
		})

		inner.Run("maps to simple struct with property bag", func(t *testing.T) {
			type Country struct {
				Props map[string]any `neo4j:"mapping_type=properties"`
			}
			nodeProps := map[string]any{"country": "France", "phone_prefix": 33, "true_is": true}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Relationship{
						Props: nodeProps,
					}}},
				},
			}

			country, err := ogm.MapSingle[Country](ctx, session, "MATCH (c:Country) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, Country{Props: nodeProps})
		})

		inner.Run("maps to pointer of struct with property bag", func(t *testing.T) {
			type Country struct {
				Props map[string]any `neo4j:"mapping_type=properties"`
			}
			nodeProps := map[string]any{"country": "France", "phone_prefix": 33, "true_is": true}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Relationship{
						Props: nodeProps,
					}}},
				},
			}

			country, err := ogm.MapSingle[*Country](ctx, session, "MATCH (c:Country) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, country, &Country{Props: nodeProps})
		})

		inner.Run("maps to dictionary's properties", func(t *testing.T) {
			relProperties := map[string]any{"name": "Neo4j", "domain": "IT", "answer": int64(42)}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Relationship{Props: relProperties}}},
				},
			}

			dictionary, err := ogm.MapSingle[map[string]any](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, dictionary, relProperties)
		})

		inner.Run("maps a pointer to dictionary from entity properties", func(t *testing.T) {
			relProperties := map[string]any{"name": "Neo4j", "domain": "IT", "answer": int64(42)}
			session := &fakeSession{
				RunResult: &fakeResult{
					SingleResult: &neo4j.Record{Values: []any{neo4j.Relationship{Props: relProperties}}},
				},
			}

			dictionary, err := ogm.MapSingle[*map[string]any](ctx, session, "MATCH (c:Company) RETURN c LIMIT 1", nil)

			AssertNoError(t, err)
			AssertDeepEquals(t, dictionary, &relProperties)
		})

		inner.Run("fails to map type to invalid field type", func(t *testing.T) {
			type Country struct {
				Type int64 `neo4j:"mapping_type=type"`
			}
			session := &fakeSession{RunResult: &fakeResult{SingleResult: &neo4j.Record{
				Values: []any{neo4j.Relationship{
					Type: "IN_COUNTRY",
				}},
			}}}

			_, err := ogm.MapSingle[*Country](ctx, session, "MATCH (tintin)-[r:IN_COUNTRY]->() RETURN r LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`failed setting type to field "Type" of type *ogm_test.Country, expected type string but this error occurred: reflect.Set: value of type string is not assignable to type int64`)
		})

		inner.Run("fails to map type from Node", func(t *testing.T) {
			type Country struct {
				Type int64 `neo4j:"mapping_type=type"`
			}
			session := &fakeSession{RunResult: &fakeResult{SingleResult: &neo4j.Record{
				Values: []any{neo4j.Node{}},
			}}}

			_, err := ogm.MapSingle[*Country](ctx, session, "MATCH (tintin)-[r:IN_COUNTRY]->() RETURN r LIMIT 1", nil)

			AssertErrorMessageContains(t, err,
				`expected query to return a dbtype.Relationship but it returned a dbtype.Node`)
		})
	})
}

type fakeSession struct {
	neo4j.SessionWithContext
	RunResult neo4j.ResultWithContext
	RunErr    error
}

func (f *fakeSession) LastBookmarks() neo4j.Bookmarks {
	//TODO implement me
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) lastBookmark() string {
	panic("implement me")
}

func (f *fakeSession) BeginTransaction(context.Context, ...func(*neo4j.TransactionConfig)) (neo4j.ExplicitTransaction, error) {
	panic("implement me")
}

func (f *fakeSession) ExecuteRead(context.Context, neo4j.ManagedTransactionWork, ...func(*neo4j.TransactionConfig)) (any, error) {
	panic("implement me")
}

func (f *fakeSession) ExecuteWrite(context.Context, neo4j.ManagedTransactionWork, ...func(*neo4j.TransactionConfig)) (any, error) {
	panic("implement me")
}

func (f *fakeSession) Run(context.Context, string, map[string]any, ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	return f.RunResult, f.RunErr
}

func (f *fakeSession) Close(context.Context) error {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) legacy() neo4j.Session {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeSession) getServerInfo(context.Context) (neo4j.ServerInfo, error) {
	panic("implement me")
}

type fakeResult struct {
	neo4j.ResultWithContext
	SingleResult *neo4j.Record
	SingleErr    error
}

func (f *fakeResult) Keys() ([]string, error) {
	panic("implement me")
}

func (f *fakeResult) NextRecord(context.Context, **neo4j.Record) bool {
	panic("implement me")
}

func (f *fakeResult) Next(context.Context) bool {
	panic("implement me")
}

func (f *fakeResult) PeekRecord(context.Context, **neo4j.Record) bool {
	panic("implement me")
}

func (f *fakeResult) Peek(context.Context) bool {
	panic("implement me")
}

func (f *fakeResult) Err() error {
	panic("implement me")
}

func (f *fakeResult) Record() *neo4j.Record {
	panic("implement me")
}

func (f *fakeResult) Collect(context.Context) ([]*neo4j.Record, error) {
	panic("implement me")
}

func (f *fakeResult) Single(context.Context) (*neo4j.Record, error) {
	return f.SingleResult, f.SingleErr
}

func (f *fakeResult) Consume(context.Context) (neo4j.ResultSummary, error) {
	panic("implement me")
}

func (f *fakeResult) IsOpen() bool {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
func (f *fakeResult) buffer(context.Context) {
	panic("implement me")
}

//lint:ignore U1000 needed for interface adherence
//lint:ignore SA1019 needed for interface adherence
func (f *fakeResult) legacy() neo4j.Result {
	panic("implement me")
}
